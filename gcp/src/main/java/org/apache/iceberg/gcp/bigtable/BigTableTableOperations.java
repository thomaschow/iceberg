/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.gcp.bigtable;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BigTableTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(BigTableTableOperations.class);

  private static final String COLUMN_FAMILY_PROPERTIES = "properties";

  private final BigtableDataClient dataClient;
  private final TableIdentifier tableIdentifier;
  private final String fullTableName;
  private final FileIO fileIO;
  private final String catalogTableName;

  BigTableTableOperations(
      BigtableDataClient dataClient,
      GCPProperties gcpProperties,
      String catalogName,
      FileIO fileIO,
      TableIdentifier tableIdentifier,
      String catalogTableName) {
    this.dataClient = dataClient;
    // this.gcpProperties = gcpProperties; // Not currently used
    this.fullTableName = String.format("%s.%s", catalogName, tableIdentifier);
    this.tableIdentifier = tableIdentifier;
    this.fileIO = fileIO;
    this.catalogTableName = catalogTableName;
  }

  @Override
  protected String tableName() {
    return fullTableName;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    String rowKey = BigTableCatalog.tableRowKey(tableIdentifier);

    try {
      Row row = dataClient.readRow(catalogTableName, rowKey);
      if (row != null) {
        metadataLocation = getMetadataLocation(row);
      } else {
        if (currentMetadataLocation() != null) {
          throw new NoSuchTableException(
              "Cannot find table %s after refresh, "
                  + "maybe another process deleted it or revoked your access permission",
              tableName());
        }
      }
    } catch (NoSuchTableException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to refresh table metadata", e);
    }

    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);
    CommitStatus commitStatus = CommitStatus.FAILURE;
    String rowKey = BigTableCatalog.tableRowKey(tableIdentifier);

    try {
      Row row = dataClient.readRow(catalogTableName, rowKey);
      checkMetadataLocation(row, base);
      Map<String, String> properties = prepareProperties(row, newMetadataLocation);
      persistTable(rowKey, row, properties);
      commitStatus = CommitStatus.SUCCESS;
    } catch (CommitFailedException e) {
      throw e;
    } catch (RuntimeException persistFailure) {
      LOG.warn(
          "Received unexpected failure when committing to {}, validating if commit ended up succeeding.",
          fullTableName,
          persistFailure);
      commitStatus = checkCommitStatus(newMetadataLocation, metadata);

      switch (commitStatus) {
        case SUCCESS:
          break;
        case FAILURE:
          throw new CommitFailedException(
              persistFailure, "Cannot commit %s due to unexpected exception", tableName());
        case UNKNOWN:
          throw new CommitStateUnknownException(persistFailure);
      }
    } finally {
      try {
        if (commitStatus == CommitStatus.FAILURE) {
          io().deleteFile(newMetadataLocation);
        }
      } catch (RuntimeException e) {
        LOG.error("Failed to cleanup metadata file at {}", newMetadataLocation, e);
      }
    }
  }

  private void checkMetadataLocation(Row row, TableMetadata base) {
    String bigtableMetadataLocation = row != null ? getMetadataLocation(row) : null;
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
    if (!Objects.equals(baseMetadataLocation, bigtableMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit %s because base metadata location '%s' is not same as the current Bigtable location '%s'",
          tableName(), baseMetadataLocation, bigtableMetadataLocation);
    }
  }

  private String getMetadataLocation(Row row) {
    List<RowCell> cells =
        row.getCells(
            COLUMN_FAMILY_PROPERTIES,
            ByteString.copyFromUtf8(BigTableCatalog.toPropertyCol(METADATA_LOCATION_PROP)));
    if (cells.isEmpty()) {
      return null;
    }
    return cells.get(0).getValue().toStringUtf8();
  }

  private Map<String, String> prepareProperties(Row row, String newMetadataLocation) {
    Map<String, String> properties = row != null ? getProperties(row) : Maps.newHashMap();
    properties.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    properties.put(METADATA_LOCATION_PROP, newMetadataLocation);
    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      properties.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }

    return properties;
  }

  private Map<String, String> getProperties(Row row) {
    return row.getCells(COLUMN_FAMILY_PROPERTIES).stream()
        .filter(cell -> BigTableCatalog.isProperty(cell.getQualifier().toStringUtf8()))
        .collect(
            Collectors.toMap(
                cell -> BigTableCatalog.toPropertyKey(cell.getQualifier().toStringUtf8()),
                cell -> cell.getValue().toStringUtf8()));
  }

  void persistTable(String rowKey, Row existingRow, Map<String, String> parameters) {
    try {
      if (existingRow != null) {
        LOG.debug("Committing existing Bigtable catalog table: {}", tableName());

        RowMutation mutation = RowMutation.create(catalogTableName, rowKey);
        parameters.forEach(
            (k, v) ->
                mutation.setCell(
                    COLUMN_FAMILY_PROPERTIES,
                    ByteString.copyFromUtf8(BigTableCatalog.toPropertyCol(k)),
                    ByteString.copyFromUtf8(v)));

        BigTableCatalog.updateCatalogEntryMetadata(mutation);

        // String currentVersion = getCurrentVersion(existingRow); // Not needed for direct mutation

        // Apply mutation directly
        dataClient.mutateRow(mutation);
      } else {
        LOG.debug("Committing new Bigtable catalog table: {}", tableName());

        RowMutation mutation = RowMutation.create(catalogTableName, rowKey);
        parameters.forEach(
            (k, v) ->
                mutation.setCell(
                    COLUMN_FAMILY_PROPERTIES,
                    ByteString.copyFromUtf8(BigTableCatalog.toPropertyCol(k)),
                    ByteString.copyFromUtf8(v)));

        BigTableCatalog.setNewCatalogEntryMetadata(mutation);

        // Apply mutation directly (check for existence first)
        Row checkRow = dataClient.readRow(catalogTableName, rowKey);
        if (checkRow != null) {
          throw new CommitFailedException("Table already exists");
        }
        dataClient.mutateRow(mutation);
      }
    } catch (CommitFailedException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to persist table", e);
    }
  }

  // private String getCurrentVersion(Row row) {
  //   List<RowCell> cells = row.getCells("metadata",
  // ByteString.copyFromUtf8(BigTableCatalog.COL_VERSION));
  //   if (cells.isEmpty()) {
  //     throw new RuntimeException("Version not found in row");
  //   }
  //   return cells.get(0).getValue().toStringUtf8();
  // }
}
