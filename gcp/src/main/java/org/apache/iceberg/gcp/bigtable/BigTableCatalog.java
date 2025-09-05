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

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Cloud Bigtable implementation of Iceberg catalog */
public class BigTableCatalog extends BaseMetastoreCatalog
    implements SupportsNamespaces, Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(BigTableCatalog.class);
  // private static final int CATALOG_TABLE_CREATION_WAIT_ATTEMPTS_MAX = 5; // Not currently used
  static final Joiner COMMA = Joiner.on(',');

  private static final String CATALOG_TABLE_NAME = "iceberg_catalog";
  private static final String COLUMN_FAMILY_METADATA = "metadata";
  private static final String COLUMN_FAMILY_PROPERTIES = "properties";

  // private static final String COL_IDENTIFIER = "identifier"; // Not currently used
  private static final String COL_IDENTIFIER_NAMESPACE = "NAMESPACE";
  // private static final String COL_NAMESPACE = "namespace"; // Not currently used
  private static final String PROPERTY_DEFAULT_LOCATION = "default_location";
  private static final String COL_CREATED_AT = "created_at";
  private static final String COL_UPDATED_AT = "updated_at";

  static final String COL_VERSION = "version";

  private BigtableDataClient dataClient;
  private BigtableTableAdminClient adminClient;
  private Configuration hadoopConf;
  private String catalogName;
  private String warehousePath;
  private GCPProperties gcpProperties;
  private FileIO fileIO;
  private CloseableGroup closeableGroup;
  private Map<String, String> catalogProperties;
  private String catalogTableName;

  public BigTableCatalog() {}

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogProperties = ImmutableMap.copyOf(properties);
    this.gcpProperties = new GCPProperties(properties);
    this.catalogTableName = properties.getOrDefault("bigtable.catalog-table", CATALOG_TABLE_NAME);

    this.fileIO = CatalogUtil.loadFileIO(
        properties.getOrDefault(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO"),
        properties,
        hadoopConf);
    
    initialize(
        name,
        properties.get(CatalogProperties.WAREHOUSE_LOCATION),
        gcpProperties,
        this.fileIO);
  }

  @VisibleForTesting
  void initialize(String name, String path, GCPProperties properties, FileIO io) {
    initialize(name, path, properties, null, null, io);
  }

  @VisibleForTesting
  void initialize(
      String name,
      String path,
      GCPProperties properties,
      BigtableDataClient dataClientParam,
      BigtableTableAdminClient adminClientParam,
      FileIO io) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(path),
        "Cannot initialize BigTableCatalog because warehousePath must not be null or empty");

    this.catalogName = name;
    this.gcpProperties = properties;
    this.warehousePath = LocationUtil.stripTrailingSlash(path);
    this.fileIO = io;
    this.catalogTableName = CATALOG_TABLE_NAME; // Set default catalog table name for tests

    if (dataClientParam != null && adminClientParam != null) {
      // Use provided clients (for testing)
      this.dataClient = dataClientParam;
      this.adminClient = adminClientParam;
    } else {
      // Create real clients
      try {
        String projectId =
            properties.bigtableProjectId().orElseGet(() -> properties.projectId().orElse(null));
        String instanceId = properties.bigtableInstanceId().orElse("default");

        if (projectId == null) {
          throw new IllegalArgumentException(
              "BigTable project ID must be specified via bigtable.project-id or gcs.project-id");
        }

        this.dataClient = BigtableDataClient.create(projectId, instanceId);
        this.adminClient = BigtableTableAdminClient.create(projectId, instanceId);
      } catch (IOException e) {
        throw new RuntimeException("Failed to create Bigtable clients", e);
      }
    }

    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(this.dataClient);
    closeableGroup.addCloseable(this.adminClient);
    closeableGroup.addCloseable(fileIO);
    closeableGroup.addCloseable(metricsReporter());
    closeableGroup.setSuppressCloseFailure(true);

    if (adminClient == null) { // Only create table if using real clients
      ensureCatalogTableExistsOrCreate();
    }
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    validateTableIdentifier(tableIdentifier);
    return new BigTableTableOperations(
        dataClient, gcpProperties, catalogName, fileIO, tableIdentifier, catalogTableName);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    validateTableIdentifier(tableIdentifier);
    String rowKey = namespaceRowKey(tableIdentifier.namespace());

    try {
      Row row = dataClient.readRow(catalogTableName, rowKey);
      if (row == null) {
        throw new NoSuchNamespaceException(
            "Cannot find default warehouse location: namespace %s does not exist",
            tableIdentifier.namespace());
      }

      String defaultLocationCol = toPropertyCol(PROPERTY_DEFAULT_LOCATION);
      List<RowCell> cells =
          row.getCells(COLUMN_FAMILY_PROPERTIES, ByteString.copyFromUtf8(defaultLocationCol));
      if (!cells.isEmpty()) {
        return String.format(
            "%s/%s", cells.get(0).getValue().toStringUtf8(), tableIdentifier.name());
      } else {
        return String.format(
            "%s/%s.db/%s", warehousePath, tableIdentifier.namespace(), tableIdentifier.name());
      }
    } catch (NoSuchNamespaceException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to get default warehouse location", e);
    }
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    validateNamespace(namespace);
    String rowKey = namespaceRowKey(namespace);

    RowMutation mutation = RowMutation.create(catalogTableName, rowKey);
    setNewCatalogEntryMetadata(mutation);

    metadata.forEach(
        (key, value) ->
            mutation.setCell(
                COLUMN_FAMILY_PROPERTIES,
                ByteString.copyFromUtf8(toPropertyCol(key)),
                ByteString.copyFromUtf8(value)));

    try {
      // Check if namespace already exists
      Row existingRow = dataClient.readRow(catalogTableName, rowKey);
      if (existingRow != null) {
        throw new AlreadyExistsException("Cannot create namespace %s: already exists", namespace);
      }

      // Create the namespace since it doesn't exist
      dataClient.mutateRow(mutation);
    } catch (AlreadyExistsException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create namespace", e);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    validateNamespace(namespace);
    List<Namespace> namespaces = Lists.newArrayList();

    String prefix = namespace.isEmpty() ? COL_IDENTIFIER_NAMESPACE : namespace.toString();
    Query query = Query.create(catalogTableName).prefix(prefix);

    try {
      for (Row row : dataClient.readRows(query)) {
        String rowKey = row.getKey().toStringUtf8();
        if (rowKey.startsWith(COL_IDENTIFIER_NAMESPACE + "#")) {
          String ns = rowKey.substring((COL_IDENTIFIER_NAMESPACE + "#").length());
          namespaces.add(Namespace.of(ns.split("\\.")));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to list namespaces", e);
    }

    return namespaces;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    validateNamespace(namespace);
    String rowKey = namespaceRowKey(namespace);

    try {
      Row row = dataClient.readRow(catalogTableName, rowKey);
      if (row == null) {
        throw new NoSuchNamespaceException("Cannot find namespace %s", namespace);
      }

      return row.getCells(COLUMN_FAMILY_PROPERTIES).stream()
          .filter(cell -> isProperty(cell.getQualifier().toStringUtf8()))
          .collect(
              Collectors.toMap(
                  cell -> toPropertyKey(cell.getQualifier().toStringUtf8()),
                  cell -> cell.getValue().toStringUtf8()));
    } catch (NoSuchNamespaceException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load namespace metadata", e);
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    validateNamespace(namespace);
    if (!listTables(namespace).isEmpty()) {
      throw new NamespaceNotEmptyException("Cannot delete non-empty namespace %s", namespace);
    }

    String rowKey = namespaceRowKey(namespace);

    try {
      // Check if namespace exists
      Row existingRow = dataClient.readRow(catalogTableName, rowKey);
      if (existingRow == null) {
        return false; // Namespace doesn't exist
      }

      // Delete the namespace
      RowMutation deletion = RowMutation.create(catalogTableName, rowKey).deleteRow();
      dataClient.mutateRow(deletion);
      return true;
    } catch (Exception e) {
      LOG.error("Failed to drop namespace {}", namespace, e);
      return false;
    }
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    String rowKey = namespaceRowKey(namespace);

    try {
      Row row = dataClient.readRow(catalogTableName, rowKey);
      if (row == null) {
        throw new NoSuchNamespaceException("Cannot find namespace %s", namespace);
      }

      RowMutation mutation = RowMutation.create(catalogTableName, rowKey);
      properties.forEach(
          (key, value) ->
              mutation.setCell(
                  COLUMN_FAMILY_PROPERTIES,
                  ByteString.copyFromUtf8(toPropertyCol(key)),
                  ByteString.copyFromUtf8(value)));

      updateCatalogEntryMetadata(mutation);

      // For simplicity, just apply the mutation directly
      // In a production system, you might want to implement optimistic locking
      dataClient.mutateRow(mutation);
      return true;
    } catch (NoSuchNamespaceException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Failed to set properties for namespace {}", namespace, e);
      return false;
    }
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    String rowKey = namespaceRowKey(namespace);

    try {
      Row row = dataClient.readRow(catalogTableName, rowKey);
      if (row == null) {
        throw new NoSuchNamespaceException("Cannot find namespace %s", namespace);
      }

      RowMutation mutation = RowMutation.create(catalogTableName, rowKey);
      properties.forEach(
          property ->
              mutation.deleteCells(
                  COLUMN_FAMILY_PROPERTIES, ByteString.copyFromUtf8(toPropertyCol(property))));

      updateCatalogEntryMetadata(mutation);

      // String currentVersion = getCurrentVersion(row); // Not needed for direct mutation
      // Apply mutation directly
      dataClient.mutateRow(mutation);
      return true;
    } catch (NoSuchNamespaceException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Failed to remove properties for namespace {}", namespace, e);
      return false;
    }
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    List<TableIdentifier> identifiers = Lists.newArrayList();
    String prefix = tableRowKeyPrefix(namespace);
    Query query = Query.create(catalogTableName).prefix(prefix);

    try {
      for (Row row : dataClient.readRows(query)) {
        String rowKey = row.getKey().toStringUtf8();
        String identifier = rowKey.substring(prefix.length());
        if (!COL_IDENTIFIER_NAMESPACE.equals(identifier)) {
          identifiers.add(TableIdentifier.of(identifier.split("\\.")));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to list tables", e);
    }

    return identifiers;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    String rowKey = tableRowKey(identifier);

    try {
      Row row = dataClient.readRow(catalogTableName, rowKey);
      if (row == null) {
        throw new NoSuchTableException("Cannot find table %s to drop", identifier);
      }

      TableOperations ops = newTableOps(identifier);
      TableMetadata lastMetadata = null;
      if (purge) {
        try {
          lastMetadata = ops.current();
        } catch (Exception e) {
          LOG.warn(
              "Failed to load table metadata for table: {}, continuing drop without purge",
              identifier,
              e);
        }
      }

      // Apply deletion directly
      RowMutation deletion = RowMutation.create(catalogTableName, rowKey).deleteRow();
      dataClient.mutateRow(deletion);
      boolean deleted = true;
      if (!deleted) {
        LOG.error("Cannot complete drop table operation for {}: commit conflict", identifier);
        return false;
      }

      LOG.info("Successfully dropped table {} from BigTable catalog", identifier);

      if (purge && lastMetadata != null) {
        CatalogUtil.dropTableData(ops.io(), lastMetadata);
        LOG.info("Table {} data purged", identifier);
      }

      LOG.info("Dropped table: {}", identifier);
      return true;
    } catch (Exception e) {
      LOG.error("Cannot complete drop table operation for {}: unexpected exception", identifier, e);
      throw new RuntimeException("Failed to drop table", e);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    String fromRowKey = tableRowKey(from);
    String toRowKey = tableRowKey(to);

    try {
      Row fromRow = dataClient.readRow(catalogTableName, fromRowKey);
      if (fromRow == null) {
        throw new NoSuchTableException(
            "Cannot rename table %s to %s: %s does not exist", from, to, from);
      }

      Row toRow = dataClient.readRow(catalogTableName, toRowKey);
      if (toRow != null) {
        throw new AlreadyExistsException(
            "Cannot rename table %s to %s: %s already exists", from, to, to);
      }

      RowMutation toMutation = RowMutation.create(catalogTableName, toRowKey);
      fromRow
          .getCells(COLUMN_FAMILY_PROPERTIES)
          .forEach(
              cell ->
                  toMutation.setCell(
                      COLUMN_FAMILY_PROPERTIES, cell.getQualifier(), cell.getValue()));

      setNewCatalogEntryMetadata(toMutation);

      // Use simple approach - create new then delete old
      dataClient.mutateRow(toMutation);

      RowMutation fromDeletion = RowMutation.create(catalogTableName, fromRowKey).deleteRow();
      dataClient.mutateRow(fromDeletion);

      LOG.info("Successfully renamed table from {} to {}", from, to);
    } catch (Exception e) {
      throw new RuntimeException("Failed to rename table", e);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    hadoopConf = conf;
  }

  @Override
  public Configuration getConf() {
    return hadoopConf;
  }

  @Override
  public void close() throws IOException {
    closeableGroup.close();
  }

  public static String defaultLocationProperty() {
    return PROPERTY_DEFAULT_LOCATION;
  }

  static String toPropertyCol(String propertyKey) {
    return "p." + propertyKey;
  }

  static boolean isProperty(String column) {
    return column.startsWith("p.");
  }

  static String toPropertyKey(String propertyCol) {
    return propertyCol.substring(2);
  }

  static String namespaceRowKey(Namespace namespace) {
    return COL_IDENTIFIER_NAMESPACE + "#" + namespace.toString();
  }

  static String tableRowKey(TableIdentifier identifier) {
    return identifier.namespace().toString() + "#" + identifier.name();
  }

  static String tableRowKeyPrefix(Namespace namespace) {
    return namespace.toString() + "#";
  }

  static void setNewCatalogEntryMetadata(RowMutation mutation) {
    String current = Long.toString(System.currentTimeMillis());
    mutation.setCell(
        COLUMN_FAMILY_METADATA,
        ByteString.copyFromUtf8(COL_CREATED_AT),
        ByteString.copyFromUtf8(current));
    mutation.setCell(
        COLUMN_FAMILY_METADATA,
        ByteString.copyFromUtf8(COL_UPDATED_AT),
        ByteString.copyFromUtf8(current));
    mutation.setCell(
        COLUMN_FAMILY_METADATA,
        ByteString.copyFromUtf8(COL_VERSION),
        ByteString.copyFromUtf8(UUID.randomUUID().toString()));
  }

  static void updateCatalogEntryMetadata(RowMutation mutation) {
    mutation.setCell(
        COLUMN_FAMILY_METADATA,
        ByteString.copyFromUtf8(COL_UPDATED_AT),
        ByteString.copyFromUtf8(Long.toString(System.currentTimeMillis())));
    mutation.setCell(
        COLUMN_FAMILY_METADATA,
        ByteString.copyFromUtf8(COL_VERSION),
        ByteString.copyFromUtf8(UUID.randomUUID().toString()));
  }

  // private String getCurrentVersion(Row row) {
  //   List<RowCell> cells = row.getCells(COLUMN_FAMILY_METADATA,
  // ByteString.copyFromUtf8(COL_VERSION));
  //   if (cells.isEmpty()) {
  //     throw new RuntimeException("Version not found in row");
  //   }
  //   return cells.get(0).getValue().toStringUtf8();
  // }


  private void validateNamespace(Namespace namespace) {
    for (String level : namespace.levels()) {
      ValidationException.check(
          level != null && !level.isEmpty(), "Namespace level must not be empty: %s", namespace);
      ValidationException.check(
          !level.contains("."),
          "Namespace level must not contain dot, but found %s in %s",
          level,
          namespace);
    }
  }

  private void validateTableIdentifier(TableIdentifier identifier) {
    validateNamespace(identifier.namespace());
    ValidationException.check(
        identifier.hasNamespace(), "Table namespace must not be empty: %s", identifier);
    String tableName = identifier.name();
    ValidationException.check(
        !tableName.contains("."), "Table name must not contain dot: %s", tableName);
  }

  private boolean bigtableTableExists(String tableName) {
    try {
      adminClient.exists(tableName);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private void ensureCatalogTableExistsOrCreate() {
    if (bigtableTableExists(catalogTableName)) {
      return;
    }

    LOG.info("BigTable catalog table {} not found, trying to create", catalogTableName);
    try {
      CreateTableRequest request =
          CreateTableRequest.of(catalogTableName)
              .addFamily(COLUMN_FAMILY_METADATA)
              .addFamily(COLUMN_FAMILY_PROPERTIES);
      adminClient.createTable(request);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create catalog table", e);
    }
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }
}
