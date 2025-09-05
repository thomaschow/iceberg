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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestBigTableTableOperations {

  private static final String CATALOG_NAME = "test_catalog";
  private static final String CATALOG_TABLE_NAME = "iceberg_catalog";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("db", "table");

  private BigtableDataClient dataClient;
  private BigTableTableOperations tableOperations;
  private FileIO fileIO;

  @BeforeEach
  public void before() {
    dataClient = Mockito.mock(BigtableDataClient.class);
    fileIO = Mockito.mock(FileIO.class);

    Map<String, String> properties = Maps.newHashMap();
    properties.put(GCPProperties.BIGTABLE_PROJECT_ID, "test-project");
    properties.put(GCPProperties.BIGTABLE_INSTANCE_ID, "test-instance");

    tableOperations =
        new BigTableTableOperations(
            dataClient,
            new GCPProperties(properties),
            CATALOG_NAME,
            fileIO,
            TABLE_IDENTIFIER,
            CATALOG_TABLE_NAME);
  }

  @Test
  public void testTableName() {
    assertThat(tableOperations.tableName()).isEqualTo("test_catalog.db.table");
  }

  @Test
  public void testFileIO() {
    assertThat(tableOperations.io()).isEqualTo(fileIO);
  }

  @Test
  public void testDoRefreshTableExists() {
    String metadataLocation = "gs://bucket/metadata/table.metadata.json";
    Row mockRow = createMockRowWithMetadata(metadataLocation);
    String rowKey = BigTableCatalog.tableRowKey(TABLE_IDENTIFIER);

    String mockMetadata =
        "{"
            + "\"format-version\": 2,"
            + "\"table-uuid\": \"test-uuid\","
            + "\"location\": \"gs://bucket/test_catalog.db/table\","
            + "\"last-sequence-number\": 0,"
            + "\"last-updated-ms\": 1234567890,"
            + "\"last-column-id\": 3,"
            + "\"current-schema-id\": 0,"
            + "\"schemas\": ["
            + "  {"
            + "    \"type\": \"struct\","
            + "    \"schema-id\": 0,"
            + "    \"fields\": ["
            + "      {\"id\": 1, \"name\": \"id\", \"required\": true, \"type\": \"long\"},"
            + "      {\"id\": 2, \"name\": \"name\", \"required\": false, \"type\": \"string\"}"
            + "    ]"
            + "  }"
            + "],"
            + "\"default-spec-id\": 0,"
            + "\"partition-specs\": ["
            + "  {\"spec-id\": 0, \"fields\": []}"
            + "],"
            + "\"last-partition-id\": 999,"
            + "\"default-sort-order-id\": 0,"
            + "\"sort-orders\": ["
            + "  {\"order-id\": 0, \"fields\": []}"
            + "],"
            + "\"snapshots\": [],"
            + "\"snapshot-log\": [],"
            + "\"metadata-log\": []"
            + "}";

    InputFile mockInputFile = Mockito.mock(InputFile.class);
    Mockito.when(mockInputFile.location()).thenReturn(metadataLocation);
    SeekableInputStream mockInputStream =
        new SeekableInputStream() {
          private final ByteArrayInputStream stream =
              new ByteArrayInputStream(mockMetadata.getBytes());

          @Override
          public long getPos() throws IOException {
            return mockMetadata.getBytes().length - stream.available();
          }

          @Override
          public void seek(long newPos) throws IOException {
            stream.reset();
            stream.skip(newPos);
          }

          @Override
          public int read() throws IOException {
            return stream.read();
          }

          @Override
          public void close() throws IOException {
            stream.close();
          }
        };
    try {
      Mockito.when(mockInputFile.newStream()).thenReturn(mockInputStream);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    Mockito.when(fileIO.newInputFile(metadataLocation)).thenReturn(mockInputFile);

    Mockito.when(dataClient.readRow(CATALOG_TABLE_NAME, rowKey)).thenReturn(mockRow);

    tableOperations.doRefresh();

    Mockito.verify(dataClient).readRow(CATALOG_TABLE_NAME, rowKey);
  }

  @Test
  public void testDoRefreshTableDoesNotExist() {
    String rowKey = BigTableCatalog.tableRowKey(TABLE_IDENTIFIER);
    Mockito.when(dataClient.readRow(CATALOG_TABLE_NAME, rowKey)).thenReturn(null);

    tableOperations.doRefresh();

    Mockito.verify(dataClient).readRow(CATALOG_TABLE_NAME, rowKey);
  }

  @Test
  public void testDoRefreshTableDeletedAfterLoad() {
    String rowKey = BigTableCatalog.tableRowKey(TABLE_IDENTIFIER);

    // Set current metadata location to simulate table was loaded before
    try {
      java.lang.reflect.Field field =
          BaseMetastoreTableOperations.class.getDeclaredField("currentMetadataLocation");
      field.setAccessible(true);
      field.set(tableOperations, "gs://bucket/metadata/existing.metadata.json");
    } catch (Exception e) {
      throw new RuntimeException("Failed to set current metadata location", e);
    }

    Mockito.when(dataClient.readRow(CATALOG_TABLE_NAME, rowKey)).thenReturn(null);

    assertThatThrownBy(() -> tableOperations.doRefresh())
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("Cannot find table");
  }

  @Test
  public void testPersistTableNew() {
    String rowKey = BigTableCatalog.tableRowKey(TABLE_IDENTIFIER);
    Map<String, String> properties =
        ImmutableMap.of(
            "table_type", "ICEBERG",
            "metadata_location", "gs://bucket/metadata/table.metadata.json");

    Mockito.when(dataClient.readRow(CATALOG_TABLE_NAME, rowKey)).thenReturn(null);

    tableOperations.persistTable(rowKey, null, properties);

    Mockito.verify(dataClient).readRow(CATALOG_TABLE_NAME, rowKey);
    Mockito.verify(dataClient).mutateRow(any());
  }

  @Test
  public void testPersistTableExisting() {
    String rowKey = BigTableCatalog.tableRowKey(TABLE_IDENTIFIER);
    String currentVersion = "current-version-uuid";
    Row existingRow = createMockRowWithVersion(currentVersion);
    Map<String, String> properties =
        ImmutableMap.of(
            "table_type", "ICEBERG",
            "metadata_location", "gs://bucket/metadata/table.metadata.json");

    tableOperations.persistTable(rowKey, existingRow, properties);

    Mockito.verify(dataClient).mutateRow(any());
  }

  @Test
  public void testPersistTableNewAlreadyExists() {
    String rowKey = BigTableCatalog.tableRowKey(TABLE_IDENTIFIER);
    Map<String, String> properties =
        ImmutableMap.of(
            "table_type", "ICEBERG",
            "metadata_location", "gs://bucket/metadata/table.metadata.json");

    Row existingRow = createMockRowWithVersion("existing-version");
    Mockito.when(dataClient.readRow(CATALOG_TABLE_NAME, rowKey)).thenReturn(existingRow);

    assertThatThrownBy(() -> tableOperations.persistTable(rowKey, null, properties))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Table already exists");
  }

  private Row createMockRowWithMetadata(String metadataLocation) {
    Row row = Mockito.mock(Row.class);
    RowCell metadataCell = Mockito.mock(RowCell.class);
    Mockito.when(metadataCell.getValue()).thenReturn(ByteString.copyFromUtf8(metadataLocation));

    String propertyCol =
        BigTableCatalog.toPropertyCol(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    Mockito.when(row.getCells(eq("properties"), eq(ByteString.copyFromUtf8(propertyCol))))
        .thenReturn(ImmutableList.of(metadataCell));

    return row;
  }

  private Row createMockRowWithVersion(String version) {
    Row row = Mockito.mock(Row.class);
    RowCell versionCell = Mockito.mock(RowCell.class);
    Mockito.when(versionCell.getValue()).thenReturn(ByteString.copyFromUtf8(version));

    Mockito.when(
            row.getCells(eq("metadata"), eq(ByteString.copyFromUtf8(BigTableCatalog.COL_VERSION))))
        .thenReturn(ImmutableList.of(versionCell));

    return row;
  }
}
