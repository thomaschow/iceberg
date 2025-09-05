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

import static org.apache.iceberg.gcp.bigtable.BigTableCatalog.toPropertyCol;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestBigTableCatalog {

  private static final String WAREHOUSE_PATH = "gs://bucket";
  private static final String CATALOG_NAME = "bigtable";
  private static final String CATALOG_TABLE_NAME = "iceberg_catalog";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("db", "table");

  private BigtableDataClient dataClient;
  private BigtableTableAdminClient adminClient;
  private BigTableCatalog bigTableCatalog;

  @BeforeEach
  public void before() {
    dataClient = Mockito.mock(BigtableDataClient.class);
    adminClient = Mockito.mock(BigtableTableAdminClient.class);
    bigTableCatalog = new BigTableCatalog();

    Map<String, String> properties = Maps.newHashMap();
    properties.put(GCPProperties.BIGTABLE_PROJECT_ID, "test-project");
    properties.put(GCPProperties.BIGTABLE_INSTANCE_ID, "test-instance");
    properties.put(CatalogProperties.LOCK_TABLE, "test_lock_table");

    bigTableCatalog.initialize(
        CATALOG_NAME, WAREHOUSE_PATH, new GCPProperties(properties), dataClient, adminClient, null);
  }

  @Test
  public void testConstructorWarehousePathWithEndSlash() {
    BigTableCatalog catalogWithSlash = new BigTableCatalog();

    Map<String, String> properties = Maps.newHashMap();
    properties.put(GCPProperties.BIGTABLE_PROJECT_ID, "test-project");
    properties.put(GCPProperties.BIGTABLE_INSTANCE_ID, "test-instance");
    properties.put(CatalogProperties.LOCK_TABLE, "test_lock_table");

    catalogWithSlash.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH + "/",
        new GCPProperties(properties),
        dataClient,
        adminClient,
        null);

    Row mockRow = createMockRow(ImmutableMap.of());
    Mockito.doReturn(mockRow).when(dataClient).readRow(eq(CATALOG_TABLE_NAME), any(String.class));

    String location = catalogWithSlash.defaultWarehouseLocation(TABLE_IDENTIFIER);
    assertThat(location).isEqualTo(WAREHOUSE_PATH + "/db.db/table");
  }

  @Test
  public void testDefaultWarehouseLocationNoDbUri() {
    Row mockRow = createMockRow(ImmutableMap.of());
    Mockito.doReturn(mockRow).when(dataClient).readRow(eq(CATALOG_TABLE_NAME), any(String.class));

    String warehousePath = WAREHOUSE_PATH + "/db.db/table";
    String defaultWarehouseLocation = bigTableCatalog.defaultWarehouseLocation(TABLE_IDENTIFIER);
    assertThat(defaultWarehouseLocation).isEqualTo(warehousePath);
  }

  @Test
  public void testDefaultWarehouseLocationDbUri() {
    String dbUri = "gs://bucket2/db";
    String propertyCol = toPropertyCol(BigTableCatalog.defaultLocationProperty());

    Row mockRow = createMockRow(ImmutableMap.of(propertyCol, dbUri));
    Mockito.doReturn(mockRow).when(dataClient).readRow(eq(CATALOG_TABLE_NAME), any(String.class));

    String defaultWarehouseLocation = bigTableCatalog.defaultWarehouseLocation(TABLE_IDENTIFIER);
    assertThat(defaultWarehouseLocation).isEqualTo("gs://bucket2/db/table");
  }

  @Test
  public void testDefaultWarehouseLocationNoNamespace() {
    Mockito.doReturn(null).when(dataClient).readRow(eq(CATALOG_TABLE_NAME), any(String.class));

    assertThatThrownBy(() -> bigTableCatalog.defaultWarehouseLocation(TABLE_IDENTIFIER))
        .as("default warehouse can't be called on non existent namespace")
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Cannot find default warehouse location:");
  }

  @Test
  public void testNamespaceRowKey() {
    assertThat(BigTableCatalog.namespaceRowKey(TABLE_IDENTIFIER.namespace()))
        .isEqualTo("NAMESPACE#db");
  }

  @Test
  public void testTableRowKey() {
    assertThat(BigTableCatalog.tableRowKey(TABLE_IDENTIFIER)).isEqualTo("db#table");
  }

  @Test
  public void testTableRowKeyPrefix() {
    assertThat(BigTableCatalog.tableRowKeyPrefix(TABLE_IDENTIFIER.namespace())).isEqualTo("db#");
  }

  @Test
  public void testToPropertyCol() {
    assertThat(BigTableCatalog.toPropertyCol("test_prop")).isEqualTo("p.test_prop");
  }

  @Test
  public void testIsProperty() {
    assertThat(BigTableCatalog.isProperty("p.test_prop")).isTrue();
    assertThat(BigTableCatalog.isProperty("test_prop")).isFalse();
    assertThat(BigTableCatalog.isProperty("metadata_col")).isFalse();
  }

  @Test
  public void testToPropertyKey() {
    assertThat(BigTableCatalog.toPropertyKey("p.test_prop")).isEqualTo("test_prop");
  }

  private Row createMockRow(Map<String, String> properties) {
    Row row = Mockito.mock(Row.class);
    List<RowCell> cells =
        properties.entrySet().stream()
            .map(
                entry -> {
                  RowCell cell = Mockito.mock(RowCell.class);
                  Mockito.when(cell.getQualifier())
                      .thenReturn(ByteString.copyFromUtf8(entry.getKey()));
                  Mockito.when(cell.getValue())
                      .thenReturn(ByteString.copyFromUtf8(entry.getValue()));
                  return cell;
                })
            .collect(ImmutableList.toImmutableList());

    Mockito.when(row.getCells(eq("properties"), any(ByteString.class)))
        .thenAnswer(
            invocation -> {
              ByteString qualifier = invocation.getArgument(1);
              return cells.stream()
                  .filter(cell -> cell.getQualifier().equals(qualifier))
                  .collect(ImmutableList.toImmutableList());
            });

    Mockito.when(row.getCells("properties")).thenReturn(cells);

    return row;
  }
}
