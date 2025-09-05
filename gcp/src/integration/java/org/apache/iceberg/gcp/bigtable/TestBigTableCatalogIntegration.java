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

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(
    named = "iceberg.test.bigtable.enabled",
    matches = "true",
    disabledReason =
        "Bigtable integration tests disabled. Enable with -Diceberg.test.bigtable.enabled=true")
public class TestBigTableCatalogIntegration {

  private static final ForkJoinPool POOL = new ForkJoinPool(16);
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.StringType.get()));

  private static String catalogTableName;
  private static BigtableDataClient dataClient;
  private static BigtableTableAdminClient adminClient;
  private static BigTableCatalog catalog;
  private static String testBucket;

  @BeforeAll
  public static void beforeClass() throws IOException {
    catalogTableName = genRandomName();
    String projectId = System.getProperty("bigtable.project-id", "test-project");
    String instanceId = System.getProperty("bigtable.instance-id", "test-instance");
    dataClient = BigtableDataClient.create(projectId, instanceId);
    adminClient = BigtableTableAdminClient.create(projectId, instanceId);
    catalog = new BigTableCatalog();
    testBucket = System.getProperty("iceberg.test.gcs.bucket", "test-bucket");
    catalog.initialize(
        "test",
        ImmutableMap.of(
            "bigtable.catalog-table",
            catalogTableName,
            CatalogProperties.WAREHOUSE_LOCATION,
            "gs://" + testBucket + "/" + genRandomName()));
  }

  @AfterAll
  public static void afterClass() {
    if (adminClient != null && adminClient.exists(catalogTableName)) {
      adminClient.deleteTable(catalogTableName);
      adminClient.close();
    }
    if (dataClient != null) {
      dataClient.close();
    }
  }

  @Test
  public void testCreateNamespace() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);

    String rowKey = BigTableCatalog.namespaceRowKey(namespace);
    Row row = dataClient.readRow(catalogTableName, rowKey);
    assertThat(row).as("namespace must exist").isNotNull();

    assertThatThrownBy(() -> catalog.createNamespace(namespace))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  public void testCreateNamespaceBadName() {
    assertThatThrownBy(() -> catalog.createNamespace(Namespace.of("a", "", "b")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must not be empty");
    assertThatThrownBy(() -> catalog.createNamespace(Namespace.of("a", "b.c")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must not contain dot");
  }

  @Test
  public void testListSubNamespaces() {
    Namespace parent = Namespace.of(genRandomName());
    List<Namespace> namespaceList =
        IntStream.range(0, 3)
            .mapToObj(i -> Namespace.of(parent.toString(), genRandomName()))
            .collect(Collectors.toList());
    catalog.createNamespace(parent);
    namespaceList.forEach(ns -> catalog.createNamespace(ns));

    List<Namespace> listed = catalog.listNamespaces(parent);
    assertThat(listed).hasSizeGreaterThanOrEqualTo(4); // parent + 3 children + possibly others
  }

  @Test
  public void testNamespaceProperties() {
    Namespace namespace = Namespace.of(genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    catalog.createNamespace(namespace, properties);
    Map<String, String> namespaceProperties = catalog.loadNamespaceMetadata(namespace);

    assertThat(namespaceProperties)
        .as("created namespace should have the assigned properties")
        .containsAllEntriesOf(properties);
  }

  @Test
  public void testUpdateNamespaceProperties() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);

    Map<String, String> properties = ImmutableMap.of("key", "val");
    catalog.setProperties(namespace, properties);

    Map<String, String> namespaceProperties = catalog.loadNamespaceMetadata(namespace);
    assertThat(namespaceProperties).containsEntry("key", "val");
  }

  @Test
  public void testRemoveNamespaceProperties() {
    Namespace namespace = Namespace.of(genRandomName());
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    catalog.createNamespace(namespace, properties);

    catalog.removeProperties(namespace, Sets.newHashSet("key1"));

    Map<String, String> namespaceProperties = catalog.loadNamespaceMetadata(namespace);
    assertThat(namespaceProperties).doesNotContainKey("key1");
    assertThat(namespaceProperties).containsEntry("key2", "val2");
  }

  @Test
  public void testCreateTable() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);

    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, genRandomName());
    Table table = catalog.createTable(tableIdentifier, SCHEMA);

    assertThat(table).isNotNull();
    assertThat(catalog.listTables(namespace)).contains(tableIdentifier);
  }

  @Test
  public void testCreateTableAlreadyExists() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);

    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, genRandomName());
    catalog.createTable(tableIdentifier, SCHEMA);

    assertThatThrownBy(() -> catalog.createTable(tableIdentifier, SCHEMA))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  public void testDropTable() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);

    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, genRandomName());
    catalog.createTable(tableIdentifier, SCHEMA);

    boolean dropped = catalog.dropTable(tableIdentifier);
    assertThat(dropped).isTrue();
    assertThat(catalog.listTables(namespace)).doesNotContain(tableIdentifier);
  }

  @Test
  public void testDropTableDoesNotExist() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);

    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, genRandomName());

    assertThatThrownBy(() -> catalog.dropTable(tableIdentifier))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("does not exist");
  }

  @Test
  public void testRenameTable() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);

    TableIdentifier from = TableIdentifier.of(namespace, genRandomName());
    TableIdentifier to = TableIdentifier.of(namespace, genRandomName());

    catalog.createTable(from, SCHEMA);
    catalog.renameTable(from, to);

    assertThat(catalog.listTables(namespace)).contains(to);
    assertThat(catalog.listTables(namespace)).doesNotContain(from);
  }

  @Test
  public void testLoadTable() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);

    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, genRandomName());
    Table originalTable = catalog.createTable(tableIdentifier, SCHEMA);

    Table loadedTable = catalog.loadTable(tableIdentifier);
    assertThat(loadedTable).isNotNull();
    assertThat(loadedTable.schema()).isEqualTo(originalTable.schema());
  }

  @Test
  public void testConcurrentFastAppendOps() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);

    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, genRandomName());
    Table table = catalog.createTable(tableIdentifier, SCHEMA);

    Table loadedTable = catalog.loadTable(tableIdentifier);
    String metadataLocation =
        ((HasTableOperations) loadedTable).operations().current().metadataFileLocation();

    List<Integer> results =
        IntStream.range(0, 10)
            .parallel()
            .mapToObj(
                i -> {
                  try {
                    Table concurrentTable = catalog.loadTable(tableIdentifier);
                    concurrentTable.newFastAppend().commit();
                    return 1;
                  } catch (Exception e) {
                    return 0;
                  }
                })
            .collect(Collectors.toList());

    long successCount = results.stream().mapToLong(Integer::longValue).sum();
    assertThat(successCount).as("some concurrent operations should succeed").isGreaterThan(0L);
  }

  private static String genRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
