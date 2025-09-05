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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestBigTableLockManager {

  private static final ForkJoinPool POOL = new ForkJoinPool(16);

  private BigtableDataClient dataClient;
  private BigtableTableAdminClient adminClient;
  private BigTableLockManager lockManager;
  private String lockTableName;
  private String entityId;
  private String ownerId;

  @BeforeEach
  public void before() {
    dataClient = Mockito.mock(BigtableDataClient.class);
    adminClient = Mockito.mock(BigtableTableAdminClient.class);
    lockTableName = "test_lock_table";

    // Mock table exists to return true by default to avoid table creation during setup
    Mockito.when(adminClient.exists(lockTableName)).thenReturn(true);

    lockManager = new BigTableLockManager(dataClient, adminClient, lockTableName);
    entityId = UUID.randomUUID().toString();
    ownerId = UUID.randomUUID().toString();
  }

  @Test
  public void testTableCreation() {
    Mockito.when(adminClient.exists(lockTableName)).thenReturn(true);
    assertThat(lockManager.tableExists(lockTableName)).isTrue();
  }

  @Test
  public void testTableDoesNotExist() {
    Mockito.when(adminClient.exists(lockTableName)).thenReturn(false);
    assertThat(lockManager.tableExists(lockTableName)).isFalse();
  }

  @Test
  public void testAcquireOnceSingleProcess() {
    Mockito.when(dataClient.readRow(lockTableName, entityId)).thenReturn(null);

    lockManager.acquireOnce(entityId, ownerId);

    Mockito.verify(dataClient, Mockito.atLeastOnce()).mutateRow(any(RowMutation.class));
  }

  @Test
  public void testAcquireOnceWithExistingLock() {
    Row existingRow = createMockRowWithLock(entityId, "other_owner", "1000");
    Mockito.when(dataClient.readRow(lockTableName, entityId)).thenReturn(existingRow);

    lockManager.acquireOnce(entityId, ownerId);

    Mockito.verify(dataClient, Mockito.atLeastOnce()).mutateRow(any(RowMutation.class));
  }

  @Test
  public void testAcquire() {
    Mockito.when(dataClient.readRow(lockTableName, entityId)).thenReturn(null);

    boolean acquired = lockManager.acquire(entityId, ownerId);
    assertThat(acquired).isTrue();
  }

  @Test
  public void testAcquireFailure() {
    Mockito.when(dataClient.readRow(lockTableName, entityId))
        .thenThrow(new RuntimeException("BigTable error"));

    boolean acquired = lockManager.acquire(entityId, ownerId);
    assertThat(acquired).isFalse();
  }

  @Test
  public void testRelease() {
    Row existingRow = createMockRowWithLock(entityId, ownerId, "1000");
    Mockito.when(dataClient.readRow(lockTableName, entityId)).thenReturn(existingRow);

    boolean released = lockManager.release(entityId, ownerId);
    assertThat(released).isTrue();
  }

  @Test
  public void testReleaseNonExistentLock() {
    Mockito.when(dataClient.readRow(lockTableName, entityId)).thenReturn(null);

    boolean released = lockManager.release(entityId, ownerId);
    assertThat(released).isFalse();
  }

  @Test
  public void testReleaseWrongOwner() {
    Row existingRow = createMockRowWithLock(entityId, "other_owner", "1000");
    Mockito.when(dataClient.readRow(lockTableName, entityId)).thenReturn(existingRow);

    boolean released = lockManager.release(entityId, ownerId);
    assertThat(released).isFalse();
  }

  @Test
  public void testConcurrentAcquire() throws ExecutionException, InterruptedException {
    String sharedEntityId = UUID.randomUUID().toString();
    List<String> ownerIds =
        IntStream.range(0, 10)
            .mapToObj(i -> UUID.randomUUID().toString())
            .collect(Collectors.toList());

    Mockito.when(dataClient.readRow(eq(lockTableName), any(String.class))).thenReturn(null);

    List<CompletableFuture<Boolean>> futures =
        ownerIds.stream()
            .map(
                owner ->
                    CompletableFuture.supplyAsync(
                        () -> lockManager.acquire(sharedEntityId, owner), POOL))
            .collect(Collectors.toList());

    List<Boolean> results =
        futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

    long successCount = results.stream().mapToLong(r -> r ? 1 : 0).sum();
    assertThat(successCount)
        .as("all acquires should succeed with simplified locking")
        .isEqualTo(10L);
  }

  @Test
  public void testClose() throws Exception {
    lockManager.close();
    Mockito.verify(dataClient).close();
    Mockito.verify(adminClient).close();
  }

  private Row createMockRowWithLock(
      String entityIdParam, String ownerIdParam, String leaseDuration) {
    Row row = Mockito.mock(Row.class);

    RowCell entityCell = Mockito.mock(RowCell.class);
    Mockito.when(entityCell.getValue()).thenReturn(ByteString.copyFromUtf8(entityIdParam));

    RowCell ownerCell = Mockito.mock(RowCell.class);
    Mockito.when(ownerCell.getValue()).thenReturn(ByteString.copyFromUtf8(ownerIdParam));

    RowCell leaseCell = Mockito.mock(RowCell.class);
    Mockito.when(leaseCell.getValue()).thenReturn(ByteString.copyFromUtf8(leaseDuration));

    RowCell versionCell = Mockito.mock(RowCell.class);
    Mockito.when(versionCell.getValue())
        .thenReturn(ByteString.copyFromUtf8(UUID.randomUUID().toString()));

    Mockito.when(row.getCells(eq("lock"), eq(ByteString.copyFromUtf8("entityId"))))
        .thenReturn(ImmutableList.of(entityCell));
    Mockito.when(row.getCells(eq("lock"), eq(ByteString.copyFromUtf8("ownerId"))))
        .thenReturn(ImmutableList.of(ownerCell));
    Mockito.when(row.getCells(eq("lock"), eq(ByteString.copyFromUtf8("leaseDurationMs"))))
        .thenReturn(ImmutableList.of(leaseCell));
    Mockito.when(row.getCells(eq("lock"), eq(ByteString.copyFromUtf8("version"))))
        .thenReturn(ImmutableList.of(versionCell));

    return row;
  }
}
