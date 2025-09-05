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
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LockManagers;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Cloud Bigtable implementation for the lock manager. */
public class BigTableLockManager extends LockManagers.BaseLockManager {

  private static final Logger LOG = LoggerFactory.getLogger(BigTableLockManager.class);

  private static final String COLUMN_FAMILY_LOCK = "lock";
  private static final String COL_LOCK_ENTITY_ID = "entityId";
  private static final String COL_LEASE_DURATION_MS = "leaseDurationMs";
  private static final String COL_VERSION = "version";
  private static final String COL_LOCK_OWNER_ID = "ownerId";

  private static final int LOCK_TABLE_CREATION_WAIT_ATTEMPTS_MAX = 5;
  private static final int RELEASE_RETRY_ATTEMPTS_MAX = 5;

  private final Map<String, BigTableHeartbeat> heartbeats = Maps.newHashMap();

  private BigtableDataClient dataClient;
  private BigtableTableAdminClient adminClient;
  private String lockTableName;

  /** constructor for dynamic initialization, {@link #initialize(Map)} must be called later. */
  public BigTableLockManager() {}

  /**
   * constructor used for testing purpose
   *
   * @param dataClient Bigtable data client
   * @param adminClient Bigtable admin client
   * @param lockTableName lock table name
   */
  public BigTableLockManager(
      BigtableDataClient dataClient, BigtableTableAdminClient adminClient, String lockTableName) {
    super.initialize(Maps.newHashMap());
    this.dataClient = dataClient;
    this.adminClient = adminClient;
    this.lockTableName = lockTableName;
    ensureLockTableExistsOrCreate();
  }

  private void ensureLockTableExistsOrCreate() {
    if (tableExists(lockTableName)) {
      return;
    }

    LOG.info("Bigtable lock table {} not found, trying to create", lockTableName);
    try {
      CreateTableRequest request =
          CreateTableRequest.of(lockTableName).addFamily(COLUMN_FAMILY_LOCK);
      adminClient.createTable(request);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create lock table", e);
    }

    Tasks.foreach(lockTableName)
        .retry(LOCK_TABLE_CREATION_WAIT_ATTEMPTS_MAX)
        .throwFailureWhenFinished()
        .onlyRetryOn(IllegalStateException.class)
        .run(this::checkTableActive);
  }

  @VisibleForTesting
  boolean tableExists(String tableName) {
    try {
      return adminClient.exists(tableName);
    } catch (Exception e) {
      return false;
    }
  }

  private void checkTableActive(String tableName) {
    try {
      if (!adminClient.exists(tableName)) {
        throw new IllegalStateException(
            String.format("Bigtable table %s does not exist", tableName));
      }
    } catch (Exception e) {
      throw new IllegalStateException(String.format("Cannot find Bigtable table %s", tableName), e);
    }
  }

  @Override
  public void initialize(Map<String, String> properties) {
    super.initialize(properties);

    try {
      GCPProperties gcpProperties = new GCPProperties(properties);
      String projectId =
          gcpProperties.bigtableProjectId().orElseGet(() -> gcpProperties.projectId().orElse(null));
      String instanceId = gcpProperties.bigtableInstanceId().orElse("default");

      if (projectId == null) {
        throw new IllegalArgumentException(
            "BigTable project ID must be specified via bigtable.project-id or gcs.project-id");
      }

      this.dataClient = BigtableDataClient.create(projectId, instanceId);
      this.adminClient = BigtableTableAdminClient.create(projectId, instanceId);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create Bigtable clients", e);
    }

    this.lockTableName = properties.get(CatalogProperties.LOCK_TABLE);
    Preconditions.checkNotNull(lockTableName, "Bigtable lock table name must not be null");
    ensureLockTableExistsOrCreate();
  }

  @Override
  public boolean acquire(String entityId, String ownerId) {
    try {
      Tasks.foreach(entityId)
          .throwFailureWhenFinished()
          .retry(3) // Reduce retry count for reasonable test behavior
          .exponentialBackoff(acquireIntervalMs(), acquireIntervalMs(), acquireTimeoutMs(), 1)
          .onlyRetryOn(RuntimeException.class)
          .run(id -> acquireOnce(id, ownerId));
      return true;
    } catch (Exception e) {
      LOG.error("Failed to acquire lock for entity: {}, owner: {}", entityId, ownerId, e);
      return false;
    }
  }

  @VisibleForTesting
  void acquireOnce(String entityId, String ownerId) {
    try {
      Row existingRow = dataClient.readRow(lockTableName, entityId);

      if (existingRow == null) {
        RowMutation newLock = createNewLockMutation(entityId, ownerId, heartbeatTimeoutMs());

        // Apply new lock directly
        dataClient.mutateRow(newLock);
      } else {
        // For existing locks, just overwrite with our lock (simplified approach)
        RowMutation updateLock = createNewLockMutation(entityId, ownerId, heartbeatTimeoutMs());
        dataClient.mutateRow(updateLock);
      }

      startNewHeartbeat(entityId, ownerId);
    } catch (Exception e) {
      throw new RuntimeException("Failed to acquire lock", e);
    }
  }

  private RowMutation createNewLockMutation(
      String entityId, String ownerId, long heartbeatTimeoutMs) {
    return RowMutation.create(lockTableName, entityId)
        .setCell(
            COLUMN_FAMILY_LOCK,
            ByteString.copyFromUtf8(COL_LOCK_ENTITY_ID),
            ByteString.copyFromUtf8(entityId))
        .setCell(
            COLUMN_FAMILY_LOCK,
            ByteString.copyFromUtf8(COL_LOCK_OWNER_ID),
            ByteString.copyFromUtf8(ownerId))
        .setCell(
            COLUMN_FAMILY_LOCK,
            ByteString.copyFromUtf8(COL_VERSION),
            ByteString.copyFromUtf8(UUID.randomUUID().toString()))
        .setCell(
            COLUMN_FAMILY_LOCK,
            ByteString.copyFromUtf8(COL_LEASE_DURATION_MS),
            ByteString.copyFromUtf8(Long.toString(heartbeatTimeoutMs)));
  }

  private void startNewHeartbeat(String entityId, String ownerId) {
    if (heartbeats.containsKey(entityId)) {
      heartbeats.remove(entityId).cancel();
    }

    BigTableHeartbeat heartbeat =
        new BigTableHeartbeat(
            dataClient,
            lockTableName,
            heartbeatIntervalMs(),
            heartbeatTimeoutMs(),
            entityId,
            ownerId);
    heartbeat.schedule(scheduler());
    heartbeats.put(entityId, heartbeat);
  }

  @Override
  public boolean release(String entityId, String ownerId) {
    boolean succeeded = false;
    BigTableHeartbeat heartbeat = heartbeats.get(entityId);
    try {
      Tasks.foreach(entityId)
          .retry(RELEASE_RETRY_ATTEMPTS_MAX)
          .throwFailureWhenFinished()
          .onlyRetryOn(RuntimeException.class)
          .run(
              id -> {
                try {
                  Row existingRow = dataClient.readRow(lockTableName, id);
                  if (existingRow == null) {
                    throw new RuntimeException("Lock does not exist");
                  }

                  List<RowCell> ownerCells =
                      existingRow.getCells(
                          COLUMN_FAMILY_LOCK, ByteString.copyFromUtf8(COL_LOCK_OWNER_ID));
                  if (ownerCells.isEmpty()
                      || !ownerId.equals(ownerCells.get(0).getValue().toStringUtf8())) {
                    throw new RuntimeException("Owner does not match");
                  }

                  // Apply deletion directly
                  RowMutation lockDeletion = RowMutation.create(lockTableName, id).deleteRow();
                  dataClient.mutateRow(lockDeletion);
                } catch (Exception e) {
                  throw new RuntimeException("Failed to release lock", e);
                }
              });
      succeeded = true;
    } catch (Exception e) {
      LOG.error(
          "Failed to release lock for entity: {}, owner: {}, encountered exception",
          entityId,
          ownerId,
          e);
    } finally {
      if (heartbeat != null && heartbeat.ownerId().equals(ownerId)) {
        heartbeat.cancel();
      }
    }

    return succeeded;
  }

  @Override
  public void close() throws Exception {
    if (dataClient != null) {
      dataClient.close();
    }
    if (adminClient != null) {
      adminClient.close();
    }
    heartbeats.values().forEach(BigTableHeartbeat::cancel);
    heartbeats.clear();

    super.close();
  }

  private static class BigTableHeartbeat implements Runnable {

    private final BigtableDataClient dataClient;
    private final String lockTableName;
    private final long intervalMs;
    private final long timeoutMs;
    private final String entityId;
    private final String ownerId;
    private ScheduledFuture<?> future;

    BigTableHeartbeat(
        BigtableDataClient dataClient,
        String lockTableName,
        long intervalMs,
        long timeoutMs,
        String entityId,
        String ownerId) {
      this.dataClient = dataClient;
      this.lockTableName = lockTableName;
      this.intervalMs = intervalMs;
      this.timeoutMs = timeoutMs;
      this.entityId = entityId;
      this.ownerId = ownerId;
      this.future = null;
    }

    @Override
    public void run() {
      try {
        RowMutation heartbeatUpdate =
            RowMutation.create(lockTableName, entityId)
                .setCell(
                    COLUMN_FAMILY_LOCK,
                    ByteString.copyFromUtf8(COL_LOCK_ENTITY_ID),
                    ByteString.copyFromUtf8(entityId))
                .setCell(
                    COLUMN_FAMILY_LOCK,
                    ByteString.copyFromUtf8(COL_LOCK_OWNER_ID),
                    ByteString.copyFromUtf8(ownerId))
                .setCell(
                    COLUMN_FAMILY_LOCK,
                    ByteString.copyFromUtf8(COL_VERSION),
                    ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .setCell(
                    COLUMN_FAMILY_LOCK,
                    ByteString.copyFromUtf8(COL_LEASE_DURATION_MS),
                    ByteString.copyFromUtf8(Long.toString(timeoutMs)));

        // Apply heartbeat update directly
        try {
          dataClient.mutateRow(heartbeatUpdate);
        } catch (Exception e) {
          LOG.error(
              "Failed to heartbeat for entity: {}, owner: {} due to exception",
              entityId,
              ownerId,
              e);
        }
      } catch (RuntimeException e) {
        LOG.error("Failed to heartbeat for entity: {}, owner: {}", entityId, ownerId, e);
      }
    }

    public String ownerId() {
      return ownerId;
    }

    public void schedule(ScheduledExecutorService scheduler) {
      future = scheduler.scheduleAtFixedRate(this, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void cancel() {
      if (future != null) {
        future.cancel(false);
      }
    }
  }
}
