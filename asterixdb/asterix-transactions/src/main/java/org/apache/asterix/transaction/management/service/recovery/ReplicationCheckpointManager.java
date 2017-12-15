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
package org.apache.asterix.transaction.management.service.recovery;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.CheckpointProperties;
import org.apache.asterix.common.transactions.ICheckpointManager;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An implementation of {@link ICheckpointManager} that defines the logic
 * of checkpoints when replication is enabled..
 */
public class ReplicationCheckpointManager extends AbstractCheckpointManager {

    private static final Logger LOGGER = LogManager.getLogger();

    public ReplicationCheckpointManager(ITransactionSubsystem txnSubsystem, CheckpointProperties checkpointProperties) {
        super(txnSubsystem, checkpointProperties);
    }

    /**
     * Performs a sharp checkpoint. All datasets are flushed and all transaction
     * log files are deleted except the files that are needed for dead replicas.
     */
    @Override
    public synchronized void doSharpCheckpoint() throws HyracksDataException {
        LOGGER.info("Starting sharp checkpoint...");
        final IDatasetLifecycleManager datasetLifecycleManager =
                txnSubsystem.getAsterixAppRuntimeContextProvider().getDatasetLifecycleManager();
        datasetLifecycleManager.flushAllDatasets();
        long minFirstLSN;
        // If shutting down, need to check if we need to keep any remote logs for dead replicas
        if (txnSubsystem.getAsterixAppRuntimeContextProvider().getAppContext().isShuttingdown()) {
            final Set<String> deadReplicaIds = txnSubsystem.getAsterixAppRuntimeContextProvider().getAppContext()
                    .getReplicationManager().getDeadReplicasIds();
            if (deadReplicaIds.isEmpty()) {
                // No dead replicas => no need to keep any log
                minFirstLSN = SHARP_CHECKPOINT_LSN;
            } else {
                // Get min LSN of dead replicas remote resources
                minFirstLSN = getDeadReplicasMinFirstLSN(deadReplicaIds);
            }
        } else {
            // Start up complete checkpoint. Avoid deleting remote recovery logs.
            minFirstLSN = txnSubsystem.getRecoveryManager().getMinFirstLSN();
        }
        capture(minFirstLSN, true);
        if (minFirstLSN == SHARP_CHECKPOINT_LSN) {
            // No need to keep any logs
            txnSubsystem.getLogManager().renewLogFiles();
        } else {
            // Delete only log files with LSNs < any dead replica partition minimum LSN
            txnSubsystem.getLogManager().deleteOldLogFiles(minFirstLSN);
        }
        LOGGER.info("Completed sharp checkpoint.");
    }

    /***
     * Attempts to perform a soft checkpoint at the specified {@code checkpointTargetLSN}.
     * If a checkpoint cannot be captured due to datasets having LSN < {@code checkpointTargetLSN},
     * an asynchronous flush is triggered on them. If the checkpoint fails due to a replica index,
     * a request is sent to the primary replica of the index to flush it.
     * When a checkpoint is successful, all transaction log files that end with
     * LSN < {@code checkpointTargetLSN} are deleted.
     */
    @Override
    public synchronized long tryCheckpoint(long checkpointTargetLSN) throws HyracksDataException {
        LOGGER.info("Attemping soft checkpoint...");
        final long minFirstLSN = txnSubsystem.getRecoveryManager().getMinFirstLSN();
        boolean checkpointSucceeded = minFirstLSN >= checkpointTargetLSN;
        if (!checkpointSucceeded) {
            // Flush datasets with indexes behind target checkpoint LSN
            final IDatasetLifecycleManager datasetLifecycleManager =
                    txnSubsystem.getAsterixAppRuntimeContextProvider().getDatasetLifecycleManager();
            datasetLifecycleManager.scheduleAsyncFlushForLaggingDatasets(checkpointTargetLSN);
            // Request remote replicas to flush lagging indexes
            final IReplicationManager replicationManager =
                    txnSubsystem.getAsterixAppRuntimeContextProvider().getAppContext().getReplicationManager();
            try {
                replicationManager.requestFlushLaggingReplicaIndexes(checkpointTargetLSN);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        capture(minFirstLSN, false);
        if (checkpointSucceeded) {
            txnSubsystem.getLogManager().deleteOldLogFiles(minFirstLSN);
            LOGGER.info(String.format("soft checkpoint succeeded with at LSN(%s)", minFirstLSN));
        }
        return minFirstLSN;
    }

    private long getDeadReplicasMinFirstLSN(Set<String> deadReplicaIds) throws HyracksDataException {
        final IReplicaResourcesManager remoteResourcesManager =
                txnSubsystem.getAsterixAppRuntimeContextProvider().getAppContext().getReplicaResourcesManager();
        final IApplicationContext propertiesProvider =
                txnSubsystem.getAsterixAppRuntimeContextProvider().getAppContext();
        final MetadataProperties metadataProperties = propertiesProvider.getMetadataProperties();
        final PersistentLocalResourceRepository localResourceRepository =
                (PersistentLocalResourceRepository) txnSubsystem.getAsterixAppRuntimeContextProvider()
                        .getLocalResourceRepository();
        // Get partitions of the dead replicas that are not active on this node
        final Set<Integer> deadReplicasPartitions = new HashSet<>();
        for (String deadReplicaId : deadReplicaIds) {
            final ClusterPartition[] nodePartitons = metadataProperties.getNodePartitions().get(deadReplicaId);
            for (ClusterPartition partition : nodePartitons) {
                if (!localResourceRepository.getActivePartitions().contains(partition.getPartitionId())) {
                    deadReplicasPartitions.add(partition.getPartitionId());
                }
            }
        }
        return remoteResourcesManager.getPartitionsMinLSN(deadReplicasPartitions);
    }
}