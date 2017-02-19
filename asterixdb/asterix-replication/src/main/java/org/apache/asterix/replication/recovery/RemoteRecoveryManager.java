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
package org.apache.asterix.replication.recovery;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.asterix.common.api.IAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.config.IPropertiesProvider;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.replication.IRemoteRecoveryManager;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.replication.storage.ReplicaResourcesManager;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class RemoteRecoveryManager implements IRemoteRecoveryManager {

    private final IReplicationManager replicationManager;
    private static final Logger LOGGER = Logger.getLogger(RemoteRecoveryManager.class.getName());
    private final IAppRuntimeContext runtimeContext;
    private final ReplicationProperties replicationProperties;
    private Map<String, Set<String>> failbackRecoveryReplicas;

    public RemoteRecoveryManager(IReplicationManager replicationManager, IAppRuntimeContext runtimeContext,
            ReplicationProperties replicationProperties) {
        this.replicationManager = replicationManager;
        this.runtimeContext = runtimeContext;
        this.replicationProperties = replicationProperties;
    }

    private Map<String, Set<String>> constructRemoteRecoveryPlan() {
        //1. identify which replicas reside in this node
        String localNodeId = runtimeContext.getTransactionSubsystem().getId();

        Set<String> nodes = replicationProperties.getNodeReplicasIds(localNodeId);
        Map<String, Set<String>> recoveryCandidates = new HashMap<>();
        Map<String, Integer> candidatesScore = new HashMap<>();

        //2. identify which nodes has backup per lost node data
        for (String node : nodes) {
            Set<String> locations = replicationProperties.getNodeReplicasIds(node);

            //since the local node just started, remove it from candidates
            locations.remove(localNodeId);

            //remove any dead replicas
            Set<String> deadReplicas = replicationManager.getDeadReplicasIds();
            for (String deadReplica : deadReplicas) {
                locations.remove(deadReplica);
            }

            //no active replicas to recover from
            if (locations.isEmpty()) {
                throw new IllegalStateException("Could not find any ACTIVE replica to recover " + node + " data.");
            }

            for (String location : locations) {
                if (candidatesScore.containsKey(location)) {
                    candidatesScore.put(location, candidatesScore.get(location) + 1);
                } else {
                    candidatesScore.put(location, 1);
                }
            }
            recoveryCandidates.put(node, locations);
        }

        Map<String, Set<String>> recoveryList = new HashMap<>();

        //3. find best candidate to recover from per lost replica data
        for (Entry<String, Set<String>> entry : recoveryCandidates.entrySet()) {
            int winnerScore = -1;
            String winner = "";
            for (String node : entry.getValue()) {

                int nodeScore = candidatesScore.get(node);

                if (nodeScore > winnerScore) {
                    winnerScore = nodeScore;
                    winner = node;
                }
            }

            if (recoveryList.containsKey(winner)) {
                recoveryList.get(winner).add(entry.getKey());
            } else {
                Set<String> nodesToRecover = new HashSet<>();
                nodesToRecover.add(entry.getKey());
                recoveryList.put(winner, nodesToRecover);
            }

        }

        return recoveryList;
    }

    @Override
    public void replayReplicaPartitionLogs(Set<Integer> partitions, boolean flush) throws HyracksDataException {
        ILogManager logManager = runtimeContext.getTransactionSubsystem().getLogManager();
        long minLSN = runtimeContext.getReplicaResourcesManager().getPartitionsMinLSN(partitions);
        long readableSmallestLSN = logManager.getReadableSmallestLSN();
        if (minLSN < readableSmallestLSN) {
            minLSN = readableSmallestLSN;
        }

        //replay logs > minLSN that belong to these partitions
        IRecoveryManager recoveryManager = runtimeContext.getTransactionSubsystem().getRecoveryManager();
        try {
            recoveryManager.replayPartitionsLogs(partitions, logManager.getLogReader(true), minLSN);
            if (flush) {
                runtimeContext.getDatasetLifecycleManager().flushAllDatasets();
            }
        } catch (IOException | ACIDException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void takeoverPartitons(Integer[] partitions) throws IOException, ACIDException {
        /**
         * TODO even though the takeover is always expected to succeed,
         * in case of any failure during the takeover, the CC should be
         * notified that the takeover failed.
         */
        Set<Integer> partitionsToTakeover = new HashSet<>(Arrays.asList(partitions));
        replayReplicaPartitionLogs(partitionsToTakeover, false);

        //mark these partitions as active in this node
        PersistentLocalResourceRepository resourceRepository = (PersistentLocalResourceRepository) runtimeContext
                .getLocalResourceRepository();
        for (Integer patitionId : partitions) {
            resourceRepository.addActivePartition(patitionId);
        }
    }

    @Override
    public void startFailbackProcess() {
        int maxRecoveryAttempts = replicationProperties.getMaxRemoteRecoveryAttempts();
        PersistentLocalResourceRepository resourceRepository = (PersistentLocalResourceRepository) runtimeContext
                .getLocalResourceRepository();
        IDatasetLifecycleManager datasetLifeCycleManager = runtimeContext.getDatasetLifecycleManager();
        Map<String, ClusterPartition[]> nodePartitions = ((IPropertiesProvider) runtimeContext).getMetadataProperties()
                .getNodePartitions();

        while (true) {
            //start recovery steps
            try {
                if (maxRecoveryAttempts <= 0) {
                    //to avoid infinite loop in case of unexpected behavior.
                    throw new IllegalStateException("Failed to perform remote recovery.");
                }

                /*** Prepare for Recovery ***/
                //1. check remote replicas states
                replicationManager.initializeReplicasState();
                int activeReplicasCount = replicationManager.getActiveReplicasCount();

                if (activeReplicasCount == 0) {
                    throw new IllegalStateException("no ACTIVE remote replica(s) exists to perform remote recovery");
                }

                //2. clean any memory data that could've existed from previous failed recovery attempt
                datasetLifeCycleManager.closeAllDatasets();

                //3. remove any existing storage data and initialize storage metadata
                resourceRepository.deleteStorageData(true);
                resourceRepository.initializeNewUniverse(ClusterProperties.INSTANCE.getStorageDirectoryName());

                //4. select remote replicas to recover from per lost replica data
                failbackRecoveryReplicas = constructRemoteRecoveryPlan();

                /*** Start Recovery Per Lost Replica ***/
                for (Entry<String, Set<String>> remoteReplica : failbackRecoveryReplicas.entrySet()) {
                    String replicaId = remoteReplica.getKey();
                    Set<String> ncsToRecoverFor = remoteReplica.getValue();
                    Set<Integer> partitionsIds = new HashSet<>();
                    for (String node : ncsToRecoverFor) {
                        partitionsIds.addAll((Arrays.asList(nodePartitions.get(node))).stream()
                                .map(ClusterPartition::getPartitionId).collect(Collectors.toList()));
                    }

                    //1. Request indexes metadata and LSM components
                    replicationManager.requestReplicaFiles(replicaId, partitionsIds, new HashSet<String>());
                }
                break;
            } catch (IOException e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.log(Level.WARNING, "Failed during remote recovery. Attempting again...", e);
                }
                maxRecoveryAttempts--;
            }
        }
    }

    @Override
    public void completeFailbackProcess() throws IOException, InterruptedException {
        ILogManager logManager = runtimeContext.getTransactionSubsystem().getLogManager();
        ReplicaResourcesManager replicaResourcesManager = (ReplicaResourcesManager) runtimeContext
                .getReplicaResourcesManager();
        Map<String, ClusterPartition[]> nodePartitions = ((IPropertiesProvider) runtimeContext).getMetadataProperties()
                .getNodePartitions();

        /**
         * for each lost partition, get the remaining files from replicas
         * to complete the failback process.
         */
        try {
            for (Entry<String, Set<String>> remoteReplica : failbackRecoveryReplicas.entrySet()) {
                String replicaId = remoteReplica.getKey();
                Set<String> NCsDataToRecover = remoteReplica.getValue();
                Set<String> existingFiles = new HashSet<>();
                Set<Integer> partitionsToRecover = new HashSet<>();
                for (String nodeId : NCsDataToRecover) {
                    //get partitions that will be recovered from this node
                    ClusterPartition[] replicaPartitions = nodePartitions.get(nodeId);
                    for (ClusterPartition partition : replicaPartitions) {
                        existingFiles.addAll(
                                replicaResourcesManager.getPartitionIndexesFiles(partition.getPartitionId(), true));
                        partitionsToRecover.add(partition.getPartitionId());
                    }
                }

                //Request remaining indexes files
                replicationManager.requestReplicaFiles(replicaId, partitionsToRecover, existingFiles);
            }
        } catch (IOException e) {
            /**
             * in case of failure during failback completion process we need to construct a new plan
             * and get all the files from the start since the remote replicas will change in the new plan.
             */
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.log(Level.WARNING, "Failed during completing failback. Restarting failback process...", e);
            }
            startFailbackProcess();
        }

        //get max LSN from selected remote replicas
        long maxRemoteLSN = replicationManager.getMaxRemoteLSN(failbackRecoveryReplicas.keySet());

        //6. force LogManager to start from a partition > maxLSN in selected remote replicas
        logManager.renewLogFilesAndStartFromLSN(maxRemoteLSN);

        //start replication service after failback completed
        runtimeContext.getReplicationChannel().start();
        runtimeContext.getReplicationManager().startReplicationThreads();

        failbackRecoveryReplicas = null;
    }

    //TODO refactor common code between remote recovery and failback process
    @Override
    public void doRemoteRecoveryPlan(Map<String, Set<Integer>> recoveryPlan) throws HyracksDataException {
        int maxRecoveryAttempts = replicationProperties.getMaxRemoteRecoveryAttempts();
        PersistentLocalResourceRepository resourceRepository = (PersistentLocalResourceRepository) runtimeContext
                .getLocalResourceRepository();
        IDatasetLifecycleManager datasetLifeCycleManager = runtimeContext.getDatasetLifecycleManager();
        ILogManager logManager = runtimeContext.getTransactionSubsystem().getLogManager();
        while (true) {
            //start recovery steps
            try {
                if (maxRecoveryAttempts <= 0) {
                    //to avoid infinite loop in case of unexpected behavior.
                    throw new IllegalStateException("Failed to perform remote recovery.");
                }

                /*** Prepare for Recovery ***/
                //1. clean any memory data that could've existed from previous failed recovery attempt
                datasetLifeCycleManager.closeAllDatasets();

                //2. remove any existing storage data and initialize storage metadata
                resourceRepository.deleteStorageData(true);
                resourceRepository.initializeNewUniverse(ClusterProperties.INSTANCE.getStorageDirectoryName());

                /*** Start Recovery Per Lost Replica ***/
                for (Entry<String, Set<Integer>> remoteReplica : recoveryPlan.entrySet()) {
                    String replicaId = remoteReplica.getKey();
                    Set<Integer> partitionsToRecover = remoteReplica.getValue();

                    //Request indexes metadata and LSM components
                    replicationManager.requestReplicaFiles(replicaId, partitionsToRecover, new HashSet<String>());
                }

                //get max LSN from selected remote replicas
                long maxRemoteLSN = replicationManager.getMaxRemoteLSN(recoveryPlan.keySet());

                //6. force LogManager to start from a partition > maxLSN in selected remote replicas
                logManager.renewLogFilesAndStartFromLSN(maxRemoteLSN);
                break;
            } catch (IOException e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.log(Level.WARNING, "Failed during remote recovery. Attempting again...", e);
                }
                maxRecoveryAttempts--;
            }
        }
    }
}