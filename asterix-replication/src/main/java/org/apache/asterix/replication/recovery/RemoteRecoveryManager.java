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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.common.config.IAsterixPropertiesProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.replication.IRemoteRecoveryManager;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.ILogReader;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.asterix.replication.storage.ReplicaResourcesManager;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.transaction.management.service.logging.RemoteLogReader;

public class RemoteRecoveryManager implements IRemoteRecoveryManager {

    private static final String RECOVERY_LOGS_FILE_NAME = "recoveryLogs";
    private final IReplicationManager replicationManager;
    private static final Logger LOGGER = Logger.getLogger(RemoteRecoveryManager.class.getName());
    private final IAsterixAppRuntimeContext runtimeContext;
    private final AsterixReplicationProperties replicationProperties;
    private final static int REMOTE_RECOVERY_JOB_ID = -1;
    private Map<String, Set<String>> failbackRecoveryReplicas;

    public RemoteRecoveryManager(IReplicationManager replicationManager, IAsterixAppRuntimeContext runtimeContext,
            AsterixReplicationProperties replicationProperties) {
        this.replicationManager = replicationManager;
        this.runtimeContext = runtimeContext;
        this.replicationProperties = replicationProperties;
    }

    @Override
    public void performRemoteRecovery() {
        //The whole remote recovery process should be atomic.
        //Any error happens, we should start the recovery from the start until the recovery is
        //complete or an illegal state is reached (cannot recover or max attempts exceed).
        int maxRecoveryAttempts = replicationProperties.getMaxRemoteRecoveryAttempts();
        PersistentLocalResourceRepository resourceRepository = (PersistentLocalResourceRepository) runtimeContext
                .getLocalResourceRepository();
        IRecoveryManager recoveryManager = runtimeContext.getTransactionSubsystem().getRecoveryManager();
        ILogManager logManager = runtimeContext.getTransactionSubsystem().getLogManager();
        while (true) {
            //start recovery steps
            try {
                if (maxRecoveryAttempts <= 0) {
                    //to avoid infinite loop in case of unexpected behavior.
                    throw new IllegalStateException("Failed to perform remote recovery.");
                }

                //delete any existing recovery files from previous failed recovery attempts
                recoveryManager.deleteRecoveryTemporaryFiles();

                //create temporary file to store recovery logs
                File recoveryLogsFile = recoveryManager.createJobRecoveryFile(REMOTE_RECOVERY_JOB_ID,
                        RECOVERY_LOGS_FILE_NAME);

                /*** Prepare for Recovery ***/
                //1. check remote replicas states
                replicationManager.initializeReplicasState();
                int activeReplicasCount = replicationManager.getActiveReplicasCount();

                if (activeReplicasCount == 0) {
                    throw new IllegalStateException("no ACTIVE remote replica(s) exists to perform remote recovery");
                }

                //2. clean any memory data that could've existed from previous failed recovery attempt
                IDatasetLifecycleManager datasetLifeCycleManager = runtimeContext.getDatasetLifecycleManager();
                datasetLifeCycleManager.closeAllDatasets();

                //3. remove any existing storage data and initialize storage metadata
                resourceRepository.deleteStorageData(true);
                resourceRepository.initializeNewUniverse(AsterixClusterProperties.INSTANCE.getStorageDirectoryName());

                //4. select remote replicas to recover from per lost replica data
                Map<String, Set<String>> selectedRemoteReplicas = constructRemoteRecoveryPlan();

                //5. get max LSN from selected remote replicas
                long maxRemoteLSN = replicationManager.getMaxRemoteLSN(selectedRemoteReplicas.keySet());

                //6. force LogManager to start from a partition > maxLSN in selected remote replicas
                logManager.renewLogFilesAndStartFromLSN(maxRemoteLSN);

                /*** Start Recovery Per Lost Replica ***/
                for (Entry<String, Set<String>> remoteReplica : selectedRemoteReplicas.entrySet()) {
                    String replicaId = remoteReplica.getKey();
                    Set<String> replicasDataToRecover = remoteReplica.getValue();

                    //Request indexes metadata and LSM components
                    replicationManager.requestReplicaFiles(replicaId, replicasDataToRecover, new HashSet<String>());

                    //Get min LSN to start requesting logs from
                    long minLSN = replicationManager.requestReplicaMinLSN(replicaId);

                    //Request remote logs from selected remote replicas
                    replicationManager.requestReplicaLogs(replicaId, replicasDataToRecover, minLSN, recoveryLogsFile);

                    //Replay remote logs using recovery manager
                    if (replicasDataToRecover.contains(logManager.getNodeId())) {
                        //replay logs for local partitions only
                        Set<Integer> nodePartitions = resourceRepository.getNodeOrignalPartitions();
                        try (RandomAccessFile raf = new RandomAccessFile(recoveryLogsFile, "r");
                                FileChannel fileChannel = raf.getChannel();) {
                            ILogReader logReader = new RemoteLogReader(fileChannel, fileChannel.size(),
                                    logManager.getLogPageSize());
                            recoveryManager.replayPartitionsLogs(nodePartitions, logReader, 0);
                        }
                    }
                }
                LOGGER.log(Level.INFO, "Completed remote recovery successfully!");
                break;
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Failed during remote recovery. Attempting again...");
                maxRecoveryAttempts--;
            }
        }
    }

    private Map<String, Set<String>> constructRemoteRecoveryPlan() {
        //1. identify which replicas reside in this node
        String localNodeId = runtimeContext.getTransactionSubsystem().getId();

        Set<String> nodes = replicationProperties.getNodeReplicationClients(localNodeId);

        Map<String, Set<String>> recoveryCandidates = new HashMap<String, Set<String>>();
        Map<String, Integer> candidatesScore = new HashMap<String, Integer>();

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
            if (locations.size() == 0) {
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

        Map<String, Set<String>> recoveryList = new HashMap<String, Set<String>>();

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
                Set<String> nodesToRecover = new HashSet<String>();
                nodesToRecover.add(entry.getKey());
                recoveryList.put(winner, nodesToRecover);
            }

        }

        return recoveryList;
    }

    @Override
    public void takeoverPartitons(Integer[] partitions) throws IOException, ACIDException {
        /**
         * TODO even though the takeover is always expected to succeed,
         * in case of any failure during the takeover, the CC should be
         * notified that the takeover failed.
         */
        Set<Integer> partitionsToTakeover = new HashSet<>(Arrays.asList(partitions));
        ILogManager logManager = runtimeContext.getTransactionSubsystem().getLogManager();

        long minLSN = runtimeContext.getReplicaResourcesManager().getPartitionsMinLSN(partitionsToTakeover);
        long readableSmallestLSN = logManager.getReadableSmallestLSN();
        if (minLSN < readableSmallestLSN) {
            minLSN = readableSmallestLSN;
        }

        //replay logs > minLSN that belong to these partitions
        IRecoveryManager recoveryManager = runtimeContext.getTransactionSubsystem().getRecoveryManager();
        recoveryManager.replayPartitionsLogs(partitionsToTakeover, logManager.getLogReader(true), minLSN);

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

        failbackRecoveryReplicas = new HashMap<>();
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
                resourceRepository.initializeNewUniverse(AsterixClusterProperties.INSTANCE.getStorageDirectoryName());

                //4. select remote replicas to recover from per lost replica data
                failbackRecoveryReplicas = constructRemoteRecoveryPlan();

                /*** Start Recovery Per Lost Replica ***/
                for (Entry<String, Set<String>> remoteReplica : failbackRecoveryReplicas.entrySet()) {
                    String replicaId = remoteReplica.getKey();
                    Set<String> partitionsToRecover = remoteReplica.getValue();

                    //1. Request indexes metadata and LSM components
                    replicationManager.requestReplicaFiles(replicaId, partitionsToRecover, new HashSet<String>());
                }
                break;
            } catch (IOException e) {
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Failed during remote recovery. Attempting again...");
                maxRecoveryAttempts--;
            }
        }
    }

    @Override
    public void completeFailbackProcess() throws IOException {
        ILogManager logManager = runtimeContext.getTransactionSubsystem().getLogManager();
        ReplicaResourcesManager replicaResourcesManager = (ReplicaResourcesManager) runtimeContext
                .getReplicaResourcesManager();
        Map<String, ClusterPartition[]> nodePartitions = ((IAsterixPropertiesProvider) runtimeContext)
                .getMetadataProperties().getNodePartitions();

        /**
         * for each lost partition, get the remaining files from replicas
         * to complete the failback process.
         */
        try {
            for (Entry<String, Set<String>> remoteReplica : failbackRecoveryReplicas.entrySet()) {
                String replicaId = remoteReplica.getKey();
                Set<String> NCsDataToRecover = remoteReplica.getValue();
                Set<String> existingFiles = new HashSet<>();
                for (String nodeId : NCsDataToRecover) {
                    //get partitions that will be recovered from this node
                    ClusterPartition[] replicaPartitions = nodePartitions.get(nodeId);
                    for (ClusterPartition partition : replicaPartitions) {
                        existingFiles.addAll(
                                replicaResourcesManager.getPartitionIndexesFiles(partition.getPartitionId(), true));
                    }
                }

                //Request remaining indexes files
                replicationManager.requestReplicaFiles(replicaId, NCsDataToRecover, existingFiles);
            }
        } catch (IOException e) {
            /**
             * in case of failure during failback completion process we need to construct a new plan
             * and get all the files from the start since the remote replicas will change in the new plan.
             */
            e.printStackTrace();
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
}
