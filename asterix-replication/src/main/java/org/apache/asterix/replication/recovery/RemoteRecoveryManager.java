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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.replication.IRemoteRecoveryManager;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;

public class RemoteRecoveryManager implements IRemoteRecoveryManager {

    private final IReplicationManager replicationManager;
    private final ILogManager logManager;
    public static final boolean IS_DEBUG_MODE = false;//true
    private static final Logger LOGGER = Logger.getLogger(RemoteRecoveryManager.class.getName());
    private final IAsterixAppRuntimeContext runtimeContext;
    private final AsterixReplicationProperties replicationProperties;

    public RemoteRecoveryManager(IReplicationManager replicationManager, IAsterixAppRuntimeContext runtimeContext,
            AsterixReplicationProperties replicationProperties) {
        this.replicationManager = replicationManager;
        this.runtimeContext = runtimeContext;
        this.logManager = runtimeContext.getTransactionSubsystem().getLogManager();
        this.replicationProperties = replicationProperties;
    }

    @Override
    public void performRemoteRecovery() {
        //The whole remote recovery process should be atomic.
        //Any error happens, we should start the recovery from the start until the recovery is complete or an illegal state is reached (cannot recovery).
        int maxRecoveryAttempts = 10;

        while (true) {
            //start recovery recovery steps
            try {
                maxRecoveryAttempts--;

                if (maxRecoveryAttempts == 0) {
                    //to avoid infinite loop in case of unexpected behavior.
                    throw new IllegalStateException("Failed to perform remote recovery.");
                }

                /*** Prepare for Recovery ***/
                //1. check remote replicas states
                replicationManager.initializeReplicasState();
                int activeReplicasCount = replicationManager.getActiveReplicasCount();

                if (activeReplicasCount == 0) {
                    throw new IllegalStateException("no ACTIVE remote replica(s) exists to performe remote recovery");
                }

                //2. clean any memory data that could've existed from previous failed recovery attempt
                IDatasetLifecycleManager datasetLifeCycleManager = runtimeContext.getDatasetLifecycleManager();
                datasetLifeCycleManager.closeAllDatasets();

                //3. remove any existing storage data
                runtimeContext.getReplicaResourcesManager().deleteAsterixStorageData();

                //4. select remote replicas to recover from per lost replica data
                Map<String, Set<String>> selectedRemoteReplicas = constructRemoteRecoveryPlan();

                //5. get max LSN from selected remote replicas
                long maxRemoteLSN = 0;
                maxRemoteLSN = replicationManager.getMaxRemoteLSN(selectedRemoteReplicas.keySet());

                //6. force LogManager to start from a partition > maxLSN in selected remote replicas
                logManager.renewLogFilesAndStartFromLSN(maxRemoteLSN);

                /*** Start Recovery Per Lost Replica ***/
                for (Entry<String, Set<String>> remoteReplica : selectedRemoteReplicas.entrySet()) {
                    String replicaId = remoteReplica.getKey();
                    Set<String> replicasDataToRecover = remoteReplica.getValue();

                    //1. Request indexes metadata and LSM components
                    replicationManager.requestReplicaFiles(replicaId, replicasDataToRecover);

                    //2. Initialize local resources based on the newly received files (if we are recovering the primary replica on this node)
                    if (replicasDataToRecover.contains(logManager.getNodeId())) {
                        ((PersistentLocalResourceRepository) runtimeContext.getLocalResourceRepository()).initialize(
                                logManager.getNodeId(),
                                runtimeContext.getReplicaResourcesManager().getLocalStorageFolder());
                        //initialize resource id factor to correct max resource id
                        runtimeContext.initializeResourceIdFactory();
                    }

                    //3. Get min LSN to start requesting logs from
                    long minLSN = replicationManager.requestReplicaMinLSN(replicaId);

                    //4. Request remote logs from selected remote replicas
                    ArrayList<ILogRecord> remoteRecoveryLogs = replicationManager.requestReplicaLogs(replicaId,
                            replicasDataToRecover, minLSN);

                    //5. Replay remote logs using recovery manager
                    if (replicasDataToRecover.contains(logManager.getNodeId())) {
                        if (remoteRecoveryLogs.size() > 0) {
                            runtimeContext.getTransactionSubsystem().getRecoveryManager()
                                    .replayRemoteLogs(remoteRecoveryLogs);
                        }
                        remoteRecoveryLogs.clear();
                    }
                }

                LOGGER.log(Level.INFO, "Completed remote recovery successfully!");
                break;
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Failed during remote recovery. Attempting again...");
            }
        }
    }

    private Map<String, Set<String>> constructRemoteRecoveryPlan() {

        //1. identify which replicas reside in this node
        String localNodeId = logManager.getNodeId();
        Set<String> nodes = replicationProperties.getNodeReplicasIds(localNodeId);

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
}