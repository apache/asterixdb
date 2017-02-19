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
package org.apache.asterix.util;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.replication.Replica;
import org.apache.asterix.runtime.message.ReplicaEventMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.api.application.IClusterLifecycleListener.ClusterEventType;

public class FaultToleranceUtil {

    private static final Logger LOGGER = Logger.getLogger(FaultToleranceUtil.class.getName());
    private static final String CLUSTER_NET_IP_ADDRESS_KEY = "cluster-net-ip-address";

    private FaultToleranceUtil() {
        throw new AssertionError();
    }

    public static void notifyImpactedReplicas(String nodeId, ClusterEventType event,
            IClusterStateManager clusterManager, ICCMessageBroker messageBroker,
            IReplicationStrategy replicationStrategy) {
        List<String> primaryRemoteReplicas = replicationStrategy.getRemotePrimaryReplicas(nodeId).stream()
                .map(Replica::getId).collect(Collectors.toList());
        String nodeIdAddress = StringUtils.EMPTY;
        Map<String, Map<String, String>> activeNcConfiguration = clusterManager.getActiveNcConfiguration();
        // In case the node joined with a new IP address, we need to send it to the other replicas
        if (event == ClusterEventType.NODE_JOIN) {
            nodeIdAddress = activeNcConfiguration.get(nodeId).get(CLUSTER_NET_IP_ADDRESS_KEY);
        }
        ReplicaEventMessage msg = new ReplicaEventMessage(nodeId, nodeIdAddress, event);
        for (String replica : primaryRemoteReplicas) {
            // If the remote replica is alive, send the event
            if (activeNcConfiguration.containsKey(replica)) {
                try {
                    messageBroker.sendApplicationMessageToNC(msg, replica);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Failed sending an application message to an NC", e);
                }
            }
        }
    }
}