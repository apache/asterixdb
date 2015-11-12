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
package org.apache.asterix.replication.management;

import java.util.Set;

import org.apache.asterix.common.api.IClusterEventsSubscriber;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.api.IClusterManagementWorkResponse;
import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.common.replication.Replica;
import org.apache.asterix.common.replication.ReplicaEvent;
import org.apache.asterix.common.replication.ReplicaEvent.ReplicaEventType;

public class ReplicationLifecycleListener implements IClusterEventsSubscriber {

    private final AsterixReplicationProperties asterixReplicationProperties;
    public static ReplicationLifecycleListener INSTANCE;

    public ReplicationLifecycleListener(AsterixReplicationProperties asterixReplicationProperties) {
        this.asterixReplicationProperties = asterixReplicationProperties;
    }

    @Override
    public Set<IClusterManagementWork> notifyNodeFailure(Set<String> deadNodeIds) {
        //notify impacted replicas
        for (String deadNodeId : deadNodeIds) {
            Replica eventOnReplica = asterixReplicationProperties.getReplicaById(deadNodeId);
            ReplicaEvent event = new ReplicaEvent(eventOnReplica, ReplicaEventType.FAIL);
            ReplicaEventNotifier notifier = new ReplicaEventNotifier(event, asterixReplicationProperties);

            //start notifier
            new Thread(notifier).start();
        }

        return null;
    }

    @Override
    public Set<IClusterManagementWork> notifyNodeJoin(String joinedNodeId) {
        Replica eventOnReplica = asterixReplicationProperties.getReplicaById(joinedNodeId);
        ReplicaEvent event = new ReplicaEvent(eventOnReplica, ReplicaEventType.JOIN);
        ReplicaEventNotifier notifier = new ReplicaEventNotifier(event, asterixReplicationProperties);

        //start notifier
        new Thread(notifier).start();

        return null;
    }

    @Override
    public void notifyRequestCompletion(IClusterManagementWorkResponse response) {
        //do nothing
    }

    @Override
    public void notifyStateChange(ClusterState previousState, ClusterState newState) {
        //do nothing
    }
}
