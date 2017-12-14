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
package org.apache.asterix.runtime.message;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.replication.Replica;
import org.apache.asterix.common.replication.ReplicaEvent;
import org.apache.hyracks.api.application.IClusterLifecycleListener.ClusterEventType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ReplicaEventMessage implements INcAddressedMessage {

    private static final long serialVersionUID = 1L;
    private final String nodeId;
    private final ClusterEventType event;
    private final String nodeIPAddress;
    private final int nodePort;

    public ReplicaEventMessage(String nodeId, String nodeIPAddress, int nodePort, ClusterEventType event) {
        this.nodeId = nodeId;
        this.nodeIPAddress = nodeIPAddress;
        this.nodePort = nodePort;
        this.event = event;
    }

    public String getNodeId() {
        return nodeId;
    }

    public ClusterEventType getEvent() {
        return event;
    }

    public String getNodeIPAddress() {
        return nodeIPAddress;
    }

    @Override
    public void handle(INcApplicationContext appContext) throws HyracksDataException, InterruptedException {
        Replica replica = new Replica(nodeId, nodeIPAddress, nodePort);
        appContext.getReplicationManager().reportReplicaEvent(new ReplicaEvent(replica, event));
    }

    @Override
    public String toString() {
        return ReplicaEventMessage.class.getSimpleName();
    }
}
