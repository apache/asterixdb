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
package org.apache.asterix.common.cluster;

public class ClusterPartition implements Cloneable {
    private final int partitionId;
    private final String nodeId;
    private final int ioDeviceNum;
    private String activeNodeId = null;
    private boolean active = false;
    /* a flag indicating if the partition was dynamically added to the cluster and pending first time activation */
    private boolean pendingActivation = false;

    public ClusterPartition(int partitionId, String nodeId, int ioDeviceNum) {
        this.partitionId = partitionId;
        this.nodeId = nodeId;
        this.ioDeviceNum = ioDeviceNum;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getIODeviceNum() {
        return ioDeviceNum;
    }

    public String getActiveNodeId() {
        return activeNodeId;
    }

    public void setActiveNodeId(String activeNodeId) {
        this.activeNodeId = activeNodeId;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean isActive() {
        return active;
    }

    public boolean isPendingActivation() {
        return pendingActivation;
    }

    public void setPendingActivation(boolean pendingActivation) {
        this.pendingActivation = pendingActivation;
    }

    @Override
    public ClusterPartition clone() {
        return new ClusterPartition(partitionId, nodeId, ioDeviceNum);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ID:" + partitionId);
        sb.append(", Original Node: " + nodeId);
        sb.append(", IODevice: " + ioDeviceNum);
        sb.append(", Active Node: " + activeNodeId);
        sb.append(", Pending Activation: " + pendingActivation);
        return sb.toString();
    }
}
