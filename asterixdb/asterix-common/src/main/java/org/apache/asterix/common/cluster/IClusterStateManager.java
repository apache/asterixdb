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

import java.util.Map;

import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IClusterStateManager {

    /**
     * @return The current cluster state.
     */
    ClusterState getState();

    /**
     * Updates the cluster state based on the state of all cluster partitions and the metadata node.
     * Cluster state after refresh:
     * ACTIVE: all cluster partitions are active and the metadata node is bound.
     * PENDING: all cluster partitions are active but the metadata node is not bound.
     * UNUSABLE: one or more cluster partitions are not active.
     */
    void refreshState() throws HyracksDataException;

    /**
     * Sets the cluster state into {@code state}
     */
    void setState(ClusterState state);

    /**
     * Updates all partitions of {@code nodeId} based on the {@code active} flag.
     * @param nodeId
     * @param active
     * @throws HyracksDataException
     */
    void updateNodePartitions(String nodeId, boolean active) throws HyracksDataException;

    /**
     * Updates the active node and active state of the cluster partition with id {@code partitionNum}
     */
    void updateClusterPartition(Integer partitionNum, String activeNode, boolean active);

    /**
     * Updates the metadata node id and its state.
     */
    void updateMetadataNode(String nodeId, boolean active);

    /**
     * @return a map of nodeId and NC Configuration for active nodes.
     */
    Map<String, Map<String, String>> getActiveNcConfiguration();

    /**
     * @return The current metadata node Id.
     */
    String getCurrentMetadataNodeId();

    /**
     * @param nodeId
     * @return The node originally assigned partitions.
     */
    ClusterPartition[] getNodePartitions(String nodeId);

    /**
     * @return A copy of the current state of the cluster partitions.
     */
    ClusterPartition[] getClusterPartitons();
}
