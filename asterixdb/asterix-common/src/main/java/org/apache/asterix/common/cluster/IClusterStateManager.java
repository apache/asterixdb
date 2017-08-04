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
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.hyracks.api.config.IOption;
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
    Map<String, Map<IOption, Object>> getActiveNcConfiguration();

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

    /**
     * Blocks until the cluster state becomes {@code state}
     */
    void waitForState(ClusterState state) throws HyracksDataException, InterruptedException;

    /**
     * Blocks until the cluster state becomes {@code state}, or timeout is exhausted.
     * @return true if the desired state was reached before timeout occurred
     */
    boolean waitForState(ClusterState waitForState, long timeout, TimeUnit unit)
            throws HyracksDataException, InterruptedException;

    /**
     * Register the specified node partitions with the specified nodeId with this cluster state manager
     * then calls {@link IClusterStateManager#refreshState()}
     *
     * @param nodeId
     * @param nodePartitions
     * @throws AsterixException
     */
    void registerNodePartitions(String nodeId, ClusterPartition[] nodePartitions) throws AsterixException;

    /**
     * De-register the specified node's partitions from this cluster state manager
     * then calls {@link IClusterStateManager#refreshState()}
     *
     * @param nodeId
     * @throws HyracksDataException
     */
    void deregisterNodePartitions(String nodeId) throws HyracksDataException;
}
