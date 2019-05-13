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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.utils.NcLocalCounters;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;

import com.fasterxml.jackson.databind.node.ObjectNode;

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
     *
     * @param nodeId
     * @param active
     * @param ncLocalCounters
     * @throws HyracksDataException
     */
    void updateNodeState(String nodeId, boolean active, NcLocalCounters ncLocalCounters) throws HyracksDataException;

    /**
     * Updates the active node and active state of the cluster partition with id {@code partitionNum}
     */
    void updateClusterPartition(int partitionNum, String activeNode, boolean active);

    /**
     * Updates the metadata node id and its state.
     */
    void updateMetadataNode(String nodeId, boolean active);

    /**
     * @return a map of nodeId and NC Configuration for active nodes.
     */
    Map<String, Map<IOption, Object>> getNcConfiguration();

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
     *
     * @return true if the desired state was reached before timeout occurred
     */
    boolean waitForState(ClusterState waitForState, long timeout, TimeUnit unit)
            throws HyracksDataException, InterruptedException;

    /**
     * Blocks until the cluster state matches supplied predicate, or timeout is exhausted.
     *
     * @return the cluster state matching the predicate if it was satisfied before timeout occurred, otherwise null
     */
    ClusterState waitForState(Predicate<ClusterState> condition, long timeout, TimeUnit unit)
            throws HyracksDataException, InterruptedException;

    /**
     * Register the specified node partitions with the specified nodeId with this cluster state manager
     * then calls {@link IClusterStateManager#refreshState()}
     *
     * @param nodeId
     * @param nodePartitions
     * @throws AsterixException
     */
    void registerNodePartitions(String nodeId, ClusterPartition[] nodePartitions) throws AlgebricksException;

    /**
     * De-register the specified node's partitions from this cluster state manager
     * then calls {@link IClusterStateManager#refreshState()}
     *
     * @param nodeId
     * @throws HyracksDataException
     */
    void deregisterNodePartitions(String nodeId) throws HyracksDataException;

    /**
     * @return true if cluster is active, false otherwise
     */
    boolean isClusterActive();

    /**
     * @return the set of participant nodes
     */
    Set<String> getParticipantNodes();

    /**
     * @return the set of currently failed nodes
     */
    Set<String> getFailedNodes();

    /**
     * @return the set of cluster nodes
     */
    Set<String> getNodes();

    /**
     * Returns the IO devices configured for a Node Controller
     *
     * @param nodeId
     *            unique identifier of the Node Controller
     * @return a list of IO devices.
     */
    String[] getIODevices(String nodeId);

    /**
     * @return the constraint representing all the partitions of the cluster
     */
    AlgebricksAbsolutePartitionConstraint getClusterLocations();

    /**
     * @param excludePendingRemoval
     *            true, if the desired set shouldn't have pending removal nodes
     * @return the set of participant nodes
     */
    Set<String> getParticipantNodes(boolean excludePendingRemoval);

    /**
     * @param node
     *            the node id
     * @return the number of partitions on that node
     */
    int getNodePartitionsCount(String node);

    /**
     * @return a json object representing the cluster state summary
     */
    ObjectNode getClusterStateSummary();

    /**
     * @return a json object representing the cluster state description
     */
    ObjectNode getClusterStateDescription();

    /**
     * Set the cc application context
     *
     * @param appCtx
     */
    void setCcAppCtx(ICcApplicationContext appCtx);

    /**
     * @return the number of cluster nodes
     */
    int getNumberOfNodes();

    /**
     * Notifies {@link IClusterStateManager} that a node has joined
     *
     * @param nodeId
     * @param ncConfiguration
     * @throws HyracksException
     */
    void notifyNodeJoin(String nodeId, Map<IOption, Object> ncConfiguration) throws HyracksException;

    /**
     * @return true if metadata node is active, false otherwise
     */
    boolean isMetadataNodeActive();

    /**
     * Notifies {@link IClusterStateManager} that a node has failed
     *
     * @param deadNode
     * @throws HyracksException
     */
    void notifyNodeFailure(String deadNode) throws HyracksException;

    /**
     * Add node to the list of nodes pending removal
     *
     * @param nodeId
     */
    void removePending(String nodeId);

    /**
     * Deregister intention to remove node id
     *
     * @param nodeId
     * @return
     */
    boolean cancelRemovePending(String nodeId);

    Map<String, Map<IOption, Object>> getActiveNcConfiguration();

    /**
     * Sets the cluster partition in which metadata datasets stored
     *
     * @param partition
     */
    void setMetadataPartitionId(ClusterPartition partition);

    /**
     * Gets the cluster partition in which metadata datasets are stored
     *
     * @return The metadata cluster partitions
     */
    ClusterPartition getMetadataPartition();

    /**
     * Indicate whether one or more datasets must be rebalanced before the cluster becomes ACTIVE
     * @param rebalanceRequired
     */
    void setRebalanceRequired(boolean rebalanceRequired) throws HyracksDataException;
}
