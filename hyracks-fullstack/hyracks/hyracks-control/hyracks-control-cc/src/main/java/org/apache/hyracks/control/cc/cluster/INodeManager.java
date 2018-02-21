/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.control.cc.cluster;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.util.annotations.Idempotent;

/**
 * This interface provides abstractions for a node manager, which manages the node membership in a cluster.
 */
public interface INodeManager {

    /**
     * A functional interface for applying a function for each node.
     */
    @FunctionalInterface
    interface NodeFunction {
        void apply(String nodeId, NodeControllerState ncState);
    }

    /**
     * Applies a function for each node in the cluster.
     *
     * @param nodeFunction,
     *            a function implementation that follows the <code>NodeFunction</code> interface.
     */
    void apply(NodeFunction nodeFunction);

    /**
     * @return all node ids.
     */
    Collection<String> getAllNodeIds();

    /**
     * @return all node controller states.
     */
    Collection<NodeControllerState> getAllNodeControllerStates();

    /**
     * @return the map that maps a IP addresses to a set of node names.
     */
    Map<InetAddress, Set<String>> getIpAddressNodeNameMap();

    /**
     * @return the map that maps a node id to its corresponding node controller info.
     */
    Map<String, NodeControllerInfo> getNodeControllerInfoMap();

    /**
     * Removes all nodes that are considered "dead", i.e., which run out of heartbeats.
     *
     * @return all dead nodes and their impacted jobs.
     * @throws HyracksException
     *             when any IP address given in the dead nodes is not valid
     */
    Pair<Collection<String>, Collection<JobId>> removeDeadNodes() throws HyracksException;

    /**
     * Retrieves the node controller state from a given node id.
     *
     * @param nodeId,
     *            a given node id.
     * @return the corresponding node controller state.
     */
    NodeControllerState getNodeControllerState(String nodeId);

    /**
     * Adds one node into the cluster.
     *
     * @param nodeId,
     *            the node id.
     * @param ncState,
     *            the node controller state.
     * @throws HyracksException
     *             when the node has already been added or the IP address given in the node state is not valid.
     */
    void addNode(String nodeId, NodeControllerState ncState) throws HyracksException;

    /**
     * Removes one node from the cluster.  This method is idempotent.
     *
     * @param nodeId,
     *            the node id.
     * @throws HyracksException
     *             when the IP address given in the node state is not valid
     */
    @Idempotent
    void removeNode(String nodeId) throws HyracksException;

}
