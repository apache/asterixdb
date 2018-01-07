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
package org.apache.asterix.common.replication;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface INcLifecycleCoordinator {

    /**
     * Defines the logic of a {@link INcLifecycleCoordinator} when a node joins the cluster.
     *
     * @param nodeId
     * @throws HyracksDataException
     */
    void notifyNodeJoin(String nodeId) throws HyracksDataException;

    /**
     * Defines the logic of a {@link INcLifecycleCoordinator} when a node leaves the cluster.
     *
     * @param nodeId
     * @throws HyracksDataException
     */
    void notifyNodeFailure(String nodeId) throws HyracksDataException;

    /**
     * Binds the coordinator to {@code cluserManager}.
     *
     * @param clusterManager
     */
    void bindTo(IClusterStateManager clusterManager);

    /**
     * Processes {@code message} based on the message type.
     *
     * @param message
     * @throws HyracksDataException
     */
    void process(INCLifecycleMessage message) throws HyracksDataException;

    /**
     * Performs the required steps to change the metadata node to {@code node}
     *
     * @param node
     */
    void notifyMetadataNodeChange(String node) throws HyracksDataException;
}