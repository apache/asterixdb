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

package org.apache.hyracks.api.job.resource;

import org.apache.hyracks.api.exceptions.HyracksException;

/**
 * This interface abstracts the mutable capacity for a cluster.
 */
public interface IClusterCapacity extends IReadOnlyClusterCapacity {

    /**
     * Sets the aggregated memory size for a cluster.
     *
     * @param aggregatedMemoryByteSize,
     *            the aggregated memory size.
     */
    void setAggregatedMemoryByteSize(long aggregatedMemoryByteSize);

    /**
     * Sets the aggregated number of CPU cores for a cluster.
     *
     * @param aggregatedCores,
     *            the total number of cores.
     */
    void setAggregatedCores(int aggregatedCores);

    /**
     * Sets the memory byte size (for computation) of a specific node.
     *
     * @param nodeId,
     *            the node id.
     * @param memoryByteSize,
     *            the available memory byte size for computation of the node.
     */
    void setMemoryByteSize(String nodeId, long memoryByteSize);

    /**
     * Sets the number of CPU cores for a specific node.
     *
     * @param nodeId,
     *            the node id.
     * @param cores,
     *            the number of CPU cores for the node.
     */
    void setCores(String nodeId, int cores);

    /**
     * Updates the cluster capacity information with the capacity of one particular node.
     *
     * @param nodeId,
     *            the id of the node for updating.
     * @param capacity,
     *            the capacity of one particular node.
     * @throws HyracksException
     *             when the parameters are invalid.
     */
    void update(String nodeId, NodeCapacity capacity) throws HyracksException;
}
