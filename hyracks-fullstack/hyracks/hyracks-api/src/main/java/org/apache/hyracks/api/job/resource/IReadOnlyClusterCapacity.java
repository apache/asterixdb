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

import java.io.Serializable;

import org.apache.hyracks.api.exceptions.HyracksException;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This interface provides read-only methods for the capacity of a cluster.
 */
public interface IReadOnlyClusterCapacity extends Serializable {

    /**
     * @return the aggregated memory byte size for the cluster.
     */
    long getAggregatedMemoryByteSize();

    /**
     * @return the aggregated number of cores
     */
    int getAggregatedCores();

    /**
     * Retrieves the memory byte size for computation on a specific node.
     * (Note that usually a portion of memory is used for storage.)
     *
     * @param nodeId,
     *            the node id.
     * @return the memory byte size for computation on the node.
     * @throws HyracksException
     *             when the input node does not exist.
     */
    long getMemoryByteSize(String nodeId) throws HyracksException;

    /**
     * Retrieves the number of CPU cores for computation on a specific node.
     *
     * @param nodeId,
     *            the node id.
     * @return the number of CPU cores for computation on the node.
     * @throws HyracksException,
     *             when the input node does not exist.
     */
    int getCores(String nodeId) throws HyracksException;

    /**
     * Translates this cluster capacity to JSON.
     */
    ObjectNode toJSON();
}
