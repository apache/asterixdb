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

package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.api.job.resource.IReadOnlyClusterCapacity;
import org.apache.hyracks.api.job.resource.NodeCapacity;

/**
 * This interface abstracts the resource management of a cluster.
 */
public interface IResourceManager {

    /**
     * @return the maximum capacity of the cluster, assuming that there is no running job
     *         that occupies capacity.
     */
    IReadOnlyClusterCapacity getMaximumCapacity();

    /**
     * @return the current capacity for computation.
     */
    IClusterCapacity getCurrentCapacity();

    /**
     * Updates the cluster capacity when a node is added, removed, or updated.
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
