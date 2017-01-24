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
import org.apache.hyracks.api.job.resource.ClusterCapacity;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.api.job.resource.IReadOnlyClusterCapacity;
import org.apache.hyracks.api.job.resource.NodeCapacity;

public class ResourceManager implements IResourceManager {

    // The maximum capacity, assuming that there is no running job that occupies capacity.
    // It is unchanged unless any node is added, removed or updated.
    private IClusterCapacity maxCapacity = new ClusterCapacity();

    // The current capacity, which is dynamically changing.
    private IClusterCapacity currentCapacity = new ClusterCapacity();

    @Override
    public IReadOnlyClusterCapacity getMaximumCapacity() {
        return maxCapacity;
    }

    @Override
    public IClusterCapacity getCurrentCapacity() {
        return currentCapacity;
    }

    @Override
    public void update(String nodeId, NodeCapacity capacity) throws HyracksException {
        maxCapacity.update(nodeId, capacity);
        currentCapacity.update(nodeId, capacity);
    }
}
