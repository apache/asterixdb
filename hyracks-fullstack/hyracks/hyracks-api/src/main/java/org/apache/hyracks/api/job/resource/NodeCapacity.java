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

/**
 * Specifies the capacity for computation on a particular node, i.e., a NCDriver process.
 */
public class NodeCapacity implements Serializable {
    private static final long serialVersionUID = -6124502740160006465L;

    // All memory for computations -- this is not changed during the lifetime of a running instance.
    private final long memoryByteSize;

    // All CPU cores -- currently we assume that it doesn't change during the lifetime of a running instance.
    // Otherwise, for each heartbeat, we will have to update global cluster capacity of a cluster.
    private final int cores;

    /**
     * NOTE: neither parameters can be negative.
     * However, both of them can be zero, which means the node is to be removed from the cluster.
     *
     * @param memorySize,
     *            the memory size of the node.
     * @param cores,
     *            the number of cores of the node.
     */
    public NodeCapacity(long memorySize, int cores) {
        this.memoryByteSize = memorySize;
        this.cores = cores;
    }

    public long getMemoryByteSize() {
        return memoryByteSize;
    }

    public int getCores() {
        return cores;
    }

}
