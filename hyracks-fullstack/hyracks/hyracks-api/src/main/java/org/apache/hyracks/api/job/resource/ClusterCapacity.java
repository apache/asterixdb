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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.util.StorageUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ClusterCapacity implements IClusterCapacity {
    private static final long serialVersionUID = 3487998182013966747L;

    private long aggregatedMemoryByteSize = 0;
    private int aggregatedCores = 0;
    private final Map<String, Long> nodeMemoryMap = new HashMap<>();
    private final Map<String, Integer> nodeCoreMap = new HashMap<>();

    @Override
    public long getAggregatedMemoryByteSize() {
        return aggregatedMemoryByteSize;
    }

    @Override
    public int getAggregatedCores() {
        return aggregatedCores;
    }

    @Override
    public long getMemoryByteSize(String nodeId) throws HyracksException {
        if (!nodeMemoryMap.containsKey(nodeId)) {
            throw HyracksException.create(ErrorCode.NO_SUCH_NODE, nodeId);
        }
        return nodeMemoryMap.get(nodeId);
    }

    @Override
    public int getCores(String nodeId) throws HyracksException {
        if (!nodeMemoryMap.containsKey(nodeId)) {
            throw HyracksException.create(ErrorCode.NO_SUCH_NODE, nodeId);
        }
        return nodeCoreMap.get(nodeId);
    }

    @Override
    public void setAggregatedMemoryByteSize(long aggregatedMemoryByteSize) {
        this.aggregatedMemoryByteSize = aggregatedMemoryByteSize;
    }

    @Override
    public void setAggregatedCores(int aggregatedCores) {
        this.aggregatedCores = aggregatedCores;
    }

    @Override
    public void setMemoryByteSize(String nodeId, long memoryByteSize) {
        nodeMemoryMap.put(nodeId, memoryByteSize);
    }

    @Override
    public void setCores(String nodeId, int cores) {
        nodeCoreMap.put(nodeId, cores);
    }

    @Override
    public void update(String nodeId, NodeCapacity nodeCapacity) throws HyracksException {
        // Removes the existing node resource and the aggregated resource statistics.
        if (nodeMemoryMap.containsKey(nodeId)) {
            aggregatedMemoryByteSize -= nodeMemoryMap.remove(nodeId);
        }
        if (nodeCoreMap.containsKey(nodeId)) {
            aggregatedCores -= nodeCoreMap.remove(nodeId);
        }

        long memorySize = nodeCapacity.getMemoryByteSize();
        int cores = nodeCapacity.getCores();
        // Updates the node capacity map when both memory size and cores are positive.
        if (memorySize > 0 && cores > 0) {
            aggregatedMemoryByteSize += memorySize;
            aggregatedCores += cores;
            nodeMemoryMap.put(nodeId, memorySize);
            nodeCoreMap.put(nodeId, cores);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregatedMemoryByteSize, aggregatedCores, nodeMemoryMap, nodeCoreMap);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ClusterCapacity)) {
            return false;
        }
        ClusterCapacity capacity = (ClusterCapacity) o;
        return aggregatedMemoryByteSize == capacity.aggregatedMemoryByteSize
                && aggregatedCores == capacity.aggregatedCores && Objects.equals(nodeMemoryMap, capacity.nodeMemoryMap)
                && Objects.equals(nodeCoreMap, capacity.nodeCoreMap);
    }

    @Override
    public String toString() {
        return "(memory: " + StorageUtil.toHumanReadableSize(aggregatedMemoryByteSize) + " bytes, CPU cores: "
                + aggregatedCores + ")";
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode jcc = om.createObjectNode();
        jcc.put("memory", aggregatedMemoryByteSize);
        jcc.put("cores", aggregatedCores);
        return jcc;
    }
}
