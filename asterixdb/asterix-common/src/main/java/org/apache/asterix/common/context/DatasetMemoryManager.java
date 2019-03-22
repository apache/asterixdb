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
package org.apache.asterix.common.context;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.api.IDatasetMemoryManager;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.hyracks.util.JSONUtil;
import org.apache.hyracks.util.annotations.ThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@ThreadSafe
public class DatasetMemoryManager implements IDatasetMemoryManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<Integer, Long> allocatedMap = new HashMap<>();
    private final Map<Integer, Long> reservedMap = new HashMap<>();
    private long available;
    private final StorageProperties storageProperties;

    public DatasetMemoryManager(StorageProperties storageProperties) {
        this.storageProperties = storageProperties;
        available = storageProperties.getMemoryComponentGlobalBudget();
    }

    @Override
    public synchronized boolean allocate(int datasetId) {
        if (allocatedMap.containsKey(datasetId)) {
            throw new IllegalStateException("Memory is already allocated for dataset: " + datasetId);
        }
        if (reservedMap.containsKey(datasetId)) {
            allocateReserved(datasetId);
            return true;
        }
        final long required = getTotalSize(datasetId);
        if (!isAllocatable(required)) {
            return false;
        }
        allocatedMap.put(datasetId, required);
        available -= required;
        LOGGER.info(() -> "Allocated(" + required + ") for dataset(" + datasetId + ")");
        return true;
    }

    @Override
    public synchronized void deallocate(int datasetId) {
        if (!allocatedMap.containsKey(datasetId) && !reservedMap.containsKey(datasetId)) {
            throw new IllegalStateException("No allocated or reserved memory for dataset: " + datasetId);
        }
        final Long allocated = allocatedMap.remove(datasetId);
        // return the allocated budget if it is not reserved
        if (allocated != null && !reservedMap.containsKey(datasetId)) {
            available += allocated;
            LOGGER.info(() -> "Deallocated(" + allocated + ") from dataset(" + datasetId + ")");
        }
    }

    @Override
    public synchronized boolean reserve(int datasetId) {
        if (reservedMap.containsKey(datasetId)) {
            LOGGER.info("Memory is already reserved for dataset: {}", () -> datasetId);
            return true;
        }
        final long required = getTotalSize(datasetId);
        if (!isAllocatable(required) && !allocatedMap.containsKey(datasetId)) {
            return false;
        }
        reservedMap.put(datasetId, required);
        // if the budget is already allocated, no need to reserve it again
        if (!allocatedMap.containsKey(datasetId)) {
            available -= required;
        }
        LOGGER.info(() -> "Reserved(" + required + ") for dataset(" + datasetId + ")");
        return true;
    }

    @Override
    public synchronized void cancelReserved(int datasetId) {
        final Long reserved = reservedMap.remove(datasetId);
        if (reserved == null) {
            throw new IllegalStateException("No reserved memory for dataset: " + datasetId);
        }
        available += reserved;
        LOGGER.info(() -> "Cancelled reserved(" + reserved + ") from dataset(" + datasetId + ")");
    }

    @Override
    public long getAvailable() {
        return available;
    }

    @Override
    public int getNumPages(int datasetId) {
        return MetadataIndexImmutableProperties.isMetadataDataset(datasetId)
                ? storageProperties.getMetadataMemoryComponentNumPages()
                : storageProperties.getMemoryComponentNumPages();
    }

    public JsonNode getState() {
        final ObjectNode state = JSONUtil.createObject();
        state.put("availableBudget", available);
        state.set("allocated", budgetMapToJsonArray(allocatedMap));
        state.set("reserved", budgetMapToJsonArray(reservedMap));
        return state;
    }

    private long getTotalSize(int datasetId) {
        return storageProperties.getMemoryComponentPageSize() * (long) getNumPages(datasetId);
    }

    private boolean isAllocatable(long required) {
        return available - required >= 0;
    }

    private void allocateReserved(int datasetId) {
        final Long reserved = reservedMap.get(datasetId);
        allocatedMap.put(datasetId, reserved);
    }

    private static ArrayNode budgetMapToJsonArray(Map<Integer, Long> memorytMap) {
        final ArrayNode array = JSONUtil.createArray();
        memorytMap.forEach((k, v) -> {
            final ObjectNode dataset = JSONUtil.createObject();
            dataset.put("datasetId", k);
            dataset.put("budget", v);
            array.add(dataset);
        });
        return array;
    }
}
