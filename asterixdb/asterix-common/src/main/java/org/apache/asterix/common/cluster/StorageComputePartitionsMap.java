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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.utils.StorageConstants;

public class StorageComputePartitionsMap {

    private final Map<Integer, ComputePartition> stoToComputeLocation = new HashMap<>();

    public void addStoragePartition(int stoPart, ComputePartition compute) {
        stoToComputeLocation.put(stoPart, compute);
    }

    public int[][] getComputeToStorageMap(boolean metadataDataset) {
        Map<Integer, List<Integer>> computeToStoragePartitions = new HashMap<>();
        if (metadataDataset) {
            final int computePartitionIdForMetadata = 0;
            computeToStoragePartitions.put(computePartitionIdForMetadata,
                    Collections.singletonList(computePartitionIdForMetadata));
        } else {
            for (int i = 0; i < StorageConstants.NUM_STORAGE_PARTITIONS; i++) {
                ComputePartition computePartition = getComputePartition(i);
                int computeId = computePartition.getId();
                List<Integer> storagePartitions =
                        computeToStoragePartitions.computeIfAbsent(computeId, k -> new ArrayList<>());
                storagePartitions.add(i);
            }
        }
        int[][] computerToStoArray = new int[computeToStoragePartitions.size()][];
        for (Map.Entry<Integer, List<Integer>> integerListEntry : computeToStoragePartitions.entrySet()) {
            computerToStoArray[integerListEntry.getKey()] =
                    integerListEntry.getValue().stream().mapToInt(i -> i).toArray();
        }
        return computerToStoArray;
    }

    public ComputePartition getComputePartition(int storagePartition) {
        return stoToComputeLocation.get(storagePartition);
    }

    public static StorageComputePartitionsMap computePartitionsMap(IClusterStateManager clusterStateManager) {
        ClusterPartition metadataPartition = clusterStateManager.getMetadataPartition();
        Map<Integer, ClusterPartition> clusterPartitions = clusterStateManager.getClusterPartitions();
        StorageComputePartitionsMap newMap = new StorageComputePartitionsMap();
        newMap.addStoragePartition(metadataPartition.getPartitionId(),
                new ComputePartition(metadataPartition.getPartitionId(), metadataPartition.getActiveNodeId()));
        int storagePartitionsPerComputePartition = StorageConstants.NUM_STORAGE_PARTITIONS / clusterPartitions.size();
        int storagePartitionId = 0;
        int lastComputePartition = 1;
        int remainingStoragePartition = StorageConstants.NUM_STORAGE_PARTITIONS % clusterPartitions.size();
        for (Map.Entry<Integer, ClusterPartition> cp : clusterPartitions.entrySet()) {
            ClusterPartition clusterPartition = cp.getValue();
            for (int i = 0; i < storagePartitionsPerComputePartition; i++) {
                newMap.addStoragePartition(storagePartitionId,
                        new ComputePartition(clusterPartition.getPartitionId(), clusterPartition.getActiveNodeId()));
                storagePartitionId++;
            }
            if (lastComputePartition == clusterPartitions.size() && remainingStoragePartition != 0) {
                // assign all remaining partitions to last compute partition
                for (int k = 0; k < remainingStoragePartition; k++) {
                    newMap.addStoragePartition(storagePartitionId, new ComputePartition(
                            clusterPartition.getPartitionId(), clusterPartition.getActiveNodeId()));
                    storagePartitionId++;
                }
            }
            lastComputePartition++;
        }
        return newMap;
    }
}
