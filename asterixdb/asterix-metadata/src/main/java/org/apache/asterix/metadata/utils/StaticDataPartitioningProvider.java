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
package org.apache.asterix.metadata.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.cluster.ComputePartition;
import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.cluster.SplitComputeLocations;
import org.apache.asterix.common.cluster.StorageComputePartitionsMap;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.MappedFileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class StaticDataPartitioningProvider extends DataPartitioningProvider {

    public StaticDataPartitioningProvider(ICcApplicationContext appCtx) {
        super(appCtx);
    }

    @Override
    public PartitioningProperties getPartitioningProperties(String databaseName) {
        SplitComputeLocations dataverseSplits = getSplits(databaseName);
        StorageComputePartitionsMap partitionMap = clusterStateManager.getStorageComputeMap();
        int[][] partitionsMap = partitionMap.getComputeToStorageMap(false);
        return PartitioningProperties.of(dataverseSplits.getSplitsProvider(), dataverseSplits.getConstraints(),
                partitionsMap);
    }

    @Override
    public PartitioningProperties getPartitioningProperties(String databaseName, DataverseName dataverseName) {
        SplitComputeLocations dataverseSplits = getDataverseSplits(databaseName, dataverseName);
        StorageComputePartitionsMap partitionMap = clusterStateManager.getStorageComputeMap();
        int[][] partitionsMap = partitionMap.getComputeToStorageMap(false);
        return PartitioningProperties.of(dataverseSplits.getSplitsProvider(), dataverseSplits.getConstraints(),
                partitionsMap);
    }

    @Override
    public PartitioningProperties getPartitioningProperties(MetadataTransactionContext mdTxnCtx, Dataset ds,
            String indexName) throws AlgebricksException {
        SplitComputeLocations datasetSplits = getDatasetSplits(ds, indexName);
        StorageComputePartitionsMap partitionMap = clusterStateManager.getStorageComputeMap();
        int[][] partitionsMap = partitionMap
                .getComputeToStorageMap(MetadataIndexImmutableProperties.isMetadataDataset(ds.getDatasetId()));
        return PartitioningProperties.of(datasetSplits.getSplitsProvider(), datasetSplits.getConstraints(),
                partitionsMap);
    }

    private SplitComputeLocations getDataverseSplits(String databaseName, DataverseName dataverseName) {
        return getSplits(namespacePathResolver.resolve(databaseName, dataverseName));
    }

    private SplitComputeLocations getSplits(String subPath) {
        List<FileSplit> splits = new ArrayList<>();
        List<String> locations = new ArrayList<>();
        Set<Integer> uniqueLocations = new HashSet<>();
        StorageComputePartitionsMap partitionMap = clusterStateManager.getStorageComputeMap();
        for (int i = 0; i < storagePartitionsCounts; i++) {
            File f = new File(StoragePathUtil.prepareStoragePartitionPath(i), subPath);
            ComputePartition computePartition = partitionMap.getComputePartition(i);
            splits.add(new MappedFileSplit(computePartition.getNodeId(), f.getPath(), 0));
            if (!uniqueLocations.contains(computePartition.getId())) {
                locations.add(computePartition.getNodeId());
            }
            uniqueLocations.add(computePartition.getId());
        }
        IFileSplitProvider splitProvider = StoragePathUtil.splitProvider(splits.toArray(new FileSplit[0]));
        AlgebricksPartitionConstraint constraints =
                new AlgebricksAbsolutePartitionConstraint(locations.toArray(new String[0]));
        return new SplitComputeLocations(splitProvider, constraints);
    }

    private SplitComputeLocations getDatasetSplits(Dataset dataset, String indexName) {
        List<FileSplit> splits = new ArrayList<>();
        List<String> locations = new ArrayList<>();
        Set<Integer> uniqueLocations = new HashSet<>();
        String namespacePath = namespacePathResolver.resolve(dataset.getDatabaseName(), dataset.getDataverseName());
        StorageComputePartitionsMap partitionMap = clusterStateManager.getStorageComputeMap();
        final int datasetPartitions = getNumberOfPartitions(dataset);
        boolean metadataDataset = MetadataIndexImmutableProperties.isMetadataDataset(dataset.getDatasetId());
        for (int i = 0; i < datasetPartitions; i++) {
            int storagePartition = metadataDataset ? StorageConstants.METADATA_PARTITION : i;
            final String relPath = StoragePathUtil.prepareNamespaceIndexName(dataset.getDatasetName(), indexName,
                    dataset.getRebalanceCount(), namespacePath);
            File f = new File(StoragePathUtil.prepareStoragePartitionPath(storagePartition), relPath);
            ComputePartition computePartition = partitionMap.getComputePartition(storagePartition);
            splits.add(new MappedFileSplit(computePartition.getNodeId(), f.getPath(), 0));
            int computePartitionId = computePartition.getId();
            if (!uniqueLocations.contains(computePartitionId)) {
                locations.add(computePartition.getNodeId());
            }
            uniqueLocations.add(computePartitionId);
        }
        IFileSplitProvider splitProvider = StoragePathUtil.splitProvider(splits.toArray(new FileSplit[0]));
        AlgebricksPartitionConstraint constraints =
                new AlgebricksAbsolutePartitionConstraint(locations.toArray(new String[0]));
        return new SplitComputeLocations(splitProvider, constraints);
    }
}
