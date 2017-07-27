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
import java.util.List;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.runtime.utils.ClusterStateManager;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class SplitsAndConstraintsUtil {

    private SplitsAndConstraintsUtil() {
    }

    private static FileSplit[] getDataverseSplits(String dataverseName) {
        File relPathFile = new File(dataverseName);
        List<FileSplit> splits = new ArrayList<>();
        // get all partitions
        ClusterPartition[] clusterPartition = ClusterStateManager.INSTANCE.getClusterPartitons();
        String storageDirName = ClusterProperties.INSTANCE.getStorageDirectoryName();
        for (int j = 0; j < clusterPartition.length; j++) {
            File f = new File(
                    StoragePathUtil.prepareStoragePartitionPath(storageDirName, clusterPartition[j].getPartitionId())
                            + File.separator + relPathFile);
            splits.add(StoragePathUtil.getFileSplitForClusterPartition(clusterPartition[j], f.getPath()));
        }
        return splits.toArray(new FileSplit[] {});
    }

    public static FileSplit[] getIndexSplits(Dataset dataset, String indexName, MetadataTransactionContext mdTxnCtx)
            throws AlgebricksException {
        try {
            NodeGroup nodeGroup = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, dataset.getNodeGroupName());
            if (nodeGroup == null) {
                throw new AlgebricksException("Couldn't find node group " + dataset.getNodeGroupName());
            }
            List<String> nodeList = nodeGroup.getNodeNames();
            return getIndexSplits(dataset, indexName, nodeList);
        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    public static FileSplit[] getIndexSplits(Dataset dataset, String indexName, List<String> nodes) {
        File relPathFile = new File(StoragePathUtil.prepareDataverseIndexName(dataset.getDataverseName(),
                dataset.getDatasetName(), indexName, dataset.getRebalanceCount()));
        String storageDirName = ClusterProperties.INSTANCE.getStorageDirectoryName();
        List<FileSplit> splits = new ArrayList<>();
        for (String nd : nodes) {
            int numPartitions = ClusterStateManager.INSTANCE.getNodePartitionsCount(nd);
            ClusterPartition[] nodePartitions = ClusterStateManager.INSTANCE.getNodePartitions(nd);
            // currently this case is never executed since the metadata group doesn't exists
            if (dataset.getNodeGroupName().compareTo(MetadataConstants.METADATA_NODEGROUP_NAME) == 0) {
                numPartitions = 1;
            }

            for (int k = 0; k < numPartitions; k++) {
                // format: 'storage dir name'/partition_#/dataverse/dataset_idx_index
                File f = new File(StoragePathUtil.prepareStoragePartitionPath(storageDirName,
                        nodePartitions[k].getPartitionId())
                        + (dataset.isTemp() ? (File.separator + StoragePathUtil.TEMP_DATASETS_STORAGE_FOLDER) : "")
                        + File.separator + relPathFile);
                splits.add(StoragePathUtil.getFileSplitForClusterPartition(nodePartitions[k], f.getPath()));
            }
        }
        return splits.toArray(new FileSplit[] {});
    }

    public static Pair<IFileSplitProvider, AlgebricksPartitionConstraint> getDataverseSplitProviderAndConstraints(
            String dataverse) {
        FileSplit[] splits = getDataverseSplits(dataverse);
        return StoragePathUtil.splitProviderAndPartitionConstraints(splits);
    }

    public static String getIndexPath(String partitionPath, int partition, String dataverse, String fullIndexName) {
        String storageDirName = ClusterProperties.INSTANCE.getStorageDirectoryName();
        return partitionPath + StoragePathUtil.prepareStoragePartitionPath(storageDirName, partition) + File.separator
                + StoragePathUtil.prepareDataverseIndexName(dataverse, fullIndexName);
    }
}
