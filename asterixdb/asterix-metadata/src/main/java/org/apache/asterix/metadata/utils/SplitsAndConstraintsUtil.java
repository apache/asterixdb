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
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.runtime.util.ClusterStateManager;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class SplitsAndConstraintsUtil {

    private SplitsAndConstraintsUtil() {
    }

    private static FileSplit[] splitsForDataverse(String dataverseName) {
        File relPathFile = new File(dataverseName);
        List<FileSplit> splits = new ArrayList<>();
        // get all partitions
        ClusterPartition[] clusterPartition = ClusterStateManager.INSTANCE.getClusterPartitons();
        String storageDirName = ClusterProperties.INSTANCE.getStorageDirectoryName();
        for (int j = 0; j < clusterPartition.length; j++) {
            int nodePartitions =
                    ClusterStateManager.INSTANCE.getNodePartitionsCount(clusterPartition[j].getNodeId());
            for (int i = 0; i < nodePartitions; i++) {
                File f = new File(StoragePathUtil.prepareStoragePartitionPath(storageDirName,
                        clusterPartition[i].getPartitionId()) + File.separator + relPathFile);
                splits.add(StoragePathUtil.getFileSplitForClusterPartition(clusterPartition[j], f));
            }
        }
        return splits.toArray(new FileSplit[] {});
    }

    public static FileSplit[] splitsForDataset(MetadataTransactionContext mdTxnCtx, String dataverseName,
            String datasetName, String targetIdxName, boolean temp) throws AlgebricksException {
        try {
            File relPathFile =
                    new File(StoragePathUtil.prepareDataverseIndexName(dataverseName, datasetName, targetIdxName));
            Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            List<String> nodeGroup =
                    MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, dataset.getNodeGroupName()).getNodeNames();
            if (nodeGroup == null) {
                throw new AlgebricksException("Couldn't find node group " + dataset.getNodeGroupName());
            }

            String storageDirName = ClusterProperties.INSTANCE.getStorageDirectoryName();
            List<FileSplit> splits = new ArrayList<>();
            for (String nd : nodeGroup) {
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
                            + (temp ? (File.separator + StoragePathUtil.TEMP_DATASETS_STORAGE_FOLDER) : "")
                            + File.separator + relPathFile);
                    splits.add(StoragePathUtil.getFileSplitForClusterPartition(nodePartitions[k], f));
                }
            }
            return splits.toArray(new FileSplit[] {});
        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    private static FileSplit[] splitsForFilesIndex(MetadataTransactionContext mdTxnCtx, String dataverseName,
            String datasetName, String targetIdxName, boolean create) throws AlgebricksException {
        try {
            File relPathFile =
                    new File(StoragePathUtil.prepareDataverseIndexName(dataverseName, datasetName, targetIdxName));
            Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            List<String> nodeGroup =
                    MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, dataset.getNodeGroupName()).getNodeNames();
            if (nodeGroup == null) {
                throw new AlgebricksException("Couldn't find node group " + dataset.getNodeGroupName());
            }

            List<FileSplit> splits = new ArrayList<>();
            for (String nodeId : nodeGroup) {
                // get node partitions
                ClusterPartition[] nodePartitions = ClusterStateManager.INSTANCE.getNodePartitions(nodeId);
                String storageDirName = ClusterProperties.INSTANCE.getStorageDirectoryName();
                int firstPartition = 0;
                if (create) {
                    // Only the first partition when create
                    File f = new File(StoragePathUtil.prepareStoragePartitionPath(storageDirName,
                            nodePartitions[firstPartition].getPartitionId()) + File.separator + relPathFile);
                    splits.add(StoragePathUtil.getFileSplitForClusterPartition(nodePartitions[firstPartition], f));
                } else {
                    for (int k = 0; k < nodePartitions.length; k++) {
                        File f = new File(StoragePathUtil.prepareStoragePartitionPath(storageDirName,
                                nodePartitions[firstPartition].getPartitionId()) + File.separator + relPathFile);
                        splits.add(StoragePathUtil.getFileSplitForClusterPartition(nodePartitions[firstPartition], f));
                    }
                }
            }
            return splits.toArray(new FileSplit[] {});
        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    public static Pair<IFileSplitProvider, AlgebricksPartitionConstraint>
            splitProviderAndPartitionConstraintsForDataverse(String dataverse) {
        FileSplit[] splits = splitsForDataverse(dataverse);
        return StoragePathUtil.splitProviderAndPartitionConstraints(splits);
    }

    public static Pair<IFileSplitProvider, AlgebricksPartitionConstraint>
            splitProviderAndPartitionConstraintsForFilesIndex(MetadataTransactionContext mdTxnCtx, String dataverseName,
                    String datasetName, String targetIdxName, boolean create) throws AlgebricksException {
        FileSplit[] splits = splitsForFilesIndex(mdTxnCtx, dataverseName, datasetName, targetIdxName, create);
        return StoragePathUtil.splitProviderAndPartitionConstraints(splits);
    }

    public static String getIndexPath(String partitionPath, int partition, String dataverse, String fullIndexName) {
        String storageDirName = ClusterProperties.INSTANCE.getStorageDirectoryName();
        return partitionPath + StoragePathUtil.prepareStoragePartitionPath(storageDirName, partition) + File.separator
                + StoragePathUtil.prepareDataverseIndexName(dataverse, fullIndexName);
    }
}
