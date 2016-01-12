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
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataConstants;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class SplitsAndConstraintsUtil {

    public static final String PARTITION_DIR_PREFIX = "partition_";
    public static final String TEMP_DATASETS_STORAGE_FOLDER = "temp";
    public static final String DATASET_INDEX_NAME_SEPARATOR = "_idx_";

    private static FileSplit[] splitsForDataverse(String dataverseName) {
        File relPathFile = new File(dataverseName);
        List<FileSplit> splits = new ArrayList<FileSplit>();
        //get all partitions
        ClusterPartition[] clusterPartition = AsterixClusterProperties.INSTANCE.getClusterPartitons();
        String storageDirName = AsterixClusterProperties.INSTANCE.getStorageDirectoryName();
        for (int j = 0; j < clusterPartition.length; j++) {
            int nodeParitions = AsterixClusterProperties.INSTANCE
                    .getNodePartitionsCount(clusterPartition[j].getNodeId());
            for (int i = 0; i < nodeParitions; i++) {
                File f = new File(prepareStoragePartitionPath(storageDirName, clusterPartition[i].getPartitionId())
                        + File.separator + relPathFile);
                splits.add(getFileSplitForClusterPartition(clusterPartition[j], f));
            }
        }
        return splits.toArray(new FileSplit[] {});
    }

    public static FileSplit[] splitsForDataset(MetadataTransactionContext mdTxnCtx, String dataverseName,
            String datasetName, String targetIdxName, boolean temp) throws AlgebricksException {
        try {
            File relPathFile = new File(prepareDataverseIndexName(dataverseName, datasetName, targetIdxName));
            Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            List<String> nodeGroup = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, dataset.getNodeGroupName())
                    .getNodeNames();
            if (nodeGroup == null) {
                throw new AlgebricksException("Couldn't find node group " + dataset.getNodeGroupName());
            }

            String storageDirName = AsterixClusterProperties.INSTANCE.getStorageDirectoryName();
            List<FileSplit> splits = new ArrayList<FileSplit>();
            for (String nd : nodeGroup) {
                int numPartitions = AsterixClusterProperties.INSTANCE.getNodePartitionsCount(nd);
                ClusterPartition[] nodePartitions = AsterixClusterProperties.INSTANCE.getNodePartitions(nd);
                //currently this case is never executed since the metadata group doesn't exists
                if (dataset.getNodeGroupName().compareTo(MetadataConstants.METADATA_NODEGROUP_NAME) == 0) {
                    numPartitions = 1;
                }

                for (int k = 0; k < numPartitions; k++) {
                    //format: 'storage dir name'/partition_#/dataverse/dataset_idx_index
                    //temp format: 'storage dir name'/temp/partition_#/dataverse/dataset_idx_index
                    File f = new File(prepareStoragePartitionPath(
                            storageDirName + (temp ? (File.separator + TEMP_DATASETS_STORAGE_FOLDER) : ""),
                            nodePartitions[k].getPartitionId()) + File.separator + relPathFile);
                    splits.add(getFileSplitForClusterPartition(nodePartitions[k], f));
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
            File relPathFile = new File(prepareDataverseIndexName(dataverseName, datasetName, targetIdxName));
            Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            List<String> nodeGroup = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, dataset.getNodeGroupName())
                    .getNodeNames();
            if (nodeGroup == null) {
                throw new AlgebricksException("Couldn't find node group " + dataset.getNodeGroupName());
            }

            List<FileSplit> splits = new ArrayList<FileSplit>();
            for (String nodeId : nodeGroup) {
                //get node partitions
                ClusterPartition[] nodePartitions = AsterixClusterProperties.INSTANCE.getNodePartitions(nodeId);
                String storageDirName = AsterixClusterProperties.INSTANCE.getStorageDirectoryName();
                int firstPartition = 0;
                if (create) {
                    // Only the first partition when create
                    File f = new File(
                            prepareStoragePartitionPath(storageDirName, nodePartitions[firstPartition].getPartitionId())
                                    + File.separator + relPathFile);
                    splits.add(getFileSplitForClusterPartition(nodePartitions[firstPartition], f));
                } else {
                    for (int k = 0; k < nodePartitions.length; k++) {
                        File f = new File(prepareStoragePartitionPath(storageDirName,
                                nodePartitions[firstPartition].getPartitionId()) + File.separator + relPathFile);
                        splits.add(getFileSplitForClusterPartition(nodePartitions[firstPartition], f));
                    }
                }
            }
            return splits.toArray(new FileSplit[] {});
        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    public static Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraintsForDataverse(
            String dataverse) {
        FileSplit[] splits = splitsForDataverse(dataverse);
        return splitProviderAndPartitionConstraints(splits);
    }

    public static Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraintsForFilesIndex(
            MetadataTransactionContext mdTxnCtx, String dataverseName, String datasetName, String targetIdxName,
            boolean create) throws AlgebricksException {
        FileSplit[] splits = splitsForFilesIndex(mdTxnCtx, dataverseName, datasetName, targetIdxName, create);
        return splitProviderAndPartitionConstraints(splits);
    }

    public static Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraints(
            FileSplit[] splits) {
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(splits);
        String[] loc = new String[splits.length];
        for (int p = 0; p < splits.length; p++) {
            loc[p] = splits[p].getNodeName();
        }
        AlgebricksPartitionConstraint pc = new AlgebricksAbsolutePartitionConstraint(loc);
        return new Pair<IFileSplitProvider, AlgebricksPartitionConstraint>(splitProvider, pc);
    }

    private static FileSplit getFileSplitForClusterPartition(ClusterPartition partition, File relativeFile) {
        return new FileSplit(partition.getActiveNodeId(), new FileReference(relativeFile), partition.getIODeviceNum(),
                partition.getPartitionId());
    }

    public static String prepareStoragePartitionPath(String storageDirName, int partitonId) {
        return storageDirName + File.separator + PARTITION_DIR_PREFIX + partitonId;
    }

    private static String prepareDataverseIndexName(String dataverseName, String datasetName, String idxName) {
        return dataverseName + File.separator + datasetName + DATASET_INDEX_NAME_SEPARATOR + idxName;
    }
}
