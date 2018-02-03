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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class SplitsAndConstraintsUtil {

    private SplitsAndConstraintsUtil() {
    }

    private static FileSplit[] getDataverseSplits(IClusterStateManager clusterStateManager, String dataverseName) {
        List<FileSplit> splits = new ArrayList<>();
        // get all partitions
        ClusterPartition[] clusterPartition = clusterStateManager.getClusterPartitons();
        for (int j = 0; j < clusterPartition.length; j++) {
            File f = new File(StoragePathUtil.prepareStoragePartitionPath(clusterPartition[j].getPartitionId()),
                    dataverseName);
            splits.add(StoragePathUtil.getFileSplitForClusterPartition(clusterPartition[j], f.getPath()));
        }
        return splits.toArray(new FileSplit[] {});
    }

    public static FileSplit[] getIndexSplits(Dataset dataset, String indexName, MetadataTransactionContext mdTxnCtx,
            IClusterStateManager csm) throws AlgebricksException {
        try {
            NodeGroup nodeGroup = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, dataset.getNodeGroupName());
            if (nodeGroup == null) {
                throw new AlgebricksException("Couldn't find node group " + dataset.getNodeGroupName());
            }
            List<String> nodeList = nodeGroup.getNodeNames();
            return getIndexSplits(csm, dataset, indexName, nodeList);
        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    public static FileSplit[] getIndexSplits(IClusterStateManager clusterStateManager, Dataset dataset,
            String indexName, List<String> nodes) {
        final String relPath = StoragePathUtil.prepareDataverseIndexName(dataset.getDataverseName(),
                dataset.getDatasetName(), indexName, dataset.getRebalanceCount());
        final List<ClusterPartition> datasetPartitions = getDatasetPartitions(clusterStateManager, dataset, nodes);
        final List<FileSplit> splits = new ArrayList<>();
        for (ClusterPartition partition : datasetPartitions) {
            File f = new File(StoragePathUtil.prepareStoragePartitionPath(partition.getPartitionId()), relPath);
            splits.add(StoragePathUtil.getFileSplitForClusterPartition(partition, f.getPath()));
        }
        return splits.toArray(new FileSplit[] {});
    }

    public static Pair<IFileSplitProvider, AlgebricksPartitionConstraint> getDataverseSplitProviderAndConstraints(
            IClusterStateManager clusterStateManager, String dataverse) {
        FileSplit[] splits = getDataverseSplits(clusterStateManager, dataverse);
        return StoragePathUtil.splitProviderAndPartitionConstraints(splits);
    }

    private static List<ClusterPartition> getDatasetPartitions(IClusterStateManager clusterStateManager,
            Dataset dataset, List<String> nodes) {
        if (dataset.getNodeGroupName().compareTo(MetadataConstants.METADATA_NODEGROUP_NAME) == 0) {
            return Collections.singletonList(clusterStateManager.getMetadataPartition());
        }
        final List<ClusterPartition> datasetPartitions = new ArrayList<>();
        for (String node : nodes) {
            final ClusterPartition[] nodePartitions = clusterStateManager.getNodePartitions(node);
            datasetPartitions.addAll(Arrays.asList(nodePartitions));
        }
        return datasetPartitions;
    }
}
