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

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.asterix.common.api.INamespacePathResolver;
import org.apache.asterix.common.api.INamespaceResolver;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.dataflow.IDataPartitioningProvider;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.common.utils.PartitioningScheme;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.runtime.utils.ClusterStateManager;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public abstract class DataPartitioningProvider implements IDataPartitioningProvider {

    protected final ICcApplicationContext appCtx;
    protected final INamespacePathResolver namespacePathResolver;
    protected final INamespaceResolver namespaceResolver;
    protected final ClusterStateManager clusterStateManager;
    protected final int storagePartitionsCounts;

    DataPartitioningProvider(ICcApplicationContext appCtx) {
        this.appCtx = appCtx;
        this.namespacePathResolver = appCtx.getNamespacePathResolver();
        this.namespaceResolver = appCtx.getNamespaceResolver();
        this.clusterStateManager = (ClusterStateManager) appCtx.getClusterStateManager();
        this.storagePartitionsCounts = clusterStateManager.getStoragePartitionsCount();
    }

    public static DataPartitioningProvider create(ICcApplicationContext appCtx) {
        PartitioningScheme partitioningScheme = appCtx.getStorageProperties().getPartitioningScheme();
        switch (partitioningScheme) {
            case DYNAMIC:
                return new DynamicDataPartitioningProvider(appCtx);
            case STATIC:
                return new StaticDataPartitioningProvider(appCtx);
            default:
                throw new IllegalStateException("unknown partitioning scheme: " + partitioningScheme);
        }
    }

    public abstract PartitioningProperties getPartitioningProperties(String databaseName);

    public abstract PartitioningProperties getPartitioningProperties(String databaseName, DataverseName dataverseName);

    public abstract PartitioningProperties getPartitioningProperties(MetadataTransactionContext mdTxnCtx, Dataset ds,
            String indexName) throws AlgebricksException;

    public PartitioningProperties getPartitioningProperties(Feed feed) throws AsterixException {
        IClusterStateManager csm = appCtx.getClusterStateManager();
        AlgebricksAbsolutePartitionConstraint allCluster = csm.getClusterLocations();
        Set<String> nodes = new TreeSet<>(Arrays.asList(allCluster.getLocations()));
        AlgebricksAbsolutePartitionConstraint locations =
                new AlgebricksAbsolutePartitionConstraint(nodes.toArray(new String[0]));
        String namespacePath = namespacePathResolver.resolve(feed.getDatabaseName(), feed.getDataverseName());
        FileSplit[] feedLogFileSplits = FeedUtils.splitsForAdapter(namespacePath, feed.getFeedName(), locations);
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spC =
                StoragePathUtil.splitProviderAndPartitionConstraints(feedLogFileSplits);
        int[][] partitionsMap = getOneToOnePartitionsMap(getLocationsCount(spC.second));
        return PartitioningProperties.of(spC.first, spC.second, partitionsMap);
    }

    protected int getNumberOfPartitions(Dataset ds) {
        return MetadataIndexImmutableProperties.isMetadataDataset(ds.getDatasetId())
                ? MetadataIndexImmutableProperties.METADATA_DATASETS_PARTITIONS : storagePartitionsCounts;
    }

    protected static int getLocationsCount(AlgebricksPartitionConstraint constraint) {
        if (constraint.getPartitionConstraintType() == AlgebricksPartitionConstraint.PartitionConstraintType.COUNT) {
            return ((AlgebricksCountPartitionConstraint) constraint).getCount();
        } else {
            return ((AlgebricksAbsolutePartitionConstraint) constraint).getLocations().length;
        }
    }

    protected static int[][] getOneToOnePartitionsMap(int numPartitions) {
        int[][] map = new int[numPartitions][1];
        for (int i = 0; i < numPartitions; i++) {
            map[i] = new int[] { i };
        }
        return map;
    }
}
