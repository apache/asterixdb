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

import static org.apache.asterix.common.utils.PartitioningScheme.DYNAMIC;
import static org.apache.asterix.common.utils.PartitioningScheme.STATIC;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.dataflow.IDataPartitioningProvider;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.utils.PartitioningScheme;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class DataPartitioningProvider implements IDataPartitioningProvider {

    private final ICcApplicationContext appCtx;
    private final PartitioningScheme scheme;

    public DataPartitioningProvider(ICcApplicationContext appCtx) {
        this.appCtx = appCtx;
        scheme = appCtx.getStorageProperties().getPartitioningScheme();
    }

    public PartitioningProperties getPartitioningProperties(DataverseName dataverseName) {
        if (scheme == DYNAMIC) {
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraints = SplitsAndConstraintsUtil
                    .getDataverseSplitProviderAndConstraints(appCtx.getClusterStateManager(), dataverseName);
            int[][] partitionsMap = getPartitionsMap(getNumPartitions(splitsAndConstraints.second));
            return PartitioningProperties.of(splitsAndConstraints.first, splitsAndConstraints.second, partitionsMap);
        } else if (scheme == STATIC) {
            throw new NotImplementedException();
        }
        throw new IllegalStateException();
    }

    public PartitioningProperties getPartitioningProperties(MetadataTransactionContext mdTxnCtx, Dataset ds,
            String indexName) throws AlgebricksException {
        if (scheme == DYNAMIC) {
            FileSplit[] splits =
                    SplitsAndConstraintsUtil.getIndexSplits(ds, indexName, mdTxnCtx, appCtx.getClusterStateManager());
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraints =
                    StoragePathUtil.splitProviderAndPartitionConstraints(splits);
            int[][] partitionsMap = getPartitionsMap(getNumPartitions(splitsAndConstraints.second));
            return PartitioningProperties.of(splitsAndConstraints.first, splitsAndConstraints.second, partitionsMap);
        } else if (scheme == STATIC) {
            throw new NotImplementedException();
        }
        throw new IllegalStateException();
    }

    public PartitioningProperties getPartitioningProperties(Feed feed) throws AsterixException {
        if (scheme == DYNAMIC) {
            IClusterStateManager csm = appCtx.getClusterStateManager();
            AlgebricksAbsolutePartitionConstraint allCluster = csm.getClusterLocations();
            Set<String> nodes = new TreeSet<>(Arrays.asList(allCluster.getLocations()));
            AlgebricksAbsolutePartitionConstraint locations =
                    new AlgebricksAbsolutePartitionConstraint(nodes.toArray(new String[0]));
            FileSplit[] feedLogFileSplits =
                    FeedUtils.splitsForAdapter(appCtx, feed.getDataverseName(), feed.getFeedName(), locations);
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spC =
                    StoragePathUtil.splitProviderAndPartitionConstraints(feedLogFileSplits);
            int[][] partitionsMap = getPartitionsMap(getNumPartitions(spC.second));
            return PartitioningProperties.of(spC.first, spC.second, partitionsMap);
        } else if (scheme == STATIC) {
            throw new NotImplementedException();
        }
        throw new IllegalStateException();
    }

    private static int getNumPartitions(AlgebricksPartitionConstraint constraint) {
        if (constraint.getPartitionConstraintType() == AlgebricksPartitionConstraint.PartitionConstraintType.COUNT) {
            return ((AlgebricksCountPartitionConstraint) constraint).getCount();
        } else {
            return ((AlgebricksAbsolutePartitionConstraint) constraint).getLocations().length;
        }
    }

    private static int[][] getPartitionsMap(int numPartitions) {
        int[][] map = new int[numPartitions][1];
        for (int i = 0; i < numPartitions; i++) {
            map[i] = new int[] { i };
        }
        return map;
    }
}
