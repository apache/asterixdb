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

import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class DynamicDataPartitioningProvider extends DataPartitioningProvider {

    public DynamicDataPartitioningProvider(ICcApplicationContext appCtx) {
        super(appCtx);
    }

    @Override
    public PartitioningProperties getPartitioningProperties(String databaseName) {
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraints = SplitsAndConstraintsUtil
                .getDatabaseSplitProviderAndConstraints(appCtx.getClusterStateManager(), databaseName);
        int[][] partitionsMap = getOneToOnePartitionsMap(getLocationsCount(splitsAndConstraints.second));
        return PartitioningProperties.of(splitsAndConstraints.first, splitsAndConstraints.second, partitionsMap);
    }

    @Override
    public PartitioningProperties getPartitioningProperties(String databaseName, DataverseName dataverseName) {
        String namespacePath = namespacePathResolver.resolve(databaseName, dataverseName);
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraints = SplitsAndConstraintsUtil
                .getDataverseSplitProviderAndConstraints(appCtx.getClusterStateManager(), namespacePath);
        int[][] partitionsMap = getOneToOnePartitionsMap(getLocationsCount(splitsAndConstraints.second));
        return PartitioningProperties.of(splitsAndConstraints.first, splitsAndConstraints.second, partitionsMap);
    }

    @Override
    public PartitioningProperties getPartitioningProperties(MetadataTransactionContext mdTxnCtx, Dataset ds,
            String indexName) throws AlgebricksException {
        String namespacePath = namespacePathResolver.resolve(ds.getDatabaseName(), ds.getDataverseName());
        FileSplit[] splits = SplitsAndConstraintsUtil.getIndexSplits(ds, indexName, mdTxnCtx,
                appCtx.getClusterStateManager(), namespacePath);
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraints =
                StoragePathUtil.splitProviderAndPartitionConstraints(splits);
        int[][] partitionsMap = getOneToOnePartitionsMap(getLocationsCount(splitsAndConstraints.second));
        return PartitioningProperties.of(splitsAndConstraints.first, splitsAndConstraints.second, partitionsMap);
    }
}
