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
package org.apache.asterix.common.utils;

import java.io.File;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class StoragePathUtil {
    public static final String PARTITION_DIR_PREFIX = "partition_";
    public static final String TEMP_DATASETS_STORAGE_FOLDER = "temp";
    public static final String DATASET_INDEX_NAME_SEPARATOR = "_idx_";
    public static final String ADAPTER_INSTANCE_PREFIX = "adapter_";

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

    public static FileSplit getFileSplitForClusterPartition(ClusterPartition partition, File relativeFile) {
        return new FileSplit(partition.getActiveNodeId(), new FileReference(relativeFile), partition.getIODeviceNum(),
                partition.getPartitionId());
    }

    public static String prepareStoragePartitionPath(String storageDirName, int partitonId) {
        return storageDirName + File.separator + StoragePathUtil.PARTITION_DIR_PREFIX + partitonId;
    }

    public static String prepareDataverseIndexName(String dataverseName, String datasetName, String idxName) {
        return dataverseName + File.separator + datasetName + StoragePathUtil.DATASET_INDEX_NAME_SEPARATOR + idxName;
    }
}
