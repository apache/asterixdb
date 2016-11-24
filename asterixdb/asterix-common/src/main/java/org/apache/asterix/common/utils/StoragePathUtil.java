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
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class StoragePathUtil {
    private static final Logger LOGGER = Logger.getLogger(StoragePathUtil.class.getName());
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
        return new Pair<>(splitProvider, pc);
    }

    public static FileSplit getFileSplitForClusterPartition(ClusterPartition partition, String relativePath) {
        return new FileSplit(partition.getActiveNodeId(), relativePath, true);
    }

    public static String prepareStoragePartitionPath(String storageDirName, int partitonId) {
        return storageDirName + File.separator + StoragePathUtil.PARTITION_DIR_PREFIX + partitonId;
    }

    public static String prepareDataverseIndexName(String dataverseName, String datasetName, String idxName) {
        return prepareDataverseIndexName(dataverseName, prepareFullIndexName(datasetName, idxName));
    }

    public static String prepareDataverseIndexName(String dataverseName, String fullIndexName) {
        return dataverseName + File.separator + fullIndexName;
    }

    private static String prepareFullIndexName(String datasetName, String idxName) {
        return (datasetName + DATASET_INDEX_NAME_SEPARATOR + idxName);
    }

    public static int getPartitionNumFromName(String name) {
        return Integer.parseInt(name.substring(PARTITION_DIR_PREFIX.length()));
    }

    /**
     * Create a file
     * Note: this method is not thread safe. It is the responsibility of the caller to ensure no path conflict when
     * creating files simultaneously
     *
     * @param name
     * @param count
     * @return
     * @throws HyracksDataException
     */
    public static File createFile(String name, int count) throws HyracksDataException {
        try {
            String fileName = name + "_" + count;
            File file = new File(fileName);
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            if (!file.exists()) {
                boolean success = file.createNewFile();
                if (!success) {
                    throw new HyracksDataException("Unable to create spill file " + fileName);
                } else {
                    if (LOGGER.isEnabledFor(Level.INFO)) {
                        LOGGER.info("Created spill file " + file.getAbsolutePath());
                    }
                }
            } else {
                throw new HyracksDataException("spill file " + fileName + " already exists");
            }
            return file;
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
