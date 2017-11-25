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
import java.nio.file.Paths;
import java.util.function.Function;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.storage.IndexPathElements;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.MappedFileSplit;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class StoragePathUtil {
    private static final Logger LOGGER = Logger.getLogger(StoragePathUtil.class.getName());
    public static final String PARTITION_DIR_PREFIX = "partition_";
    public static final String DATASET_INDEX_NAME_SEPARATOR = "_idx_";
    private static Function<IndexPathElements, String> indexPathProvider;

    private StoragePathUtil() {
    }

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
        return new MappedFileSplit(partition.getActiveNodeId(), relativePath, partition.getIODeviceNum());
    }

    public static String prepareStoragePartitionPath(String storageDirName, int partitonId) {
        return storageDirName + File.separator + StoragePathUtil.PARTITION_DIR_PREFIX + partitonId;
    }

    public static String prepareDataverseIndexName(String dataverseName, String datasetName, String idxName,
            long rebalanceCount) {
        return prepareDataverseIndexName(dataverseName, prepareFullIndexName(datasetName, idxName, rebalanceCount));
    }

    public static String prepareDataverseIndexName(String dataverseName, String fullIndexName) {
        return dataverseName + File.separator + fullIndexName;
    }

    private static String prepareFullIndexName(String datasetName, String idxName, long rebalanceCount) {
        if (indexPathProvider != null) {
            return indexPathProvider.apply(new IndexPathElements(datasetName, idxName, String.valueOf(rebalanceCount)));
        }
        return datasetName + File.separator + rebalanceCount + File.separator + idxName;
    }

    public static int getPartitionNumFromName(String name) {
        return Integer.parseInt(name.substring(PARTITION_DIR_PREFIX.length()));
    }

    public static int getPartitionNumFromRelativePath(String relativePath) {
        int startIdx = relativePath.indexOf(PARTITION_DIR_PREFIX) + PARTITION_DIR_PREFIX.length();
        String partition = relativePath.substring(startIdx, relativePath.indexOf(File.separatorChar, startIdx));
        return Integer.parseInt(partition);
    }

    /**
     * @param fileAbsolutePath
     * @return the file relative path starting from the partition directory
     */
    public static String getIndexFileRelativePath(String fileAbsolutePath) {
        return ResourceReference.of(fileAbsolutePath).getRelativePath().toString();
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
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Gets the index name part in the index relative path.
     *
     * @param path
     * @return The index name
     */
    public static String getIndexNameFromPath(String path) {
        return Paths.get(path).getFileName().toString();
    }

    public static void setIndexPathProvider(Function<IndexPathElements, String> indexPathProvider) {
        StoragePathUtil.indexPathProvider = indexPathProvider;
    }
}
