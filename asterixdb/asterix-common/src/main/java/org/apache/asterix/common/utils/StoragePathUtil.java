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

import static org.apache.asterix.common.utils.StorageConstants.PARTITION_DIR_PREFIX;
import static org.apache.asterix.common.utils.StorageConstants.STORAGE_ROOT_DIR_NAME;

import java.io.File;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.DefaultIoDeviceFileSplit;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.MappedFileSplit;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StoragePathUtil {

    private static final Logger LOGGER = LogManager.getLogger();
    public static final char DATAVERSE_CONTINUATION_MARKER = '^';
    private static final String PARTITION_PATH = STORAGE_ROOT_DIR_NAME + File.separator + PARTITION_DIR_PREFIX;

    private StoragePathUtil() {
    }

    public static IFileSplitProvider splitProvider(FileSplit[] splits) {
        return new ConstantFileSplitProvider(splits);
    }

    public static Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraints(
            FileSplit[] splits) {
        IFileSplitProvider splitProvider = splitProvider(splits);
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

    public static FileSplit getDefaultIoDeviceFileSpiltForNode(String nodeId, String relativePath) {
        return new DefaultIoDeviceFileSplit(nodeId, relativePath);
    }

    public static String prepareStoragePartitionPath(int partitionId) {
        return Paths.get(StorageConstants.STORAGE_ROOT_DIR_NAME, PARTITION_DIR_PREFIX + partitionId).toString();
    }

    public static String prepareIngestionLogPath() {
        return Paths.get(StorageConstants.INGESTION_LOGS_DIR_NAME).toString();
    }

    public static String prepareNamespaceIndexName(String datasetName, String idxName, long rebalanceCount,
            String namespacePath) {
        return prepareNamespaceComponentName(namespacePath, prepareFullIndexName(datasetName, idxName, rebalanceCount));
    }

    public static String prepareDataverseName(DataverseName dataverseName) {
        List<String> parts = dataverseName.getParts();
        if (parts.size() < 2) {
            return parts.get(0);
        }
        Iterator<String> dvParts = parts.iterator();
        StringBuilder builder = new StringBuilder();
        builder.append(dvParts.next());
        while (dvParts.hasNext()) {
            builder.append(File.separatorChar).append(DATAVERSE_CONTINUATION_MARKER).append(dvParts.next());
        }
        return builder.toString();
    }

    public static String prepareNamespaceComponentName(String namespacePath, String component) {
        return namespacePath + File.separatorChar + component;
    }

    private static String prepareFullIndexName(String datasetName, String idxName, long rebalanceCount) {
        return datasetName + File.separator + rebalanceCount + File.separator + idxName;
    }

    public static int getPartitionNumFromRelativePath(String relativePath) {
        int startIdx = relativePath.lastIndexOf(PARTITION_DIR_PREFIX) + PARTITION_DIR_PREFIX.length();
        int partitionEndIdx = relativePath.indexOf(File.separatorChar, startIdx);
        int idxEnd = partitionEndIdx != -1 ? partitionEndIdx : relativePath.length();
        String partition = relativePath.substring(startIdx, idxEnd);
        return Integer.parseInt(partition);
    }

    /**
     * @param fileAbsolutePath
     * @return the file's index relative path starting from the storage directory
     */
    public static String getIndexFileRelativePath(String fileAbsolutePath) {
        return ResourceReference.of(fileAbsolutePath).getRelativePath().toString();
    }

    /**
     * @param fileAbsolutePath
     * @return the file's relative path starting from the storage directory
     */
    public static String getFileRelativePath(String fileAbsolutePath) {
        return ResourceReference.of(fileAbsolutePath).getFileRelativePath().toString();
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
                    if (LOGGER.isInfoEnabled()) {
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

    /**
     * Get the path of the index containing the passed reference
     *
     * @param ioManager
     * @param ref
     * @return
     * @throws HyracksDataException
     */
    public static FileReference getIndexPath(IIOManager ioManager, ResourceReference ref) throws HyracksDataException {
        return ioManager.resolve(ref.getRelativePath().toString());
    }

    /**
     * Returns the index's path after the partition directory
     * Example:
     * - Input:
     * /../storage/partition_8/dataverse_p1[/^dataverse_p2[/^dataverse_p3...]]/dataset/rebalanceCount/index/0_0_b
     * - Output
     * dataverse_p1[/^dataverse_p2[/^dataverse_p3...]]/dataset/rebalanceCount/index
     *
     * @param fileReference a file inside the index director
     * @param isDirectory   if the provided {@link FileReference} corresponds to a directory
     * @return index path
     */
    public static String getIndexSubPath(FileReference fileReference, boolean isDirectory) {
        String relativePath = fileReference.getRelativePath();
        if (relativePath.length() <= PARTITION_PATH.length() || !relativePath.startsWith(PARTITION_PATH)) {
            return "";
        }
        String partition = PARTITION_PATH + getPartitionNumFromRelativePath(relativePath);
        int partitionStart = relativePath.indexOf(partition);
        int start = partitionStart + partition.length() + 1;
        int end = isDirectory ? relativePath.length() : relativePath.lastIndexOf('/');
        if (start >= end) {
            // This could happen if the provided path contains only a partition path (e.g., storage/partition_0)
            return "";
        }
        return relativePath.substring(start, end);
    }

    public static boolean hasSameStorageRoot(FileReference file1, FileReference file2) {
        return file1.getDeviceHandle().equals(file2.getDeviceHandle());
    }

    public static boolean isRelativeParent(FileReference parent, FileReference child) {
        return child.getRelativePath().startsWith(parent.getRelativePath());
    }
}
