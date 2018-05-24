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
package org.apache.asterix.external.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.indexing.IndexingScheduler;
import org.apache.asterix.external.indexing.RecordId.RecordIdType;
import org.apache.asterix.external.input.stream.HDFSInputStream;
import org.apache.asterix.hivecompat.io.RCFileInputFormat;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.context.ICCContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.hdfs.scheduler.Scheduler;

public class HDFSUtils {

    public static Scheduler initializeHDFSScheduler(ICCServiceContext serviceCtx) throws HyracksDataException {
        ICCContext ccContext = serviceCtx.getCCContext();
        Scheduler scheduler = null;
        try {
            scheduler = new Scheduler(ccContext.getClusterControllerInfo().getClientNetAddress(),
                    ccContext.getClusterControllerInfo().getClientNetPort());
        } catch (HyracksException e) {
            throw new RuntimeDataException(ErrorCode.UTIL_HDFS_UTILS_CANNOT_OBTAIN_HDFS_SCHEDULER);
        }
        return scheduler;
    }

    public static IndexingScheduler initializeIndexingHDFSScheduler(ICCServiceContext serviceCtx)
            throws HyracksDataException {
        IndexingScheduler scheduler = null;
        try {
            ICcApplicationContext appCtx = (ICcApplicationContext) serviceCtx.getApplicationContext();
            IHyracksClientConnection hcc = appCtx.getHcc();
            scheduler = new IndexingScheduler(hcc.getNodeControllerInfos());
        } catch (HyracksException e) {
            throw new RuntimeDataException(ErrorCode.UTIL_HDFS_UTILS_CANNOT_OBTAIN_HDFS_SCHEDULER);
        }
        return scheduler;
    }

    /**
     * Instead of creating the split using the input format, we do it manually
     * This function returns fileSplits (1 per hdfs file block) irrespective of the number of partitions
     * and the produced splits only cover intersection between current files in hdfs and files stored internally
     * in AsterixDB
     * 1. NoOp means appended file
     * 2. AddOp means new file
     * 3. UpdateOp means the delta of a file
     *
     * @return
     * @throws IOException
     */
    public static InputSplit[] getSplits(JobConf conf, List<ExternalFile> files) throws IOException {
        // Create file system object
        FileSystem fs = FileSystem.get(conf);
        ArrayList<FileSplit> fileSplits = new ArrayList<>();
        ArrayList<ExternalFile> orderedExternalFiles = new ArrayList<>();
        // Create files splits
        for (ExternalFile file : files) {
            Path filePath = new Path(file.getFileName());
            FileStatus fileStatus;
            try {
                fileStatus = fs.getFileStatus(filePath);
            } catch (FileNotFoundException e) {
                // file was deleted at some point, skip to next file
                continue;
            }
            if (file.getPendingOp() == ExternalFilePendingOp.ADD_OP
                    && fileStatus.getModificationTime() == file.getLastModefiedTime().getTime()) {
                // Get its information from HDFS name node
                BlockLocation[] fileBlocks = fs.getFileBlockLocations(fileStatus, 0, file.getSize());
                // Create a split per block
                for (BlockLocation block : fileBlocks) {
                    if (block.getOffset() < file.getSize()) {
                        fileSplits
                                .add(new FileSplit(filePath,
                                        block.getOffset(), (block.getLength() + block.getOffset()) < file.getSize()
                                                ? block.getLength() : (file.getSize() - block.getOffset()),
                                        block.getHosts()));
                        orderedExternalFiles.add(file);
                    }
                }
            } else if (file.getPendingOp() == ExternalFilePendingOp.NO_OP
                    && fileStatus.getModificationTime() == file.getLastModefiedTime().getTime()) {
                long oldSize = 0L;
                long newSize = file.getSize();
                for (int i = 0; i < files.size(); i++) {
                    if (files.get(i).getFileName() == file.getFileName() && files.get(i).getSize() != file.getSize()) {
                        newSize = files.get(i).getSize();
                        oldSize = file.getSize();
                        break;
                    }
                }

                // Get its information from HDFS name node
                BlockLocation[] fileBlocks = fs.getFileBlockLocations(fileStatus, 0, newSize);
                // Create a split per block
                for (BlockLocation block : fileBlocks) {
                    if (block.getOffset() + block.getLength() > oldSize) {
                        if (block.getOffset() < newSize) {
                            // Block interact with delta -> Create a split
                            long startCut = (block.getOffset() > oldSize) ? 0L : oldSize - block.getOffset();
                            long endCut = (block.getOffset() + block.getLength() < newSize) ? 0L
                                    : block.getOffset() + block.getLength() - newSize;
                            long splitLength = block.getLength() - startCut - endCut;
                            fileSplits.add(new FileSplit(filePath, block.getOffset() + startCut, splitLength,
                                    block.getHosts()));
                            orderedExternalFiles.add(file);
                        }
                    }
                }
            }
        }
        fs.close();
        files.clear();
        files.addAll(orderedExternalFiles);
        return fileSplits.toArray(new FileSplit[fileSplits.size()]);
    }

    public static String getInputFormatClassName(Map<String, String> configuration) {
        String inputFormatParameter = configuration.get(ExternalDataConstants.KEY_INPUT_FORMAT).trim();
        switch (inputFormatParameter) {
            case ExternalDataConstants.INPUT_FORMAT_TEXT:
                return ExternalDataConstants.CLASS_NAME_TEXT_INPUT_FORMAT;
            case ExternalDataConstants.INPUT_FORMAT_SEQUENCE:
                return ExternalDataConstants.CLASS_NAME_SEQUENCE_INPUT_FORMAT;
            case ExternalDataConstants.INPUT_FORMAT_RC:
                return ExternalDataConstants.CLASS_NAME_RC_INPUT_FORMAT;
            default:
                return inputFormatParameter;
        }
    }

    public static Class<?> getInputFormatClass(Map<String, String> configuration) throws ClassNotFoundException {
        String inputFormatParameter = configuration.get(ExternalDataConstants.KEY_INPUT_FORMAT).trim();
        switch (inputFormatParameter) {
            case ExternalDataConstants.INPUT_FORMAT_TEXT:
                return TextInputFormat.class;
            case ExternalDataConstants.INPUT_FORMAT_SEQUENCE:
                return SequenceFileInputFormat.class;
            case ExternalDataConstants.INPUT_FORMAT_RC:
                return RCFileInputFormat.class;
            default:
                return Class.forName(inputFormatParameter);
        }
    }

    public static JobConf configureHDFSJobConf(Map<String, String> configuration) {
        JobConf conf = new JobConf();

        String localShortCircuitSocketPath = configuration.get(ExternalDataConstants.KEY_LOCAL_SOCKET_PATH);
        String formatClassName = HDFSUtils.getInputFormatClassName(configuration);
        conf.set(ExternalDataConstants.KEY_HADOOP_FILESYSTEM_URI,
                configuration.get(ExternalDataConstants.KEY_HDFS_URL).trim());
        conf.set(ExternalDataConstants.KEY_HADOOP_FILESYSTEM_CLASS, ExternalDataConstants.CLASS_NAME_HDFS_FILESYSTEM);
        conf.setClassLoader(HDFSInputStream.class.getClassLoader());
        conf.set(ExternalDataConstants.KEY_HADOOP_INPUT_DIR, configuration.get(ExternalDataConstants.KEY_PATH).trim());
        conf.set(ExternalDataConstants.KEY_HADOOP_INPUT_FORMAT, formatClassName);

        // Enable local short circuit reads if user supplied the parameters
        if (localShortCircuitSocketPath != null) {
            conf.set(ExternalDataConstants.KEY_HADOOP_SHORT_CIRCUIT, "true");
            conf.set(ExternalDataConstants.KEY_HADOOP_SOCKET_PATH, localShortCircuitSocketPath.trim());
        }
        return conf;
    }

    public static AlgebricksAbsolutePartitionConstraint getPartitionConstraints(IApplicationContext appCtx,
            AlgebricksAbsolutePartitionConstraint clusterLocations) {
        if (clusterLocations == null) {
            IClusterStateManager clusterStateManager = ((ICcApplicationContext) appCtx).getClusterStateManager();
            ArrayList<String> locs = new ArrayList<>();
            Map<String, String[]> stores = appCtx.getMetadataProperties().getStores();
            for (String node : stores.keySet()) {

                int numIODevices = clusterStateManager.getIODevices(node).length;
                for (int k = 0; k < numIODevices; k++) {
                    locs.add(node);
                }
            }
            String[] cluster = new String[locs.size()];
            cluster = locs.toArray(cluster);
            return new AlgebricksAbsolutePartitionConstraint(cluster);
        }
        return clusterLocations;
    }

    public static RecordIdType getRecordIdType(Map<String, String> configuration) {
        String inputFormatParameter = configuration.get(ExternalDataConstants.KEY_INPUT_FORMAT).trim();
        switch (inputFormatParameter) {
            case ExternalDataConstants.INPUT_FORMAT_TEXT:
            case ExternalDataConstants.INPUT_FORMAT_SEQUENCE:
                return RecordIdType.OFFSET;
            case ExternalDataConstants.INPUT_FORMAT_RC:
                return RecordIdType.RC;
            default:
                return null;
        }
    }
}
