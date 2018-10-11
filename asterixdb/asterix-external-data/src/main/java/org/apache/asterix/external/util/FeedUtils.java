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

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint.PartitionConstraintType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.util.IntSerDeUtils;

public class FeedUtils {

    public static final String FEED_EXTENSION_NAME = "Feed";
    private static final String FEED_HOST_MODE_NC = "NC";
    private static final String FEED_HOST_MODE_IP = "IP";

    public enum JobType {
        INTAKE,
        FEED_CONNECT
    }

    public enum FeedRuntimeType {
        INTAKE,
        COLLECT,
        COMPUTE_COLLECT,
        COMPUTE,
        STORE,
        OTHER,
        ETS,
        JOIN
    }

    public enum Mode {
        PROCESS, // There is memory
        SPILL, // Memory budget has been consumed. Now we're writing to disk
        DISCARD // Memory and Disk space budgets have been consumed. Now we're discarding
    }

    private FeedUtils() {
    }

    private static String prepareDataverseFeedName(String dataverseName, String feedName) {
        return dataverseName + File.separator + feedName;
    }

    public static FileSplit splitsForAdapter(String dataverseName, String feedName, String nodeName,
            ClusterPartition partition) {
        File relPathFile = new File(prepareDataverseFeedName(dataverseName, feedName));
        String storagePartitionPath = StoragePathUtil.prepareStoragePartitionPath(partition.getPartitionId());
        // Note: feed adapter instances in a single node share the feed logger
        // format: 'storage dir name'/partition_#/dataverse/feed/node
        File f = new File(storagePartitionPath + File.separator + relPathFile + File.separator + nodeName);
        return StoragePathUtil.getFileSplitForClusterPartition(partition, f.getPath());
    }

    public static FileSplit[] splitsForAdapter(ICcApplicationContext appCtx, String dataverseName, String feedName,
            AlgebricksPartitionConstraint partitionConstraints) throws AsterixException {
        if (partitionConstraints.getPartitionConstraintType() == PartitionConstraintType.COUNT) {
            throw new AsterixException("Can't create file splits for adapter with count partitioning constraints");
        }
        String[] locations = ((AlgebricksAbsolutePartitionConstraint) partitionConstraints).getLocations();
        List<FileSplit> splits = new ArrayList<>();
        for (String nd : locations) {
            splits.add(splitsForAdapter(dataverseName, feedName, nd,
                    appCtx.getClusterStateManager().getNodePartitions(nd)[0]));
        }
        return splits.toArray(new FileSplit[] {});
    }

    public static FileReference getAbsoluteFileRef(String relativePath, int ioDeviceId, IIOManager ioManager) {
        return ioManager.getFileReference(ioDeviceId, relativePath);
    }

    public static FeedLogManager getFeedLogManager(IHyracksTaskContext ctx, int partition,
            FileSplit[] feedLogFileSplits) throws HyracksDataException {
        return new FeedLogManager(
                FeedUtils.getAbsoluteFileRef(feedLogFileSplits[partition].getPath(), 0, ctx.getIoManager()).getFile());
    }

    public static FeedLogManager getFeedLogManager(IHyracksTaskContext ctx, FileSplit feedLogFileSplit)
            throws HyracksDataException {
        return new FeedLogManager(
                FeedUtils.getAbsoluteFileRef(feedLogFileSplit.getPath(), 0, ctx.getIoManager()).getFile());
    }

    public static void processFeedMessage(ByteBuffer input, VSizeFrame message, FrameTupleAccessor fta)
            throws HyracksDataException {
        // read the message and reduce the number of tuples
        fta.reset(input);
        int tc = fta.getTupleCount() - 1;
        int offset = fta.getTupleStartOffset(tc);
        int len = fta.getTupleLength(tc);
        int newSize = FrameHelper.calcAlignedFrameSizeToStore(1, len, message.getMinSize());
        message.reset();
        message.ensureFrameSize(newSize);
        message.getBuffer().clear();
        message.getBuffer().put(input.array(), offset, len);
        message.getBuffer().flip();
        IntSerDeUtils.putInt(input.array(), FrameHelper.getTupleCountOffset(input.capacity()), tc);
    }

    public static String getFeedMetaTypeName(Map<String, String> configuration) {
        return configuration.get(ExternalDataConstants.KEY_META_TYPE_NAME);

    }

    public static List<Pair<String, Integer>> extractHostsPorts(String modeValue, IServiceContext serviceCtx,
            String hostsValue) throws AlgebricksException {
        try {
            List<Pair<String, Integer>> sockets = new ArrayList<>();
            String mode = modeValue.trim().toUpperCase();
            if (hostsValue == null) {
                throw new CompilationException(ErrorCode.FEED_METADATA_SOCKET_ADAPTOR_SOCKET_NOT_PROPERLY_CONFIGURED);
            }
            Map<InetAddress, Set<String>> ncMap =
                    RuntimeUtils.getNodeControllerMap((ICcApplicationContext) serviceCtx.getApplicationContext());
            List<String> ncs =
                    RuntimeUtils.getAllNodeControllers((ICcApplicationContext) serviceCtx.getApplicationContext());
            String[] socketsArray = hostsValue.split(",");
            Random random = new Random();
            for (String socket : socketsArray) {
                String[] socketTokens = socket.split(":");
                String host = socketTokens[0].trim();
                int port = Integer.parseInt(socketTokens[1].trim());
                Pair<String, Integer> p = null;
                if (FEED_HOST_MODE_IP.equals(mode)) {
                    Set<String> ncsOnIp = ncMap.get(InetAddress.getByName(host));
                    if ((ncsOnIp == null) || ncsOnIp.isEmpty()) {
                        throw new CompilationException(ErrorCode.FEED_METADATA_SOCKET_ADAPTOR_SOCKET_INVALID_HOST_NC,
                                FEED_HOST_MODE_IP, host, StringUtils.join(ncMap.keySet(), ", "));
                    }
                    String[] ncArray = ncsOnIp.toArray(new String[] {});
                    String nc = ncArray[random.nextInt(ncArray.length)];
                    p = Pair.of(nc, port);
                } else if (FEED_HOST_MODE_NC.equals(mode)) {
                    p = Pair.of(host, port);
                    if (!ncs.contains(host)) {
                        throw new CompilationException(ErrorCode.FEED_METADATA_SOCKET_ADAPTOR_SOCKET_INVALID_HOST_NC,
                                FEED_HOST_MODE_NC, host, StringUtils.join(ncs, ", "));

                    }
                }
                sockets.add(p);
            }
            return sockets;
        } catch (HyracksDataException | UnknownHostException e) {
            throw new AlgebricksException(e);
        }
    }

    public static AlgebricksAbsolutePartitionConstraint addressToAbsolutePartitionConstraints(
            List<Pair<String, Integer>> sockets) {
        List<String> locations = new ArrayList<>();
        for (Pair<String, Integer> socket : sockets) {
            locations.add(socket.getLeft());
        }
        return new AlgebricksAbsolutePartitionConstraint(locations.toArray(new String[] {}));
    }
}
