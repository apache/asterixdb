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
package org.apache.hyracks.control.cc;

import java.io.File;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.resource.NodeCapacity;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.common.controllers.NodeRegistration;
import org.apache.hyracks.control.common.heartbeat.HeartbeatData;
import org.apache.hyracks.control.common.heartbeat.HeartbeatSchema;
import org.apache.hyracks.control.common.heartbeat.HeartbeatSchema.GarbageCollectorInfo;
import org.apache.hyracks.control.common.ipc.NodeControllerRemoteProxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class NodeControllerState {
    private static final int RRD_SIZE = 720;

    private final NodeControllerRemoteProxy nodeController;

    private final NCConfig ncConfig;

    private final NetworkAddress dataPort;

    private final NetworkAddress datasetPort;

    private final NetworkAddress messagingPort;

    private final Set<JobId> activeJobIds;

    private final String osName;

    private final String arch;

    private final String osVersion;

    private final int nProcessors;

    private final String vmName;

    private final String vmVersion;

    private final String vmVendor;

    private final String classpath;

    private final String libraryPath;

    private final String bootClasspath;

    private final List<String> inputArguments;

    private final Map<String, String> systemProperties;

    private final int pid;

    private final HeartbeatSchema hbSchema;

    private final long[] hbTime;

    private final long[] heapInitSize;

    private final long[] heapUsedSize;

    private final long[] heapCommittedSize;

    private final long[] heapMaxSize;

    private final long[] nonheapInitSize;

    private final long[] nonheapUsedSize;

    private final long[] nonheapCommittedSize;

    private final long[] nonheapMaxSize;

    private final int[] threadCount;

    private final int[] peakThreadCount;

    private final double[] systemLoadAverage;

    private final String[] gcNames;

    private final long[][] gcCollectionCounts;

    private final long[][] gcCollectionTimes;

    private final long[] netPayloadBytesRead;

    private final long[] netPayloadBytesWritten;

    private final long[] netSignalingBytesRead;

    private final long[] netSignalingBytesWritten;

    private final long[] datasetNetPayloadBytesRead;

    private final long[] datasetNetPayloadBytesWritten;

    private final long[] datasetNetSignalingBytesRead;

    private final long[] datasetNetSignalingBytesWritten;

    private final long[] ipcMessagesSent;

    private final long[] ipcMessageBytesSent;

    private final long[] ipcMessagesReceived;

    private final long[] ipcMessageBytesReceived;

    private final long[] diskReads;

    private final long[] diskWrites;

    private int rrdPtr;

    private int lastHeartbeatDuration;

    private NodeCapacity capacity;

    public NodeControllerState(NodeControllerRemoteProxy nodeController, NodeRegistration reg) {
        this.nodeController = nodeController;
        ncConfig = reg.getNCConfig();
        dataPort = reg.getDataPort();
        datasetPort = reg.getDatasetPort();
        messagingPort = reg.getMessagingPort();
        activeJobIds = new HashSet<>();

        osName = reg.getOSName();
        arch = reg.getArch();
        osVersion = reg.getOSVersion();
        nProcessors = reg.getNProcessors();
        vmName = reg.getVmName();
        vmVersion = reg.getVmVersion();
        vmVendor = reg.getVmVendor();
        classpath = reg.getClasspath();
        libraryPath = reg.getLibraryPath();
        bootClasspath = reg.getBootClasspath();
        inputArguments = reg.getInputArguments();
        systemProperties = reg.getSystemProperties();
        pid = reg.getPid();

        hbSchema = reg.getHeartbeatSchema();

        hbTime = new long[RRD_SIZE];
        heapInitSize = new long[RRD_SIZE];
        heapUsedSize = new long[RRD_SIZE];
        heapCommittedSize = new long[RRD_SIZE];
        heapMaxSize = new long[RRD_SIZE];
        nonheapInitSize = new long[RRD_SIZE];
        nonheapUsedSize = new long[RRD_SIZE];
        nonheapCommittedSize = new long[RRD_SIZE];
        nonheapMaxSize = new long[RRD_SIZE];
        threadCount = new int[RRD_SIZE];
        peakThreadCount = new int[RRD_SIZE];
        systemLoadAverage = new double[RRD_SIZE];
        GarbageCollectorInfo[] gcInfos = hbSchema.getGarbageCollectorInfos();
        int gcN = gcInfos.length;
        gcNames = new String[gcN];
        for (int i = 0; i < gcN; ++i) {
            gcNames[i] = gcInfos[i].getName();
        }
        gcCollectionCounts = new long[gcN][RRD_SIZE];
        gcCollectionTimes = new long[gcN][RRD_SIZE];
        netPayloadBytesRead = new long[RRD_SIZE];
        netPayloadBytesWritten = new long[RRD_SIZE];
        netSignalingBytesRead = new long[RRD_SIZE];
        netSignalingBytesWritten = new long[RRD_SIZE];
        datasetNetPayloadBytesRead = new long[RRD_SIZE];
        datasetNetPayloadBytesWritten = new long[RRD_SIZE];
        datasetNetSignalingBytesRead = new long[RRD_SIZE];
        datasetNetSignalingBytesWritten = new long[RRD_SIZE];
        ipcMessagesSent = new long[RRD_SIZE];
        ipcMessageBytesSent = new long[RRD_SIZE];
        ipcMessagesReceived = new long[RRD_SIZE];
        ipcMessageBytesReceived = new long[RRD_SIZE];

        diskReads = new long[RRD_SIZE];
        diskWrites = new long[RRD_SIZE];

        rrdPtr = 0;
        capacity = reg.getCapacity();
    }

    public synchronized void notifyHeartbeat(HeartbeatData hbData) {
        lastHeartbeatDuration = 0;
        hbTime[rrdPtr] = System.currentTimeMillis();
        if (hbData != null) {
            heapInitSize[rrdPtr] = hbData.heapInitSize;
            heapUsedSize[rrdPtr] = hbData.heapUsedSize;
            heapCommittedSize[rrdPtr] = hbData.heapCommittedSize;
            heapMaxSize[rrdPtr] = hbData.heapMaxSize;
            nonheapInitSize[rrdPtr] = hbData.nonheapInitSize;
            nonheapUsedSize[rrdPtr] = hbData.nonheapUsedSize;
            nonheapCommittedSize[rrdPtr] = hbData.nonheapCommittedSize;
            nonheapMaxSize[rrdPtr] = hbData.nonheapMaxSize;
            threadCount[rrdPtr] = hbData.threadCount;
            peakThreadCount[rrdPtr] = hbData.peakThreadCount;
            systemLoadAverage[rrdPtr] = hbData.systemLoadAverage;
            int gcN = hbSchema.getGarbageCollectorInfos().length;
            for (int i = 0; i < gcN; ++i) {
                gcCollectionCounts[i][rrdPtr] = hbData.gcCollectionCounts[i];
                gcCollectionTimes[i][rrdPtr] = hbData.gcCollectionTimes[i];
            }
            netPayloadBytesRead[rrdPtr] = hbData.netPayloadBytesRead;
            netPayloadBytesWritten[rrdPtr] = hbData.netPayloadBytesWritten;
            netSignalingBytesRead[rrdPtr] = hbData.netSignalingBytesRead;
            netSignalingBytesWritten[rrdPtr] = hbData.netSignalingBytesWritten;
            datasetNetPayloadBytesRead[rrdPtr] = hbData.datasetNetPayloadBytesRead;
            datasetNetPayloadBytesWritten[rrdPtr] = hbData.datasetNetPayloadBytesWritten;
            datasetNetSignalingBytesRead[rrdPtr] = hbData.datasetNetSignalingBytesRead;
            datasetNetSignalingBytesWritten[rrdPtr] = hbData.datasetNetSignalingBytesWritten;
            ipcMessagesSent[rrdPtr] = hbData.ipcMessagesSent;
            ipcMessageBytesSent[rrdPtr] = hbData.ipcMessageBytesSent;
            ipcMessagesReceived[rrdPtr] = hbData.ipcMessagesReceived;
            ipcMessageBytesReceived[rrdPtr] = hbData.ipcMessageBytesReceived;
            diskReads[rrdPtr] = hbData.diskReads;
            diskWrites[rrdPtr] = hbData.diskWrites;
            rrdPtr = (rrdPtr + 1) % RRD_SIZE;
        }
    }

    public int incrementLastHeartbeatDuration() {
        return lastHeartbeatDuration++;
    }

    public NodeControllerRemoteProxy getNodeController() {
        return nodeController;
    }

    public NCConfig getNCConfig() {
        return ncConfig;
    }

    public Set<JobId> getActiveJobIds() {
        return activeJobIds;
    }

    public NetworkAddress getDataPort() {
        return dataPort;
    }

    public NetworkAddress getDatasetPort() {
        return datasetPort;
    }

    public NetworkAddress getMessagingPort() {
        return messagingPort;
    }

    public NodeCapacity getCapacity() {
        return capacity;
    }

    public synchronized ObjectNode toSummaryJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode o = om.createObjectNode();
        o.put("node-id", ncConfig.getNodeId());
        o.put("heap-used", heapUsedSize[(rrdPtr + RRD_SIZE - 1) % RRD_SIZE]);
        o.put("system-load-average", systemLoadAverage[(rrdPtr + RRD_SIZE - 1) % RRD_SIZE]);

        return o;
    }

    public synchronized ObjectNode toDetailedJSON(boolean includeStats, boolean includeConfig) {
        ObjectMapper om = new ObjectMapper();
        ObjectNode o = om.createObjectNode();

        o.put("node-id", ncConfig.getNodeId());

        if (includeConfig) {
            o.put("os-name", osName);
            o.put("arch", arch);
            o.put("os-version", osVersion);
            o.put("num-processors", nProcessors);
            o.put("vm-name", vmName);
            o.put("vm-version", vmVersion);
            o.put("vm-vendor", vmVendor);
            o.putPOJO("classpath", classpath.split(File.pathSeparator));
            o.putPOJO("library-path", libraryPath.split(File.pathSeparator));
            o.putPOJO("boot-classpath", bootClasspath.split(File.pathSeparator));
            o.putPOJO("input-arguments", inputArguments);
            o.putPOJO("system-properties", systemProperties);
            o.put("pid", pid);
        }
        if (includeStats) {
            o.putPOJO("date", new Date());
            o.put("rrd-ptr", rrdPtr);
            o.putPOJO("heartbeat-times", hbTime);
            o.putPOJO("heap-init-sizes", heapInitSize);
            o.putPOJO("heap-used-sizes", heapUsedSize);
            o.putPOJO("heap-committed-sizes", heapCommittedSize);
            o.putPOJO("heap-max-sizes", heapMaxSize);
            o.putPOJO("nonheap-init-sizes", nonheapInitSize);
            o.putPOJO("nonheap-used-sizes", nonheapUsedSize);
            o.putPOJO("nonheap-committed-sizes", nonheapCommittedSize);
            o.putPOJO("nonheap-max-sizes", nonheapMaxSize);
            o.putPOJO("application-memory-budget", capacity.getMemoryByteSize());
            o.putPOJO("application-cpu-core-budget", capacity.getCores());
            o.putPOJO("thread-counts", threadCount);
            o.putPOJO("peak-thread-counts", peakThreadCount);
            o.putPOJO("system-load-averages", systemLoadAverage);
            o.putPOJO("gc-names", gcNames);
            o.putPOJO("gc-collection-counts", gcCollectionCounts);
            o.putPOJO("gc-collection-times", gcCollectionTimes);
            o.putPOJO("net-payload-bytes-read", netPayloadBytesRead);
            o.putPOJO("net-payload-bytes-written", netPayloadBytesWritten);
            o.putPOJO("net-signaling-bytes-read", netSignalingBytesRead);
            o.putPOJO("net-signaling-bytes-written", netSignalingBytesWritten);
            o.putPOJO("dataset-net-payload-bytes-read", datasetNetPayloadBytesRead);
            o.putPOJO("dataset-net-payload-bytes-written", datasetNetPayloadBytesWritten);
            o.putPOJO("dataset-net-signaling-bytes-read", datasetNetSignalingBytesRead);
            o.putPOJO("dataset-net-signaling-bytes-written", datasetNetSignalingBytesWritten);
            o.putPOJO("ipc-messages-sent", ipcMessagesSent);
            o.putPOJO("ipc-message-bytes-sent", ipcMessageBytesSent);
            o.putPOJO("ipc-messages-received", ipcMessagesReceived);
            o.putPOJO("ipc-message-bytes-received", ipcMessageBytesReceived);
            o.putPOJO("disk-reads", diskReads);
            o.putPOJO("disk-writes", diskWrites);
        }

        return o;
    }

}
