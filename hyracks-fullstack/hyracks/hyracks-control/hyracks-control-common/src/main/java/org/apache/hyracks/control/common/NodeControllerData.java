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
package org.apache.hyracks.control.common;

import static org.apache.hyracks.control.common.utils.ConfigurationUtil.toPathElements;
import static org.apache.hyracks.util.JSONUtil.put;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.config.SerializedOption;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.resource.NodeCapacity;
import org.apache.hyracks.control.common.controllers.NodeRegistration;
import org.apache.hyracks.control.common.heartbeat.HeartbeatData;
import org.apache.hyracks.control.common.heartbeat.HeartbeatSchema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class NodeControllerData {

    private static final int RRD_SIZE = 720;

    private final String nodeId;

    private final Map<SerializedOption, Object> config;

    private final NetworkAddress dataPort;

    private final NetworkAddress resultPort;

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

    private final long pid;

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

    private final long[] resultNetPayloadBytesRead;

    private final long[] resultNetPayloadBytesWritten;

    private final long[] resultNetSignalingBytesRead;

    private final long[] resultNetSignalingBytesWritten;

    private final long[] ipcMessagesSent;

    private final long[] ipcMessageBytesSent;

    private final long[] ipcMessagesReceived;

    private final long[] ipcMessageBytesReceived;

    private final long[] diskReads;

    private final long[] diskWrites;

    private int rrdPtr;

    private volatile long lastHeartbeatNanoTime;

    private NodeCapacity capacity;

    public NodeControllerData(NodeRegistration reg) {
        nodeId = reg.getNodeId();
        config = Collections.unmodifiableMap(reg.getConfig());

        dataPort = reg.getDataPort();
        resultPort = reg.getResultPort();
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
        HeartbeatSchema.GarbageCollectorInfo[] gcInfos = hbSchema.getGarbageCollectorInfos();
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
        resultNetPayloadBytesRead = new long[RRD_SIZE];
        resultNetPayloadBytesWritten = new long[RRD_SIZE];
        resultNetSignalingBytesRead = new long[RRD_SIZE];
        resultNetSignalingBytesWritten = new long[RRD_SIZE];
        ipcMessagesSent = new long[RRD_SIZE];
        ipcMessageBytesSent = new long[RRD_SIZE];
        ipcMessagesReceived = new long[RRD_SIZE];
        ipcMessageBytesReceived = new long[RRD_SIZE];

        diskReads = new long[RRD_SIZE];
        diskWrites = new long[RRD_SIZE];

        rrdPtr = 0;
        capacity = reg.getCapacity();
        touchHeartbeat();
    }

    public synchronized void notifyHeartbeat(HeartbeatData hbData) {
        touchHeartbeat();
        hbTime[rrdPtr] = System.currentTimeMillis();
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
        resultNetPayloadBytesRead[rrdPtr] = hbData.resultNetPayloadBytesRead;
        resultNetPayloadBytesWritten[rrdPtr] = hbData.resultNetPayloadBytesWritten;
        resultNetSignalingBytesRead[rrdPtr] = hbData.resultNetSignalingBytesRead;
        resultNetSignalingBytesWritten[rrdPtr] = hbData.resultNetSignalingBytesWritten;
        ipcMessagesSent[rrdPtr] = hbData.ipcMessagesSent;
        ipcMessageBytesSent[rrdPtr] = hbData.ipcMessageBytesSent;
        ipcMessagesReceived[rrdPtr] = hbData.ipcMessagesReceived;
        ipcMessageBytesReceived[rrdPtr] = hbData.ipcMessageBytesReceived;
        diskReads[rrdPtr] = hbData.diskReads;
        diskWrites[rrdPtr] = hbData.diskWrites;
        rrdPtr = (rrdPtr + 1) % RRD_SIZE;
    }

    public void touchHeartbeat() {
        lastHeartbeatNanoTime = System.nanoTime();
    }

    public long nanosSinceLastHeartbeat() {
        return System.nanoTime() - lastHeartbeatNanoTime;
    }

    public Map<SerializedOption, Object> getConfig() {
        return config;
    }

    public Set<JobId> getActiveJobIds() {
        return activeJobIds;
    }

    public NetworkAddress getDataPort() {
        return dataPort;
    }

    public NetworkAddress getResultPort() {
        return resultPort;
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
        put(o, "node-id", nodeId);
        put(o, "heap-used", heapUsedSize[(rrdPtr + RRD_SIZE - 1) % RRD_SIZE]);
        put(o, "system-load-average", systemLoadAverage[(rrdPtr + RRD_SIZE - 1) % RRD_SIZE]);

        return o;
    }

    public synchronized ObjectNode toDetailedJSON(boolean includeStats, boolean includeConfig) {
        ObjectMapper om = new ObjectMapper();
        ObjectNode o = om.createObjectNode();

        put(o, "node-id", nodeId);

        if (includeConfig) {
            put(o, "os-name", osName);
            put(o, "arch", arch);
            put(o, "os-version", osVersion);
            put(o, "num-processors", nProcessors);
            put(o, "vm-name", vmName);
            put(o, "vm-version", vmVersion);
            put(o, "vm-vendor", vmVendor);
            put(o, "classpath", toPathElements(classpath));
            put(o, "library-path", toPathElements(libraryPath));
            put(o, "boot-classpath", toPathElements(bootClasspath));
            put(o, "input-arguments", inputArguments);
            put(o, "input-arguments", inputArguments);
            put(o, "system-properties", systemProperties);
            put(o, "pid", pid);
        }
        if (includeStats) {
            o.putPOJO("date", new Date());
            put(o, "rrd-ptr", rrdPtr);
            put(o, "heartbeat-times", hbTime);
            put(o, "heap-init-sizes", heapInitSize);
            put(o, "heap-used-sizes", heapUsedSize);
            put(o, "heap-committed-sizes", heapCommittedSize);
            put(o, "heap-max-sizes", heapMaxSize);
            put(o, "nonheap-init-sizes", nonheapInitSize);
            put(o, "nonheap-used-sizes", nonheapUsedSize);
            put(o, "nonheap-committed-sizes", nonheapCommittedSize);
            put(o, "nonheap-max-sizes", nonheapMaxSize);
            put(o, "application-memory-budget", capacity.getMemoryByteSize());
            put(o, "application-cpu-core-budget", capacity.getCores());
            put(o, "thread-counts", threadCount);
            put(o, "peak-thread-counts", peakThreadCount);
            put(o, "system-load-averages", systemLoadAverage);
            put(o, "gc-names", gcNames);
            put(o, "gc-collection-counts", gcCollectionCounts);
            put(o, "gc-collection-times", gcCollectionTimes);
            put(o, "net-payload-bytes-read", netPayloadBytesRead);
            put(o, "net-payload-bytes-written", netPayloadBytesWritten);
            put(o, "net-signaling-bytes-read", netSignalingBytesRead);
            put(o, "net-signaling-bytes-written", netSignalingBytesWritten);
            put(o, "result-net-payload-bytes-read", resultNetPayloadBytesRead);
            put(o, "result-net-payload-bytes-written", resultNetPayloadBytesWritten);
            put(o, "result-net-signaling-bytes-read", resultNetSignalingBytesRead);
            put(o, "result-net-signaling-bytes-written", resultNetSignalingBytesWritten);
            put(o, "ipc-messages-sent", ipcMessagesSent);
            put(o, "ipc-message-bytes-sent", ipcMessageBytesSent);
            put(o, "ipc-messages-received", ipcMessagesReceived);
            put(o, "ipc-message-bytes-received", ipcMessageBytesReceived);
            put(o, "disk-reads", diskReads);
            put(o, "disk-writes", diskWrites);
        }

        return o;
    }

}
