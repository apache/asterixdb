/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.cc;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NodeRegistration;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatData;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatSchema;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatSchema.GarbageCollectorInfo;

public class NodeControllerState {
    private static final int RRD_SIZE = 720;

    private final INodeController nodeController;

    private final NCConfig ncConfig;

    private final NetworkAddress dataPort;

    private final NetworkAddress datasetPort;

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

    private int rrdPtr;

    private int lastHeartbeatDuration;

    public NodeControllerState(INodeController nodeController, NodeRegistration reg) {
        this.nodeController = nodeController;
        ncConfig = reg.getNCConfig();
        dataPort = reg.getDataPort();
        datasetPort = reg.getDatasetPort();
        activeJobIds = new HashSet<JobId>();

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

        rrdPtr = 0;
    }

    public void notifyHeartbeat(HeartbeatData hbData) {
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
        }
        rrdPtr = (rrdPtr + 1) % RRD_SIZE;
    }

    public int incrementLastHeartbeatDuration() {
        return lastHeartbeatDuration++;
    }

    public int getLastHeartbeatDuration() {
        return lastHeartbeatDuration;
    }

    public INodeController getNodeController() {
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

    public JSONObject toSummaryJSON() throws JSONException {
        JSONObject o = new JSONObject();
        o.put("node-id", ncConfig.nodeId);
        o.put("heap-used", heapUsedSize[(rrdPtr + RRD_SIZE - 1) % RRD_SIZE]);
        o.put("system-load-average", systemLoadAverage[(rrdPtr + RRD_SIZE - 1) % RRD_SIZE]);

        return o;
    }

    public JSONObject toDetailedJSON() throws JSONException {
        JSONObject o = new JSONObject();

        o.put("node-id", ncConfig.nodeId);
        o.put("os-name", osName);
        o.put("arch", arch);
        o.put("os-version", osVersion);
        o.put("num-processors", nProcessors);
        o.put("vm-name", vmName);
        o.put("vm-version", vmVersion);
        o.put("vm-vendor", vmVendor);
        o.put("classpath", classpath);
        o.put("library-path", libraryPath);
        o.put("boot-classpath", bootClasspath);
        o.put("input-arguments", new JSONArray(inputArguments));
        o.put("rrd-ptr", rrdPtr);
        o.put("heartbeat-times", hbTime);
        o.put("heap-init-sizes", heapInitSize);
        o.put("heap-used-sizes", heapUsedSize);
        o.put("heap-committed-sizes", heapCommittedSize);
        o.put("heap-max-sizes", heapMaxSize);
        o.put("nonheap-init-sizes", nonheapInitSize);
        o.put("nonheap-used-sizes", nonheapUsedSize);
        o.put("nonheap-committed-sizes", nonheapCommittedSize);
        o.put("nonheap-max-sizes", nonheapMaxSize);
        o.put("thread-counts", threadCount);
        o.put("peak-thread-counts", peakThreadCount);
        o.put("system-load-averages", systemLoadAverage);
        o.put("gc-names", gcNames);
        o.put("gc-collection-counts", gcCollectionCounts);
        o.put("gc-collection-times", gcCollectionTimes);
        o.put("net-payload-bytes-read", netPayloadBytesRead);
        o.put("net-payload-bytes-written", netPayloadBytesWritten);
        o.put("net-signaling-bytes-read", netSignalingBytesRead);
        o.put("net-signaling-bytes-written", netSignalingBytesWritten);
        o.put("dataset-net-payload-bytes-read", datasetNetPayloadBytesRead);
        o.put("dataset-net-payload-bytes-written", datasetNetPayloadBytesWritten);
        o.put("dataset-net-signaling-bytes-read", datasetNetSignalingBytesRead);
        o.put("dataset-net-signaling-bytes-written", datasetNetSignalingBytesWritten);
        o.put("ipc-messages-sent", ipcMessagesSent);
        o.put("ipc-message-bytes-sent", ipcMessageBytesSent);
        o.put("ipc-messages-received", ipcMessagesReceived);
        o.put("ipc-message-bytes-received", ipcMessageBytesReceived);

        return o;
    }
}