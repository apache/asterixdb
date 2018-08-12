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
package org.apache.hyracks.control.nc.heartbeat;

import static org.apache.hyracks.util.MXHelper.gcMXBeans;
import static org.apache.hyracks.util.MXHelper.memoryMXBean;
import static org.apache.hyracks.util.MXHelper.osMXBean;
import static org.apache.hyracks.util.MXHelper.threadMXBean;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryUsage;
import java.util.TimerTask;

import org.apache.hyracks.control.common.heartbeat.HeartbeatData;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.io.profiling.IIOCounter;
import org.apache.hyracks.control.nc.io.profiling.IOCounterFactory;
import org.apache.hyracks.ipc.api.IPCPerformanceCounters;
import org.apache.hyracks.net.protocols.muxdemux.MuxDemuxPerformanceCounters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HeartbeatComputeTask extends TimerTask {
    private static final Logger LOGGER = LogManager.getLogger();
    private final IIOCounter ioCounter;

    private NodeControllerService ncs;
    private final HeartbeatData hbData;

    public HeartbeatComputeTask(NodeControllerService ncs) {
        this.ncs = ncs;
        hbData = new HeartbeatData();
        ioCounter = IOCounterFactory.INSTANCE.getIOCounter();
        run();
    }

    public HeartbeatData getHeartbeatData() {
        return hbData;
    }

    @Override
    public void run() {
        synchronized (hbData) {
            MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
            hbData.heapInitSize = heapUsage.getInit();
            hbData.heapUsedSize = heapUsage.getUsed();
            hbData.heapCommittedSize = heapUsage.getCommitted();
            hbData.heapMaxSize = heapUsage.getMax();
            MemoryUsage nonheapUsage = memoryMXBean.getNonHeapMemoryUsage();
            hbData.nonheapInitSize = nonheapUsage.getInit();
            hbData.nonheapUsedSize = nonheapUsage.getUsed();
            hbData.nonheapCommittedSize = nonheapUsage.getCommitted();
            hbData.nonheapMaxSize = nonheapUsage.getMax();
            hbData.threadCount = threadMXBean.getThreadCount();
            hbData.peakThreadCount = threadMXBean.getPeakThreadCount();
            hbData.totalStartedThreadCount = threadMXBean.getTotalStartedThreadCount();
            hbData.systemLoadAverage = osMXBean.getSystemLoadAverage();
            int gcN = gcMXBeans.size();
            for (int i = 0; i < gcN; ++i) {
                GarbageCollectorMXBean gcMXBean = gcMXBeans.get(i);
                hbData.gcCollectionCounts[i] = gcMXBean.getCollectionCount();
                hbData.gcCollectionTimes[i] = gcMXBean.getCollectionTime();
            }

            MuxDemuxPerformanceCounters netPC = ncs.getNetworkManager().getPerformanceCounters();
            hbData.netPayloadBytesRead = netPC.getPayloadBytesRead();
            hbData.netPayloadBytesWritten = netPC.getPayloadBytesWritten();
            hbData.netSignalingBytesRead = netPC.getSignalingBytesRead();
            hbData.netSignalingBytesWritten = netPC.getSignalingBytesWritten();

            MuxDemuxPerformanceCounters resultNetPC = ncs.getResultNetworkManager().getPerformanceCounters();
            hbData.resultNetPayloadBytesRead = resultNetPC.getPayloadBytesRead();
            hbData.resultNetPayloadBytesWritten = resultNetPC.getPayloadBytesWritten();
            hbData.resultNetSignalingBytesRead = resultNetPC.getSignalingBytesRead();
            hbData.resultNetSignalingBytesWritten = resultNetPC.getSignalingBytesWritten();

            IPCPerformanceCounters ipcPC = ncs.getIpcSystem().getPerformanceCounters();
            hbData.ipcMessagesSent = ipcPC.getMessageSentCount();
            hbData.ipcMessageBytesSent = ipcPC.getMessageBytesSent();
            hbData.ipcMessagesReceived = ipcPC.getMessageReceivedCount();
            hbData.ipcMessageBytesReceived = ipcPC.getMessageBytesReceived();

            hbData.diskReads = ioCounter.getReads();
            hbData.diskWrites = ioCounter.getWrites();
            hbData.numCores = Runtime.getRuntime().availableProcessors();

            ncs.getNodeControllerData().notifyHeartbeat(hbData);
        }
        LOGGER.trace("Successfully refreshed heartbeat data");
    }
}
