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
package edu.uci.ics.asterix.common.feeds;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import edu.uci.ics.asterix.common.feeds.api.IFeedManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedMessageService;
import edu.uci.ics.asterix.common.feeds.api.IFeedService;
import edu.uci.ics.asterix.common.feeds.message.NodeReportMessage;

public class NodeLoadReportService implements IFeedService {

    private static final int NODE_LOAD_REPORT_FREQUENCY = 2000;
    private static final float CPU_CHANGE_THRESHOLD = 0.2f;
    private static final float HEAP_CHANGE_THRESHOLD = 0.4f;

    private final String nodeId;
    private final NodeLoadReportTask task;
    private final Timer timer;

    public NodeLoadReportService(String nodeId, IFeedManager feedManager) {
        this.nodeId = nodeId;
        this.task = new NodeLoadReportTask(nodeId, feedManager);
        this.timer = new Timer();
    }

    @Override
    public void start() throws Exception {
        timer.schedule(task, 0, NODE_LOAD_REPORT_FREQUENCY);
    }

    @Override
    public void stop() {
        timer.cancel();
    }

    private static class NodeLoadReportTask extends TimerTask {

        private final String nodeId;
        private final IFeedManager feedManager;
        private final NodeReportMessage message;
        private final IFeedMessageService messageService;

        private static OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        private static MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();

        public NodeLoadReportTask(String nodeId, IFeedManager feedManager) {
            this.nodeId = nodeId;
            this.feedManager = feedManager;
            this.message = new NodeReportMessage(0.0f, 0L, 0);
            this.messageService = feedManager.getFeedMessageService();
        }

        @Override
        public void run() {
            List<FeedRuntimeId> runtimeIds = feedManager.getFeedConnectionManager().getRegisteredRuntimes();
            int nRuntimes = runtimeIds.size();
            double cpuLoad = getCpuLoad();
            double usedHeap = getUsedHeap();
            if (sendMessage(nRuntimes, cpuLoad, usedHeap)) {
                message.reset(cpuLoad, usedHeap, nRuntimes);
                messageService.sendMessage(message);
            }
        }

        private boolean sendMessage(int nRuntimes, double cpuLoad, double usedHeap) {
            if (message == null) {
                return true;
            }

            boolean changeInCpu = (Math.abs(cpuLoad - message.getCpuLoad()) / message.getCpuLoad()) > CPU_CHANGE_THRESHOLD;
            boolean changeInUsedHeap = (Math.abs(usedHeap - message.getUsedHeap()) / message.getUsedHeap()) > HEAP_CHANGE_THRESHOLD;
            boolean changeInRuntimeSize = nRuntimes != message.getnRuntimes();
            return changeInCpu || changeInUsedHeap || changeInRuntimeSize;
        }

        private double getCpuLoad() {
            return osBean.getSystemLoadAverage();
        }

        private double getUsedHeap() {
            return ((double) memBean.getHeapMemoryUsage().getUsed()) / memBean.getHeapMemoryUsage().getMax();
        }
    }

}
