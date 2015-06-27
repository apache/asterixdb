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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.config.AsterixFeedProperties;
import edu.uci.ics.asterix.common.feeds.api.IFeedManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedMessageService;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.ValueType;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.Mode;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback.FrameEvent;
import edu.uci.ics.asterix.common.feeds.message.FeedReportMessage;
import edu.uci.ics.asterix.common.feeds.message.ScaleInReportMessage;
import edu.uci.ics.asterix.common.feeds.message.StorageReportFeedMessage;

public class MonitoredBufferTimerTasks {

    private static final Logger LOGGER = Logger.getLogger(MonitorInputQueueLengthTimerTask.class.getName());

    public static class MonitoredBufferStorageTimerTask extends TimerTask {

        private static final int PERSISTENCE_DELAY_VIOLATION_MAX = 5;

        private final StorageSideMonitoredBuffer mBuffer;
        private final IFeedManager feedManager;
        private final int partition;
        private final FeedConnectionId connectionId;
        private final FeedPolicyAccessor policyAccessor;
        private final StorageFrameHandler storageFromeHandler;
        private final StorageReportFeedMessage storageReportMessage;
        private final FeedTupleCommitAckMessage tupleCommitAckMessage;

        private Map<Integer, Integer> maxIntakeBaseCovered;
        private int countDelayExceeded = 0;

        public MonitoredBufferStorageTimerTask(StorageSideMonitoredBuffer mBuffer, IFeedManager feedManager,
                FeedConnectionId connectionId, int partition, FeedPolicyAccessor policyAccessor,
                StorageFrameHandler storageFromeHandler) {
            this.mBuffer = mBuffer;
            this.feedManager = feedManager;
            this.connectionId = connectionId;
            this.partition = partition;
            this.policyAccessor = policyAccessor;
            this.storageFromeHandler = storageFromeHandler;
            this.storageReportMessage = new StorageReportFeedMessage(this.connectionId, this.partition, 0, false, 0, 0);
            this.tupleCommitAckMessage = new FeedTupleCommitAckMessage(this.connectionId, 0, 0, null);
            this.maxIntakeBaseCovered = new HashMap<Integer, Integer>();
        }

        @Override
        public void run() {
            if (mBuffer.isAckingEnabled() && !mBuffer.getInputHandler().isThrottlingEnabled()) {
                ackRecords();
            }
            if (mBuffer.isTimeTrackingEnabled()) {
                checkLatencyViolation();
            }
        }

        private void ackRecords() {
            Set<Integer> partitions = storageFromeHandler.getPartitionsWithStats();
            List<Integer> basesCovered = new ArrayList<Integer>();
            for (int intakePartition : partitions) {
                Map<Integer, IntakePartitionStatistics> baseAcks = storageFromeHandler
                        .getBaseAcksForPartition(intakePartition);
                for (Entry<Integer, IntakePartitionStatistics> entry : baseAcks.entrySet()) {
                    int base = entry.getKey();
                    IntakePartitionStatistics stats = entry.getValue();
                    Integer maxIntakeBaseForPartition = maxIntakeBaseCovered.get(intakePartition);
                    if (maxIntakeBaseForPartition == null || maxIntakeBaseForPartition < base) {
                        tupleCommitAckMessage.reset(intakePartition, base, stats.getAckInfo());
                        feedManager.getFeedMessageService().sendMessage(tupleCommitAckMessage);
                    } else {
                        basesCovered.add(base);
                    }
                }
                for (Integer b : basesCovered) {
                    baseAcks.remove(b);
                }
                basesCovered.clear();
            }
        }

        private void checkLatencyViolation() {
            long avgDelayPersistence = storageFromeHandler.getAvgDelayPersistence();
            if (avgDelayPersistence > policyAccessor.getMaxDelayRecordPersistence()) {
                countDelayExceeded++;
                if (countDelayExceeded > PERSISTENCE_DELAY_VIOLATION_MAX) {
                    storageReportMessage.reset(0, false, mBuffer.getAvgDelayRecordPersistence());
                    feedManager.getFeedMessageService().sendMessage(storageReportMessage);
                }
            } else {
                countDelayExceeded = 0;
            }
        }

        public void receiveCommitAckResponse(FeedTupleCommitResponseMessage message) {
            maxIntakeBaseCovered.put(message.getIntakePartition(), message.getMaxWindowAcked());
        }
    }

    public static class LogInputOutputRateTask extends TimerTask {

        private final MonitoredBuffer mBuffer;
        private final boolean log;
        private final boolean reportInflow;
        private final boolean reportOutflow;

        private final IFeedMessageService messageService;
        private final FeedReportMessage message;

        public LogInputOutputRateTask(MonitoredBuffer mBuffer, boolean log, boolean reportInflow, boolean reportOutflow) {
            this.mBuffer = mBuffer;
            this.log = log;
            this.reportInflow = reportInflow;
            this.reportOutflow = reportOutflow;
            if (reportInflow || reportOutflow) {
                ValueType vType = reportInflow ? ValueType.INFLOW_RATE : ValueType.OUTFLOW_RATE;
                messageService = mBuffer.getInputHandler().getFeedManager().getFeedMessageService();
                message = new FeedReportMessage(mBuffer.getInputHandler().getConnectionId(), mBuffer.getRuntimeId(),
                        vType, 0);
            } else {
                messageService = null;
                message = null;
            }

        }

        @Override
        public void run() {
            int pendingWork = mBuffer.getWorkSize();
            int outflowRate = mBuffer.getOutflowRate();
            int inflowRate = mBuffer.getInflowRate();
            if (log) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(mBuffer.getRuntimeId() + " " + "Inflow rate:" + inflowRate + " Outflow Rate:"
                            + outflowRate + " Pending Work " + pendingWork);
                }
            }
            if (reportInflow) {
                message.reset(inflowRate);
            } else if (reportOutflow) {
                message.reset(outflowRate);
            }
            messageService.sendMessage(message);
        }
    }

    public static class MonitorInputQueueLengthTimerTask extends TimerTask {

        private final MonitoredBuffer mBuffer;
        private final IFrameEventCallback callback;
        private final int pendingWorkThreshold;
        private final int maxSuccessiveThresholdPeriods;
        private FrameEvent lastEvent = FrameEvent.NO_OP;
        private int pendingWorkExceedCount = 0;

        public MonitorInputQueueLengthTimerTask(MonitoredBuffer mBuffer, IFrameEventCallback callback) {
            this.mBuffer = mBuffer;
            this.callback = callback;
            AsterixFeedProperties props = mBuffer.getInputHandler().getFeedManager().getAsterixFeedProperties();
            pendingWorkThreshold = props.getPendingWorkThreshold();
            maxSuccessiveThresholdPeriods = props.getMaxSuccessiveThresholdPeriod();
        }

        @Override
        public void run() {
            int pendingWork = mBuffer.getWorkSize();
            if (mBuffer.getMode().equals(Mode.PROCESS_SPILL) || mBuffer.getMode().equals(Mode.PROCESS_BACKLOG)) {
                return;
            }

            switch (lastEvent) {
                case NO_OP:
                case PENDING_WORK_DONE:
                case FINISHED_PROCESSING_SPILLAGE:
                    if (pendingWork > pendingWorkThreshold) {
                        pendingWorkExceedCount++;
                        if (pendingWorkExceedCount > maxSuccessiveThresholdPeriods) {
                            pendingWorkExceedCount = 0;
                            lastEvent = FrameEvent.PENDING_WORK_THRESHOLD_REACHED;
                            callback.frameEvent(lastEvent);
                        }
                    } else if (pendingWork == 0 && mBuffer.getMode().equals(Mode.SPILL)) {
                        lastEvent = FrameEvent.PENDING_WORK_DONE;
                        callback.frameEvent(lastEvent);
                    }
                    break;
                case PENDING_WORK_THRESHOLD_REACHED:
                    if (((pendingWork * 1.0) / pendingWorkThreshold) <= 0.5) {
                        lastEvent = FrameEvent.PENDING_WORK_DONE;
                        callback.frameEvent(lastEvent);
                    }
                    break;
                case FINISHED_PROCESSING:
                    break;

            }
        }
    }

    /**
     * A timer task to measure and compare the processing rate and inflow rate
     * to look for possibility to scale-in, that is reduce the degree of cardinality
     * of the compute operator.
     */
    public static class MonitoreProcessRateTimerTask extends TimerTask {

        private final MonitoredBuffer mBuffer;
        private final IFeedManager feedManager;
        private int nPartitions;
        private ScaleInReportMessage sMessage;
        private boolean proposedChange;

        public MonitoreProcessRateTimerTask(MonitoredBuffer mBuffer, IFeedManager feedManager,
                FeedConnectionId connectionId, int nPartitions) {
            this.mBuffer = mBuffer;
            this.feedManager = feedManager;
            this.nPartitions = nPartitions;
            this.sMessage = new ScaleInReportMessage(connectionId, FeedRuntimeType.COMPUTE, 0, 0);
            this.proposedChange = false;
        }

        public int getNumberOfPartitions() {
            return nPartitions;
        }

        public void setNumberOfPartitions(int nPartitions) {
            this.nPartitions = nPartitions;
            proposedChange = false;
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Reset the number of partitions for " + mBuffer.getRuntimeId() + " to " + nPartitions);
            }
        }

        @Override
        public void run() {
            if (!proposedChange) {
                int inflowRate = mBuffer.getInflowRate();
                int procRate = mBuffer.getProcessingRate();
                if (inflowRate > 0 && procRate > 0) {
                    if (inflowRate < procRate) {
                        int possibleCardinality = (int) Math.ceil(nPartitions * inflowRate / (double) procRate);
                        if (possibleCardinality < nPartitions
                                && ((((nPartitions - possibleCardinality) * 1.0) / nPartitions) >= 0.25)) {
                            sMessage.reset(nPartitions, possibleCardinality);
                            feedManager.getFeedMessageService().sendMessage(sMessage);
                            proposedChange = true;
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Proposed scale-in " + sMessage);
                            }
                        }
                    } else {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Inflow Rate (" + inflowRate + ") exceeds Processing Rate" + " (" + procRate
                                    + ")");
                        }
                    }
                }
            } else {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Waiting for earlier proposal to scale in to be applied");
                }
            }
        }
    }
}
