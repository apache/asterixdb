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
package org.apache.asterix.common.feeds;

import java.nio.ByteBuffer;

import org.apache.asterix.common.feeds.FeedConstants.StatisticsConstants;
import org.apache.asterix.common.feeds.api.IExceptionHandler;
import org.apache.asterix.common.feeds.api.IFeedMetricCollector;
import org.apache.asterix.common.feeds.api.IFrameEventCallback;
import org.apache.asterix.common.feeds.api.IFramePostProcessor;
import org.apache.asterix.common.feeds.api.IFramePreprocessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class StorageSideMonitoredBuffer extends MonitoredBuffer {

    private static final long STORAGE_TIME_TRACKING_FREQUENCY = 5000; // 10
                                                                      // seconds

    private boolean ackingEnabled;
    private final boolean timeTrackingEnabled;

    public StorageSideMonitoredBuffer(IHyracksTaskContext ctx, FeedRuntimeInputHandler inputHandler,
            IFrameWriter frameWriter, FrameTupleAccessor fta, RecordDescriptor recordDesc,
            IFeedMetricCollector metricCollector, FeedConnectionId connectionId, FeedRuntimeId runtimeId,
            IExceptionHandler exceptionHandler, IFrameEventCallback callback, int nPartitions,
            FeedPolicyAccessor policyAccessor) {
        super(ctx, inputHandler, frameWriter, fta, recordDesc, metricCollector, connectionId, runtimeId,
                exceptionHandler, callback, nPartitions, policyAccessor);
        timeTrackingEnabled = policyAccessor.isTimeTrackingEnabled();
        ackingEnabled = policyAccessor.atleastOnceSemantics();
        if (ackingEnabled || timeTrackingEnabled) {
            storageFromeHandler = new StorageFrameHandler();
            this.storageTimeTrackingRateTask = new MonitoredBufferTimerTasks.MonitoredBufferStorageTimerTask(this,
                    inputHandler.getFeedManager(), connectionId, runtimeId.getPartition(), policyAccessor,
                    storageFromeHandler);
            this.timer.scheduleAtFixedRate(storageTimeTrackingRateTask, 0, STORAGE_TIME_TRACKING_FREQUENCY);
        }
    }

    @Override
    protected boolean monitorProcessingRate() {
        return false;
    }

    @Override
    protected boolean logInflowOutflowRate() {
        return true;
    }

    @Override
    public IFramePreprocessor getFramePreProcessor() {
        return new IFramePreprocessor() {

            @Override
            public void preProcess(ByteBuffer frame) {
                try {
                    if (ackingEnabled) {
                        storageFromeHandler.updateTrackingInformation(frame, inflowFta);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
    }

    @Override
    protected IFramePostProcessor getFramePostProcessor() {
        return new IFramePostProcessor() {

            private static final long NORMAL_WINDOW_LIMIT = 400 * 1000;
            private static final long HIGH_WINDOW_LIMIT = 800 * 1000;

            private long delayNormalWindow = 0;
            private long delayHighWindow = 0;
            private long delayLowWindow = 0;

            private int countNormalWindow;
            private int countHighWindow;
            private int countLowWindow;

            private long beginIntakeTimestamp = 0;

            @Override
            public void postProcessFrame(ByteBuffer frame, FrameTupleAccessor frameAccessor) {
                if (ackingEnabled || timeTrackingEnabled) {
                    int nTuples = frameAccessor.getTupleCount();
                    long intakeTimestamp;
                    long currentTime = System.currentTimeMillis();
                    for (int i = 0; i < nTuples; i++) {
                        int recordStart = frameAccessor.getTupleStartOffset(i) + frameAccessor.getFieldSlotsLength();
                        int openPartOffsetOrig = frame.getInt(recordStart + 6);
                        int numOpenFields = frame.getInt(recordStart + openPartOffsetOrig);

                        int recordIdOffset = openPartOffsetOrig + 4 + 8 * numOpenFields
                                + (StatisticsConstants.INTAKE_TUPLEID.length() + 2) + 1;

                        int partitionOffset = recordIdOffset + 4 + (StatisticsConstants.INTAKE_PARTITION.length() + 2)
                                + 1;

                        int intakeTimestampValueOffset = partitionOffset + 4
                                + (StatisticsConstants.INTAKE_TIMESTAMP.length() + 2) + 1;
                        intakeTimestamp = frame.getLong(recordStart + intakeTimestampValueOffset);
                        if (beginIntakeTimestamp == 0) {
                            beginIntakeTimestamp = intakeTimestamp;
                            LOGGER.warning("Begin Timestamp: " + beginIntakeTimestamp);
                        }

                        updateRunningAvg(intakeTimestamp, currentTime);

                        int storeTimestampValueOffset = intakeTimestampValueOffset + 8
                                + (StatisticsConstants.STORE_TIMESTAMP.length() + 2) + 1;
                        frame.putLong(recordStart + storeTimestampValueOffset, System.currentTimeMillis());
                    }
                    logRunningAvg();
                    resetRunningAvg();
                }
            }

            private void updateRunningAvg(long intakeTimestamp, long currentTime) {
                long diffTimestamp = intakeTimestamp - beginIntakeTimestamp;
                long delay = (currentTime - intakeTimestamp);
                if (diffTimestamp < NORMAL_WINDOW_LIMIT) {
                    delayNormalWindow += delay;
                    countNormalWindow++;
                } else if (diffTimestamp < HIGH_WINDOW_LIMIT) {
                    delayHighWindow += delay;
                    countHighWindow++;
                } else {
                    delayLowWindow += delay;
                    countLowWindow++;
                }
            }

            private void resetRunningAvg() {
                delayNormalWindow = 0;
                countNormalWindow = 0;
                delayHighWindow = 0;
                countHighWindow = 0;
                delayLowWindow = 0;
                countLowWindow = 0;
            }

            private void logRunningAvg() {
                if (countNormalWindow != 0 && delayNormalWindow != 0) {
                    LOGGER.warning("Window:" + 0 + ":" + "Avg Travel_Time:" + (delayNormalWindow / countNormalWindow));
                }
                if (countHighWindow != 0 && delayHighWindow != 0) {
                    LOGGER.warning("Window:" + 1 + ":" + "Avg Travel_Time:" + (delayHighWindow / countHighWindow));
                }
                if (countLowWindow != 0 && delayLowWindow != 0) {
                    LOGGER.warning("Window:" + 2 + ":" + "Avg Travel_Time:" + (delayLowWindow / countLowWindow));
                }
            }

        };
    }

    public boolean isAckingEnabled() {
        return ackingEnabled;
    }

    public void setAcking(boolean ackingEnabled) {
        this.ackingEnabled = ackingEnabled;
    }

    public boolean isTimeTrackingEnabled() {
        return timeTrackingEnabled;
    }

    @Override
    protected boolean monitorInputQueueLength() {
        return true;
    }

    @Override
    protected boolean reportOutflowRate() {
        return true;
    }

    @Override
    protected boolean reportInflowRate() {
        return false;
    }

}
