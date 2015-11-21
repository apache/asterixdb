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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.feeds.DataBucket.ContentType;
import org.apache.asterix.common.feeds.api.IExceptionHandler;
import org.apache.asterix.common.feeds.api.IFeedManager;
import org.apache.asterix.common.feeds.api.IFeedMemoryComponent;
import org.apache.asterix.common.feeds.api.IFeedMessage;
import org.apache.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.common.feeds.api.IFeedRuntime.Mode;
import org.apache.asterix.common.feeds.message.FeedCongestionMessage;
import org.apache.asterix.common.feeds.message.ThrottlingEnabledFeedMessage;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

/**
 * Provides for error-handling and input-side buffering for a feed runtime.
 */
public class FeedRuntimeInputHandler implements IFrameWriter {

    private static Logger LOGGER = Logger.getLogger(FeedRuntimeInputHandler.class.getName());

    private final FeedConnectionId connectionId;
    private final FeedRuntimeId runtimeId;
    private final FeedPolicyAccessor feedPolicyAccessor;
    private boolean bufferingEnabled;
    private final IExceptionHandler exceptionHandler;
    private final FeedFrameDiscarder discarder;
    private final FeedFrameSpiller spiller;
    private final FeedPolicyAccessor fpa;
    private final IFeedManager feedManager;

    private IFrameWriter coreOperator;
    private MonitoredBuffer mBuffer;
    private DataBucketPool pool;
    private FrameCollection frameCollection;
    private Mode mode;
    private Mode lastMode;
    private boolean finished;
    private long nProcessed;
    private boolean throttlingEnabled;

    private FrameEventCallback frameEventCallback;

    public FeedRuntimeInputHandler(IHyracksTaskContext ctx, FeedConnectionId connectionId, FeedRuntimeId runtimeId,
            IFrameWriter coreOperator, FeedPolicyAccessor fpa, boolean bufferingEnabled, FrameTupleAccessor fta,
            RecordDescriptor recordDesc, IFeedManager feedManager, int nPartitions) throws IOException {
        this.connectionId = connectionId;
        this.runtimeId = runtimeId;
        this.coreOperator = coreOperator;
        this.bufferingEnabled = bufferingEnabled;
        this.feedPolicyAccessor = fpa;
        this.spiller = new FeedFrameSpiller(ctx, connectionId, runtimeId, fpa);
        this.discarder = new FeedFrameDiscarder(connectionId, runtimeId, fpa, this);
        this.exceptionHandler = new FeedExceptionHandler(ctx, fta, recordDesc, feedManager, connectionId);
        this.mode = Mode.PROCESS;
        this.lastMode = Mode.PROCESS;
        this.finished = false;
        this.fpa = fpa;
        this.feedManager = feedManager;
        this.pool = (DataBucketPool) feedManager.getFeedMemoryManager()
                .getMemoryComponent(IFeedMemoryComponent.Type.POOL);
        this.frameCollection = (FrameCollection) feedManager.getFeedMemoryManager()
                .getMemoryComponent(IFeedMemoryComponent.Type.COLLECTION);
        this.frameEventCallback = new FrameEventCallback(fpa, this, coreOperator);
        this.mBuffer = MonitoredBuffer.getMonitoredBuffer(ctx, this, coreOperator, fta, recordDesc,
                feedManager.getFeedMetricCollector(), connectionId, runtimeId, exceptionHandler, frameEventCallback,
                nPartitions, fpa);
        this.mBuffer.start();
        this.throttlingEnabled = false;
    }

    @Override
    public synchronized void nextFrame(ByteBuffer frame) throws HyracksDataException {
        try {
            switch (mode) {
                case PROCESS:
                    switch (lastMode) {
                        case SPILL:
                        case POST_SPILL_DISCARD:
                            setMode(Mode.PROCESS_SPILL);
                            processSpilledBacklog();
                            break;
                        case STALL:
                            setMode(Mode.PROCESS_BACKLOG);
                            processBufferredBacklog();
                            break;
                        default:
                            break;
                    }
                    process(frame);
                    break;
                case PROCESS_BACKLOG:
                case PROCESS_SPILL:
                    process(frame);
                    break;
                case SPILL:
                    spill(frame);
                    break;
                case DISCARD:
                case POST_SPILL_DISCARD:
                    discard(frame);
                    break;
                case STALL:
                    switch (runtimeId.getFeedRuntimeType()) {
                        case COLLECT:
                        case COMPUTE_COLLECT:
                        case COMPUTE:
                        case STORE:
                            bufferDataUntilRecovery(frame);
                            break;
                        default:
                            if (LOGGER.isLoggable(Level.WARNING)) {
                                LOGGER.warning("Discarding frame during " + mode + " mode " + this.runtimeId);
                            }
                            break;
                    }
                    break;
                case END:
                case FAIL:
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Ignoring incoming tuples in " + mode + " mode");
                    }
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    private void bufferDataUntilRecovery(ByteBuffer frame) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Bufferring data until recovery is complete " + this.runtimeId);
        }
        if (frameCollection == null) {
            this.frameCollection = (FrameCollection) feedManager.getFeedMemoryManager()
                    .getMemoryComponent(IFeedMemoryComponent.Type.COLLECTION);
        }
        if (frameCollection == null) {
            discarder.processMessage(frame);
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Running low on memory! DISCARDING FRAME ");
            }
        } else {
            boolean success = frameCollection.addFrame(frame);
            if (!success) {
                if (fpa.spillToDiskOnCongestion()) {
                    if (frame != null) {
                        spiller.processMessage(frame);
                    } // TODO handle the else case
                } else {
                    discarder.processMessage(frame);
                }
            }
        }
    }

    public void reportUnresolvableCongestion() throws HyracksDataException {
        if (this.runtimeId.getFeedRuntimeType().equals(FeedRuntimeType.COMPUTE)) {
            FeedCongestionMessage congestionReport = new FeedCongestionMessage(connectionId, runtimeId,
                    mBuffer.getInflowRate(), mBuffer.getOutflowRate(), mode);
            feedManager.getFeedMessageService().sendMessage(congestionReport);
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Congestion reported " + this.connectionId + " " + this.runtimeId);
            }
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unresolvable congestion at " + this.connectionId + " " + this.runtimeId);
            }
        }
    }

    private void processBufferredBacklog() throws HyracksDataException {
        try {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Processing backlog " + this.runtimeId);
            }

            if (frameCollection != null) {
                Iterator<ByteBuffer> backlog = frameCollection.getFrameCollectionIterator();
                while (backlog.hasNext()) {
                    process(backlog.next());
                    nProcessed++;
                }
                DataBucket bucket = pool.getDataBucket();
                bucket.setContentType(ContentType.EOSD);
                bucket.setDesiredReadCount(1);
                mBuffer.sendMessage(bucket);
                feedManager.getFeedMemoryManager().releaseMemoryComponent(frameCollection);
                frameCollection = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    private void processSpilledBacklog() throws HyracksDataException {
        try {
            Iterator<ByteBuffer> backlog = spiller.replayData();
            while (backlog.hasNext()) {
                process(backlog.next());
                nProcessed++;
            }
            DataBucket bucket = pool.getDataBucket();
            bucket.setContentType(ContentType.EOSD);
            bucket.setDesiredReadCount(1);
            mBuffer.sendMessage(bucket);
            spiller.reset();
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    protected void process(ByteBuffer frame) throws HyracksDataException {
        boolean frameProcessed = false;
        while (!frameProcessed) {
            try {
                if (!bufferingEnabled) {
                    coreOperator.nextFrame(frame); // synchronous
                    mBuffer.sendReport(frame);
                } else {
                    DataBucket bucket = pool.getDataBucket();
                    if (bucket != null) {
                        if (frame != null) {
                            bucket.reset(frame); // created a copy here
                            bucket.setContentType(ContentType.DATA);
                        } else {
                            bucket.setContentType(ContentType.EOD);
                        }
                        bucket.setDesiredReadCount(1);
                        mBuffer.sendMessage(bucket);
                        mBuffer.sendReport(frame);
                        nProcessed++;
                    } else {
                        if (fpa.spillToDiskOnCongestion()) {
                            if (frame != null) {
                                boolean spilled = spiller.processMessage(frame);
                                if (spilled) {
                                    setMode(Mode.SPILL);
                                } else {
                                    reportUnresolvableCongestion();
                                }
                            }
                        } else if (fpa.discardOnCongestion()) {
                            boolean discarded = discarder.processMessage(frame);
                            if (!discarded) {
                                reportUnresolvableCongestion();
                            }
                        } else if (fpa.throttlingEnabled()) {
                            setThrottlingEnabled(true);
                        } else {
                            reportUnresolvableCongestion();
                        }

                    }
                }
                frameProcessed = true;
            } catch (Exception e) {
                if (feedPolicyAccessor.continueOnSoftFailure()) {
                    frame = exceptionHandler.handleException(e, frame);
                    if (frame == null) {
                        frameProcessed = true;
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Encountered exception! " + e.getMessage()
                                    + "Insufficient information, Cannot extract failing tuple");
                        }
                    }
                } else {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Ingestion policy does not require recovering from tuple. Feed would terminate");
                    }
                    mBuffer.close(false);
                    throw new HyracksDataException(e);
                }
            }
        }
    }

    private void spill(ByteBuffer frame) throws Exception {
        boolean success = spiller.processMessage(frame);
        if (!success) {
            // limit reached
            setMode(Mode.POST_SPILL_DISCARD);
            reportUnresolvableCongestion();
        }
    }

    private void discard(ByteBuffer frame) throws Exception {
        boolean success = discarder.processMessage(frame);
        if (!success) { // limit reached
            reportUnresolvableCongestion();
        }
    }

    public Mode getMode() {
        return mode;
    }

    public synchronized void setMode(Mode mode) {
        if (mode.equals(this.mode)) {
            return;
        }
        this.lastMode = this.mode;
        this.mode = mode;
        if (mode.equals(Mode.END)) {
            this.close();
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Switched from " + lastMode + " to " + mode + " " + this.runtimeId);
        }
    }

    @Override
    public void close() {
        if (mBuffer != null) {
            boolean disableMonitoring = !this.mode.equals(Mode.STALL);
            if (frameCollection != null) {
                feedManager.getFeedMemoryManager().releaseMemoryComponent(frameCollection);
            }
            if (pool != null) {
                feedManager.getFeedMemoryManager().releaseMemoryComponent(pool);
            }
            mBuffer.close(false, disableMonitoring);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Closed input side handler for " + this.runtimeId + " disabled monitoring "
                        + disableMonitoring + " Mode for runtime " + this.mode);
            }
        }
    }

    public IFrameWriter getCoreOperator() {
        return coreOperator;
    }

    public void setCoreOperator(IFrameWriter coreOperator) {
        this.coreOperator = coreOperator;
        mBuffer.setFrameWriter(coreOperator);
        frameEventCallback.setCoreOperator(coreOperator);
    }

    public boolean isFinished() {
        return finished;
    }

    public void setFinished(boolean finished) {
        this.finished = finished;
    }

    public long getProcessed() {
        return nProcessed;
    }

    public FeedRuntimeId getRuntimeId() {
        return runtimeId;
    }

    @Override
    public void open() throws HyracksDataException {
        coreOperator.open();
    }

    @Override
    public void fail() throws HyracksDataException {
        coreOperator.fail();
    }

    public void reset(int nPartitions) {
        this.mBuffer.setNumberOfPartitions(nPartitions);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Reset number of partitions to " + nPartitions + " for " + this.runtimeId);
        }
        if (mBuffer != null) {
            mBuffer.reset();
        }
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

    public IFeedManager getFeedManager() {
        return feedManager;
    }

    public MonitoredBuffer getmBuffer() {
        return mBuffer;
    }

    public boolean isThrottlingEnabled() {
        return throttlingEnabled;
    }

    public void setThrottlingEnabled(boolean throttlingEnabled) {
        if (this.throttlingEnabled != throttlingEnabled) {
            this.throttlingEnabled = throttlingEnabled;
            IFeedMessage throttlingEnabledMesg = new ThrottlingEnabledFeedMessage(connectionId, runtimeId);
            feedManager.getFeedMessageService().sendMessage(throttlingEnabledMesg);
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Throttling " + throttlingEnabled + " for " + this.connectionId + "[" + runtimeId + "]");
            }
        }
    }

    public boolean isBufferingEnabled() {
        return bufferingEnabled;
    }

    public void setBufferingEnabled(boolean bufferingEnabled) {
        this.bufferingEnabled = bufferingEnabled;
    }
}
