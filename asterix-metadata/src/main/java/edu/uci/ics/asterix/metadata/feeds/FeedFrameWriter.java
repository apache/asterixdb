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
package edu.uci.ics.asterix.metadata.feeds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedMessageService;
import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.IFeedManager;
import edu.uci.ics.asterix.common.feeds.SuperFeedManager;
import edu.uci.ics.asterix.common.feeds.SuperFeedManager.FeedReportMessageType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

/**
 * A wrapper around the standard frame writer provided to an operator node pushable.
 * The wrapper monitors the flow of data from this operator to a downstream operator
 * over a connector. It collects statistics if required by the feed ingestion policy
 * and reports them to the Super Feed Manager chosen for the feed. In addition any
 * congestion experienced by the operator is also reported.
 */
public class FeedFrameWriter implements IFrameWriter {

    private static final Logger LOGGER = Logger.getLogger(FeedFrameWriter.class.getName());

    /** The threshold for the time required in pushing a frame to the network. **/
    public static final long FLUSH_THRESHOLD_TIME = 5000; // 5 seconds

    /** Actual frame writer provided to an operator. **/
    private IFrameWriter writer;

    /** The node pushable associated with the operator **/
    private IOperatorNodePushable nodePushable;

    /** set to true if health need to be monitored **/
    private final boolean reportHealth;

    /** A buffer for keeping frames that are waiting to be processed **/
    private List<ByteBuffer> frames = new ArrayList<ByteBuffer>();

    /**
     * Mode associated with the frame writer
     * Possible values: FORWARD, STORE
     * 
     * @see Mode
     */
    private Mode mode;

    /**
     * Detects if the operator is unable to push a frame downstream
     * within a threshold period of time. In addition, it measure the
     * throughput as observed on the output channel of the associated operator.
     */
    private HealthMonitor healthMonitor;

    /**
     * A Timer instance for managing scheduling of tasks.
     */
    private Timer timer;

    /**
     * Provides access to the tuples in a frame. Used in collecting statistics
     */
    private FrameTupleAccessor fta;

    public enum Mode {
        /**
         * **
         * Normal mode of operation for an operator when
         * frames are pushed to the downstream operator.
         */
        FORWARD,

        /**
         * Failure mode of operation for an operator when
         * input frames are not pushed to the downstream operator but
         * are buffered for future retrieval. This mode is adopted
         * during failure recovery.
         */
        STORE
    }

    public FeedFrameWriter(IFrameWriter writer, IOperatorNodePushable nodePushable, FeedConnectionId feedId,
            FeedPolicyEnforcer policyEnforcer, String nodeId, FeedRuntimeType feedRuntimeType, int partition,
            FrameTupleAccessor fta, IFeedManager feedManager) {
        this.writer = writer;
        this.mode = Mode.FORWARD;
        this.nodePushable = nodePushable;
        this.reportHealth = policyEnforcer.getFeedPolicyAccessor().collectStatistics();
        if (reportHealth) {
            timer = new Timer();
            healthMonitor = new HealthMonitor(feedId, nodeId, feedRuntimeType, partition, timer, fta, feedManager);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Statistics collection enabled for the feed " + feedId + " " + feedRuntimeType + " ["
                        + partition + "]");
            }
            timer.scheduleAtFixedRate(healthMonitor, 0, FLUSH_THRESHOLD_TIME);
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Statistics collection *not* enabled for the feed " + feedId + " " + feedRuntimeType + " ["
                        + partition + "]");
            }
        }
        this.fta = fta;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode newMode) throws HyracksDataException {
        if (this.mode.equals(newMode)) {
            return;
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Switching to :" + newMode + " from " + this.mode);
        }
        this.mode = newMode;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        switch (mode) {
            case FORWARD:
                try {
                    if (reportHealth) {
                        fta.reset(buffer);
                        healthMonitor.notifyStartFrameFlushActivity();
                        writer.nextFrame(buffer);
                        healthMonitor.notifyFinishFrameFlushActivity();
                    } else {
                        writer.nextFrame(buffer);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Unable to write frame " + " on behalf of " + nodePushable.getDisplayName()
                                + ":\n" + e);
                    }
                }
                if (frames.size() > 0) {
                    for (ByteBuffer buf : frames) {
                        writer.nextFrame(buf);
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Flushed old frame (from previous failed execution) : " + buf
                                    + " on behalf of " + nodePushable.getDisplayName());
                        }
                    }
                    frames.clear();
                }
                break;
            case STORE:

                /* TODO:
                 * Limit the in-memory space utilized during the STORE mode. The limit (expressed in bytes) 
                 * is a parameter specified as part of the feed ingestion policy. Below is a basic implemenation
                 * that allocates a buffer on demand.   
                 * */

                ByteBuffer storageBuffer = ByteBuffer.allocate(buffer.capacity());
                storageBuffer.put(buffer);
                frames.add(storageBuffer);
                storageBuffer.flip();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Stored frame for " + nodePushable.getDisplayName());
                }
                break;
        }
    }

    /**
     * Detects if the operator is unable to push a frame downstream
     * within a threshold period of time. In addition, it measure the
     * throughput as observed on the output channel of the associated operator.
     */
    private static class HealthMonitor extends TimerTask {

        private static final String EOL = "\n";

        private long startTime = -1;
        private FramePushState state;
        private AtomicLong numTuplesInInterval = new AtomicLong(0);
        private boolean collectThroughput;
        private FeedMessageService mesgService;

        private final FeedConnectionId feedId;
        private final String nodeId;
        private final FeedRuntimeType feedRuntimeType;
        private final int partition;
        private final long period;
        private final FrameTupleAccessor fta;
        private final IFeedManager feedManager;

        public HealthMonitor(FeedConnectionId feedId, String nodeId, FeedRuntimeType feedRuntimeType, int partition,
                Timer timer, FrameTupleAccessor fta, IFeedManager feedManager) {
            this.state = FramePushState.INTIALIZED;
            this.feedId = feedId;
            this.nodeId = nodeId;
            this.feedRuntimeType = feedRuntimeType;
            this.partition = partition;
            this.period = FLUSH_THRESHOLD_TIME;
            this.collectThroughput = feedRuntimeType.equals(FeedRuntimeType.INGESTION);
            this.fta = fta;
            this.feedManager = feedManager;
        }

        public void notifyStartFrameFlushActivity() {
            startTime = System.currentTimeMillis();
            state = FramePushState.WAITING_FOR_FLUSH_COMPLETION;
        }

        /**
         * Reset method is invoked when a live instance of operator needs to take
         * over from the zombie instance from the previously failed execution
         */
        public void reset() {
            mesgService = null;
            collectThroughput = feedRuntimeType.equals(FeedRuntimeType.INGESTION);
        }

        public void notifyFinishFrameFlushActivity() {
            state = FramePushState.WAITNG_FOR_NEXT_FRAME;
            numTuplesInInterval.set(numTuplesInInterval.get() + fta.getTupleCount());
        }

        @Override
        public void run() {
            if (state.equals(FramePushState.WAITING_FOR_FLUSH_COMPLETION)) {
                long currentTime = System.currentTimeMillis();
                if (currentTime - startTime > FLUSH_THRESHOLD_TIME) {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Congestion reported by " + feedRuntimeType + " [" + partition + "]");
                    }
                    sendReportToSuperFeedManager(currentTime - startTime, FeedReportMessageType.CONGESTION,
                            System.currentTimeMillis());
                }
            }
            if (collectThroughput) {
                int instantTput = (int) Math.ceil((((double) numTuplesInInterval.get() * 1000) / period));
                sendReportToSuperFeedManager(instantTput, FeedReportMessageType.THROUGHPUT, System.currentTimeMillis());
            }
            numTuplesInInterval.set(0);
        }

        private void sendReportToSuperFeedManager(long value, SuperFeedManager.FeedReportMessageType mesgType,
                long timestamp) {
            if (mesgService == null) {
                waitTillMessageServiceIsUp();
            }
            String feedRep = feedId.getDataverse() + ":" + feedId.getFeedName() + ":" + feedId.getDatasetName();
            String message = mesgType.name().toLowerCase() + FeedMessageService.MessageSeparator + feedRep
                    + FeedMessageService.MessageSeparator + feedRuntimeType + FeedMessageService.MessageSeparator
                    + partition + FeedMessageService.MessageSeparator + value + FeedMessageService.MessageSeparator
                    + nodeId + FeedMessageService.MessageSeparator + timestamp + FeedMessageService.MessageSeparator
                    + EOL;
            try {
                mesgService.sendMessage(message);
            } catch (IOException ioe) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Unable to send feed report to Super Feed Manager for feed " + feedId + " "
                            + feedRuntimeType + "[" + partition + "]");
                }
            }
        }

        private void waitTillMessageServiceIsUp() {
            while (mesgService == null) {
                mesgService = feedManager.getFeedMessageService(feedId);
                if (mesgService == null) {
                    try {
                        /**
                         * wait for the message service to be available
                         */
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Encountered an interrupted exception " + " Exception " + e);
                        }
                    }
                }
            }
        }

        public void deactivate() {
            // cancel the timer task to avoid future execution. 
            cancel();
            collectThroughput = false;
        }

        private enum FramePushState {
            /**
             * Frame writer has been initialized
             */
            INTIALIZED,

            /**
             * Frame writer is waiting for a pending flush to finish.
             */
            WAITING_FOR_FLUSH_COMPLETION,

            /**
             * Frame writer is waiting to be given the next frame.
             */
            WAITNG_FOR_NEXT_FRAME
        }

    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
        if(healthMonitor != null) {
            if (!healthMonitor.feedRuntimeType.equals(FeedRuntimeType.INGESTION)) {
              healthMonitor.deactivate();
            } else {
              healthMonitor.reset();
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (healthMonitor != null) {
            healthMonitor.deactivate();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Closing frame statistics collection activity" + healthMonitor);
            }
        }
        writer.close();
    }

    public IFrameWriter getWriter() {
        return writer;
    }

    public void setWriter(IFrameWriter writer) {
        this.writer = writer;
    }

    @Override
    public String toString() {
        return "MaterializingFrameWriter using " + writer;
    }

    public List<ByteBuffer> getStoredFrames() {
        return frames;
    }

    public void clear() {
        frames.clear();
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    public void reset() {
        healthMonitor.reset();
    }

}
