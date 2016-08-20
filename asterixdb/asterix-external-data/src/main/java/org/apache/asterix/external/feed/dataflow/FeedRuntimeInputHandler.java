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
package org.apache.asterix.external.feed.dataflow;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ConcurrentFramePool;
import org.apache.asterix.active.FrameAction;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.util.FeedUtils.Mode;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/**
 * TODO: Add Failure cases unit tests for this class
 * Provides for error-handling and input-side buffering for a feed runtime.
 * .............______.............
 * ............|......|............
 * ============|(core)|============
 * ============|( op )|============
 * ^^^^^^^^^^^^|______|............
 * .Input Side.
 * ..Handler...
 **/
public class FeedRuntimeInputHandler extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(FeedRuntimeInputHandler.class.getName());
    private static final double MAX_SPILL_USED_BEFORE_RESUME = 0.8;
    private static final boolean DEBUG = false;
    private final Object mutex = new Object();
    private final FeedExceptionHandler exceptionHandler;
    private final FrameSpiller spiller;
    private final FeedPolicyAccessor fpa;
    private final FrameAction frameAction;
    private final int initialFrameSize;
    private final FrameTransporter consumer;
    private final Thread consumerThread;
    private final LinkedBlockingDeque<ByteBuffer> inbox;
    private final ConcurrentFramePool framePool;
    private Mode mode = Mode.PROCESS;
    private int total = 0;
    private int numDiscarded = 0;
    private int numSpilled = 0;
    private int numProcessedInMemory = 0;
    private int numStalled = 0;

    public FeedRuntimeInputHandler(IHyracksTaskContext ctx, FeedConnectionId connectionId, ActiveRuntimeId runtimeId,
            IFrameWriter writer, FeedPolicyAccessor fpa, FrameTupleAccessor fta, ConcurrentFramePool framePool)
            throws HyracksDataException {
        this.writer = writer;

        this.spiller =
                fpa.spillToDiskOnCongestion() ? new FrameSpiller(ctx,
                        connectionId.getFeedId() + "_" + connectionId.getDatasetName() + "_"
                                + runtimeId.getRuntimeName() + "_" + runtimeId.getPartition(),
                        fpa.getMaxSpillOnDisk()) : null;
        this.exceptionHandler = new FeedExceptionHandler(ctx, fta);
        this.fpa = fpa;
        this.framePool = framePool;
        this.inbox = new LinkedBlockingDeque<>();
        this.consumer = new FrameTransporter();
        this.consumerThread = new Thread(consumer);
        this.consumerThread.start();
        this.initialFrameSize = ctx.getInitialFrameSize();
        this.frameAction = new FrameAction();
    }

    @Override
    public void open() throws HyracksDataException {
        synchronized (writer) {
            writer.open();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        synchronized (writer) {
            writer.fail();
        }
    }

    @Override
    public void close() throws HyracksDataException {
        consumer.poison();
        synchronized (mutex) {
            if (DEBUG) {
                LOGGER.info("Producer is waking up consumer");
            }
            mutex.notify();
        }
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            LOGGER.log(Level.WARNING, e.getMessage(), e);
        }
        try {
            framePool.release(inbox);
        } catch (Throwable th) {
            LOGGER.log(Level.WARNING, th.getMessage(), th);
        }
        try {
            if (spiller != null) {
                spiller.close();
            }
        } catch (Throwable th) {
            LOGGER.log(Level.WARNING, th.getMessage(), th);
        }
        writer.close();
    }

    @Override
    public void nextFrame(ByteBuffer frame) throws HyracksDataException {
        try {
            total++;
            if (consumer.cause() != null) {
                throw consumer.cause();
            }
            if (DEBUG) {
                LOGGER.info("nextFrame() called. inputHandler is in mode: " + mode.toString());
            }
            switch (mode) {
                case PROCESS:
                    process(frame);
                    break;
                case SPILL:
                    spill(frame);
                    break;
                case DISCARD:
                    discard(frame);
                    break;
                default:
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Ignoring incoming tuples in " + mode + " mode");
                    }
                    break;
            }
        } catch (Throwable th) {
            throw new HyracksDataException(th);
        }
    }

    // For unit testing purposes
    public int framesOnDisk() {
        return spiller.remaining();
    }

    private ByteBuffer getFreeBuffer(int frameSize) throws HyracksDataException {
        int numFrames = frameSize / initialFrameSize;
        if (numFrames == 1) {
            return framePool.get();
        } else {
            return framePool.get(frameSize);
        }
    }

    private void discard(ByteBuffer frame) throws HyracksDataException {
        if (DEBUG) {
            LOGGER.info("starting discard(frame)");
        }
        if (fpa.spillToDiskOnCongestion()) {
            if (DEBUG) {
                LOGGER.info("Spilling to disk is enabled. Will try that");
            }
            if (spiller.spill(frame)) {
                numSpilled++;
                mode = Mode.SPILL;
                return;
            }
        } else {
            if (DEBUG) {
                LOGGER.info("Spilling to disk is disabled. Will try to get a buffer");
            }
            ByteBuffer next = getFreeBuffer(frame.capacity());
            if (next != null) {
                if (DEBUG) {
                    LOGGER.info("Was able to get a buffer");
                }
                numProcessedInMemory++;
                next.put(frame);
                inbox.offer(next);
                mode = Mode.PROCESS;
                return;
            }
        }
        if ((numDiscarded + 1.0) / total > fpa.getMaxFractionDiscard()) {
            if (DEBUG) {
                LOGGER.info("in discard(frame). Discard allowance has been consumed. --> Stalling");
            }
            stall(frame);
        } else {
            if (DEBUG) {
                LOGGER.info("in discard(frame). So far, I have discarded " + numDiscarded);
            }
            numDiscarded++;
        }
    }

    private void exitProcessState(ByteBuffer frame) throws HyracksDataException {
        if (fpa.spillToDiskOnCongestion()) {
            mode = Mode.SPILL;
            spiller.open();
            spill(frame);
        } else {
            if (DEBUG) {
                LOGGER.info("Spilling is disabled --> discardOrStall(frame)");
            }
            discardOrStall(frame);
        }
    }

    private void discardOrStall(ByteBuffer frame) throws HyracksDataException {
        if (fpa.discardOnCongestion()) {
            mode = Mode.DISCARD;
            discard(frame);
        } else {
            if (DEBUG) {
                LOGGER.info("Discard is disabled --> stall(frame)");
            }
            stall(frame);
        }
    }

    private void stall(ByteBuffer frame) throws HyracksDataException {
        try {
            if (DEBUG) {
                LOGGER.info("in stall(frame). So far, I have stalled " + numStalled);
            }
            numStalled++;
            // If spilling is enabled, we wait on the spiller
            if (fpa.spillToDiskOnCongestion()) {
                if (DEBUG) {
                    LOGGER.info("in stall(frame). Spilling is enabled so we will attempt to spill");
                }
                waitforSpillSpace();
                spiller.spill(frame);
                numSpilled++;
                synchronized (mutex) {
                    if (DEBUG) {
                        LOGGER.info("Producer is waking up consumer");
                    }
                    mutex.notify();
                }
                return;
            }
            if (DEBUG) {
                LOGGER.info("in stall(frame). Spilling is disabled. We will subscribe to frame pool");
            }
            // Spilling is disabled, we subscribe to feedMemoryManager
            frameAction.setFrame(frame);
            framePool.subscribe(frameAction);
            ByteBuffer temp = frameAction.retrieve();
            inbox.put(temp);
            numProcessedInMemory++;
            if (DEBUG) {
                LOGGER.info("stall(frame) has been completed. Notifying the consumer that a frame is ready");
            }
            synchronized (mutex) {
                if (DEBUG) {
                    LOGGER.info("Producer is waking up consumer");
                }
                mutex.notify();
            }
        } catch (InterruptedException e) {
            throw new HyracksDataException(e);
        }
    }

    private void waitforSpillSpace() throws InterruptedException {
        synchronized (spiller) {
            while (spiller.usedBudget() > MAX_SPILL_USED_BEFORE_RESUME) {
                if (DEBUG) {
                    LOGGER.info("in stall(frame). Spilling has been consumed. We will wait for it to be less than "
                            + MAX_SPILL_USED_BEFORE_RESUME + " consumed. Current consumption = "
                            + spiller.usedBudget());
                }
                spiller.wait();
            }
        }
    }

    private void process(ByteBuffer frame) throws HyracksDataException {
        // Get a page from frame pool
        ByteBuffer next = (frame.capacity() <= framePool.getMaxFrameSize()) ? getFreeBuffer(frame.capacity()) : null;
        if (next != null) {
            // Got a page from memory pool
            numProcessedInMemory++;
            next.put(frame);
            try {
                inbox.put(next);
                notifyMemoryConsumer();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        } else {
            if (DEBUG) {
                LOGGER.info("Couldn't allocate memory --> exitProcessState(frame)");
            }
            // Out of memory. we switch to next mode as per policy
            exitProcessState(frame);
        }
    }

    private void notifyMemoryConsumer() {
        if (inbox.size() == 1) {
            synchronized (mutex) {
                if (DEBUG) {
                    LOGGER.info("Producer is waking up consumer");
                }
                mutex.notify();
            }
        }
    }

    private void spill(ByteBuffer frame) throws HyracksDataException {
        if (spiller.switchToMemory()) {
            synchronized (mutex) {
                // Check if there is memory
                ByteBuffer next = null;
                if (frame.capacity() <= framePool.getMaxFrameSize()) {
                    next = getFreeBuffer(frame.capacity());
                }
                if (next != null) {
                    spiller.close();
                    numProcessedInMemory++;
                    next.put(frame);
                    inbox.offer(next);
                    notifyMemoryConsumer();
                    mode = Mode.PROCESS;
                } else {
                    // spill. This will always succeed since spilled = 0 (TODO must verify that budget can't be 0)
                    spiller.spill(frame);
                    numSpilled++;
                    if (DEBUG) {
                        LOGGER.info("Producer is waking up consumer");
                    }
                    mutex.notify();
                }
            }
        } else {
            // try to spill. If failed switch to either discard or stall
            if (spiller.spill(frame)) {
                notifyDiskConsumer();
                numSpilled++;
            } else {
                if (fpa.discardOnCongestion()) {
                    mode = Mode.DISCARD;
                    discard(frame);
                } else {
                    stall(frame);
                }
            }
        }
    }

    private void notifyDiskConsumer() {
        if (spiller.remaining() == 1) {
            synchronized (mutex) {
                if (DEBUG) {
                    LOGGER.info("Producer is waking up consumer");
                }
                mutex.notify();
            }
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        synchronized (writer) {
            writer.flush();
        }
    }

    public int getNumDiscarded() {
        return numDiscarded;
    }

    public int getNumSpilled() {
        return numSpilled;
    }

    public int getNumProcessedInMemory() {
        return numProcessedInMemory;
    }

    public int getNumStalled() {
        return numStalled;
    }

    private class FrameTransporter implements Runnable {
        private volatile Throwable cause;
        private int consumed = 0;
        private boolean poisoned = false;

        public Throwable cause() {
            return cause;
        }

        public void poison() {
            poisoned = true;
        }

        private Throwable consume(ByteBuffer frame) {
            while (frame != null) {
                try {
                    writer.nextFrame(frame);
                    consumed++;
                    frame = null;
                } catch (HyracksDataException e) {
                    // It is fine to catch throwable here since this thread is always expected to terminate gracefully
                    frame = exceptionHandler.handle(e, frame);
                    if (frame == null) {
                        this.cause = e;
                        return e;
                    }
                } catch (Throwable th) {
                    this.cause = th;
                    return th;
                }
            }
            return null;
        }

        @Override
        public void run() {
            try {
                ByteBuffer frame = inbox.poll();
                while (true) {
                    if (frame != null) {
                        try {
                            if (consume(frame) != null) {
                                return;
                            }
                        } finally {
                            // Done with frame.
                            framePool.release(frame);
                        }
                    }
                    frame = inbox.poll();
                    if (frame == null) {
                        // Memory queue is empty. Check spill
                        if (spiller != null) {
                            frame = spiller.next();
                            while (frame != null) {
                                if (consume(frame) != null) {
                                    // We don't release the frame since this is a spill frame that we didn't get from memory
                                    // manager
                                    return;
                                }
                                frame = spiller.next();
                            }
                        }
                        writer.flush();
                        // At this point. We consumed all memory and spilled
                        // We can't assume the next will be in memory. what if there is 0 memory?
                        synchronized (mutex) {
                            frame = inbox.poll();
                            // Nothing in memory
                            if (frame == null && (spiller == null || spiller.switchToMemory())) {
                                if (poisoned) {
                                    break;
                                }
                                if (DEBUG) {
                                    LOGGER.info("Consumer is going to sleep");
                                }
                                // Nothing in disk
                                mutex.wait();
                                if (DEBUG) {
                                    LOGGER.info("Consumer is waking up");
                                }
                            }
                        }
                    }
                }
            } catch (Throwable th) {
                this.cause = th;
            }
            // cleanup will always be done through the close() call
        }

        @Override
        public String toString() {
            return "consumed: " + consumed;
        }
    }

    public int getTotal() {
        return total;
    }

    public LinkedBlockingDeque<ByteBuffer> getInternalBuffer() {
        return inbox;
    }
}
