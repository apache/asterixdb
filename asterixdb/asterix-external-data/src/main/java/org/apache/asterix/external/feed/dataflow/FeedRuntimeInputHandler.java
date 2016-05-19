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

import org.apache.asterix.external.feed.api.IFeedRuntime.Mode;
import org.apache.asterix.external.feed.management.ConcurrentFramePool;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.runtime.FeedRuntimeId;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/**
 * TODO: Add unit test cases for this class
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
    private static final ByteBuffer POISON_PILL = ByteBuffer.allocate(0);
    private static final double MAX_SPILL_USED_BEFORE_RESUME = 0.8;
    private final FeedExceptionHandler exceptionHandler;
    private final FrameSpiller spiller;
    private final FeedPolicyAccessor fpa;
    private final FrameAction frameAction;
    private final int initialFrameSize;
    private final FrameTransporter consumer;
    private final Thread consumerThread;
    private final LinkedBlockingDeque<ByteBuffer> inbox;
    private final ConcurrentFramePool memoryManager;
    private Mode mode = Mode.PROCESS;
    private int numDiscarded = 0;
    private int numSpilled = 0;
    private int numProcessedInMemory = 0;
    private int numStalled = 0;

    public FeedRuntimeInputHandler(IHyracksTaskContext ctx, FeedConnectionId connectionId, FeedRuntimeId runtimeId,
            IFrameWriter writer, FeedPolicyAccessor fpa, FrameTupleAccessor fta, ConcurrentFramePool feedMemoryManager)
            throws HyracksDataException {
        this.writer = writer;
        this.spiller =
                new FrameSpiller(ctx,
                        connectionId.getFeedId() + "_" + connectionId.getDatasetName() + "_"
                                + runtimeId.getFeedRuntimeType() + "_" + runtimeId.getPartition(),
                        fpa.getMaxSpillOnDisk());
        this.exceptionHandler = new FeedExceptionHandler(ctx, fta);
        this.fpa = fpa;
        this.memoryManager = feedMemoryManager;
        this.inbox = new LinkedBlockingDeque<>();
        this.consumer = new FrameTransporter();
        this.consumerThread = new Thread();
        this.consumerThread.start();
        this.initialFrameSize = ctx.getInitialFrameSize();
        this.frameAction = new FrameAction(inbox);
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
        inbox.add(POISON_PILL);
        notify();
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            LOGGER.log(Level.WARNING, e.getMessage(), e);
        }
        try {
            memoryManager.release(inbox);
        } catch (Throwable th) {
            LOGGER.log(Level.WARNING, th.getMessage(), th);
        }
        try {
            spiller.close();
        } catch (Throwable th) {
            LOGGER.log(Level.WARNING, th.getMessage(), th);
        }
        writer.close();
    }

    @Override
    public void nextFrame(ByteBuffer frame) throws HyracksDataException {
        try {
            if (consumer.cause() != null) {
                throw consumer.cause();
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

    private ByteBuffer getFreeBuffer(int frameSize) throws HyracksDataException {
        int numFrames = frameSize / initialFrameSize;
        if (numFrames == 1) {
            return memoryManager.get();
        } else {
            return memoryManager.get(frameSize);
        }
    }

    private void discard(ByteBuffer frame) throws HyracksDataException {
        if (fpa.spillToDiskOnCongestion()) {
            if (spiller.spill(frame)) {
                numSpilled++;
                mode = Mode.SPILL;
                return;
            }
        } else {
            ByteBuffer next = getFreeBuffer(frame.capacity());
            if (next != null) {
                numProcessedInMemory++;
                next.put(frame);
                inbox.offer(next);
                mode = Mode.PROCESS;
                return;
            }
        }
        numDiscarded++;
    }

    private synchronized void exitProcessState(ByteBuffer frame) throws HyracksDataException {
        if (fpa.spillToDiskOnCongestion()) {
            mode = Mode.SPILL;
            spiller.open();
            spill(frame);
        } else {
            discardOrStall(frame);
        }
    }

    private void discardOrStall(ByteBuffer frame) throws HyracksDataException {
        if (fpa.discardOnCongestion()) {
            numDiscarded++;
            mode = Mode.DISCARD;
            discard(frame);
        } else {
            stall(frame);
        }
    }

    private void stall(ByteBuffer frame) throws HyracksDataException {
        try {
            numStalled++;
            // If spilling is enabled, we wait on the spiller
            if (fpa.spillToDiskOnCongestion()) {
                synchronized (spiller) {
                    while (spiller.usedBudget() > MAX_SPILL_USED_BEFORE_RESUME) {
                        spiller.wait();
                    }
                }
                spiller.spill(frame);
                synchronized (this) {
                    notify();
                }
                return;
            }
            // Spilling is disabled, we subscribe to feedMemoryManager
            frameAction.setFrame(frame);
            synchronized (frameAction) {
                if (memoryManager.subscribe(frameAction)) {
                    frameAction.wait();
                }
            }
            synchronized (this) {
                notify();
            }
        } catch (InterruptedException e) {
            throw new HyracksDataException(e);
        }
    }

    private void process(ByteBuffer frame) throws HyracksDataException {
        // Get a page from
        ByteBuffer next = getFreeBuffer(frame.capacity());
        if (next != null) {
            numProcessedInMemory++;
            next.put(frame);
            inbox.offer(next);
            if (inbox.size() == 1) {
                synchronized (this) {
                    notify();
                }
            }
        } else {
            // out of memory. we switch to next mode as per policy -- synchronized method
            exitProcessState(frame);
        }
    }

    private void spill(ByteBuffer frame) throws HyracksDataException {
        if (spiller.switchToMemory()) {
            synchronized (this) {
                // Check if there is memory
                ByteBuffer next = getFreeBuffer(frame.capacity());
                if (next != null) {
                    spiller.close();
                    numProcessedInMemory++;
                    next.put(frame);
                    inbox.offer(next);
                    mode = Mode.PROCESS;
                } else {
                    // spill. This will always succeed since spilled = 0 (must verify that budget can't be 0)
                    spiller.spill(frame);
                    numSpilled++;
                    notify();
                }
            }
        } else {
            // try to spill. If failed switch to either discard or stall
            if (spiller.spill(frame)) {
                numSpilled++;
            } else {
                if (fpa.discardOnCongestion()) {
                    discard(frame);
                } else {
                    stall(frame);
                }
            }
        }
    }

    public Mode getMode() {
        return mode;
    }

    public synchronized void setMode(Mode mode) {
        this.mode = mode;
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

        public Throwable cause() {
            return cause;
        }

        private Throwable consume(ByteBuffer frame) {
            while (frame != null) {
                try {
                    writer.nextFrame(frame);
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
                while (frame != POISON_PILL) {
                    if (frame != null) {
                        try {
                            if (consume(frame) != null) {
                                return;
                            }
                        } finally {
                            // Done with frame.
                            memoryManager.release(frame);
                        }
                    }
                    frame = inbox.poll();
                    if (frame == null) {
                        // Memory queue is empty. Check spill
                        frame = spiller.next();
                        while (frame != null) {
                            if (consume(frame) != null) {
                                // We don't release the frame since this is a spill frame that we didn't get from memory
                                // manager
                                return;
                            }
                            frame = spiller.next();
                        }
                        writer.flush();
                        // At this point. We consumed all memory and spilled
                        // We can't assume the next will be in memory. what if there is 0 memory?
                        synchronized (FeedRuntimeInputHandler.this) {
                            frame = inbox.poll();
                            if (frame == null) {
                                // Nothing in memory
                                if (spiller.switchToMemory()) {
                                    // Nothing in disk
                                    FeedRuntimeInputHandler.this.wait();
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
    }
}
