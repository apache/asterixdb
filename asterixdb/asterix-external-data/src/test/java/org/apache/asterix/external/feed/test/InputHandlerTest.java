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
package org.apache.asterix.external.feed.test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.common.memory.ConcurrentFramePool;
import org.apache.asterix.external.feed.dataflow.FeedRuntimeInputHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.test.FrameWriterTestUtils;
import org.apache.hyracks.api.test.TestControlledFrameWriter;
import org.apache.hyracks.api.test.TestFrameWriter;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class InputHandlerTest {

    private static final int DEFAULT_FRAME_SIZE = 32768;
    private static final int NUM_FRAMES = 128;
    private static final long FEED_MEM_BUDGET = DEFAULT_FRAME_SIZE * NUM_FRAMES;
    private static final String DATAVERSE = "dataverse";
    private static final String DATASET = "dataset";
    private static final String FEED = "feed";
    private static final String NODE_ID = "NodeId";
    private static final float DISCARD_ALLOWANCE = 0.15f;
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(1);
    private volatile static HyracksDataException cause = null;

    private FeedRuntimeInputHandler createInputHandler(IHyracksTaskContext ctx, IFrameWriter writer,
            FeedPolicyAccessor fpa, ConcurrentFramePool framePool) throws HyracksDataException {
        FrameTupleAccessor fta = Mockito.mock(FrameTupleAccessor.class);
        EntityId feedId = new EntityId(FeedUtils.FEED_EXTENSION_NAME, DATAVERSE, FEED);
        FeedConnectionId connectionId = new FeedConnectionId(feedId, DATASET);
        ActiveRuntimeId runtimeId = new ActiveRuntimeId(feedId, FeedRuntimeType.COLLECT.toString(), 0);
        return new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, writer, fpa, fta, framePool);
    }

    /*
     * Testing the following scenarios
     * 01. Positive Frames memory budget with fixed size frames, no spill, no discard.
     * 02. Positive Frames memory budget with variable size frames, no spill, no discard.
     * 03. Positive Frames memory budget with fixed size frames, with spill, no discard.
     * 04. Positive Frames memory budget with variable size frames, with spill, no discard.
     * 05. Positive Frames memory budget with fixed size frames, no spill, with discard.
     * 06. Positive Frames memory budget with variable size frames, no spill, with discard.
     * 07. Positive Frames memory budget with fixed size frames, with spill, with discard.
     * 08. Positive Frames memory budget with variable size frames, with spill, with discard.
     * 09. 0 Frames memory budget with fixed size frames, with spill, no discard.
     * 10. 0 Frames memory budget with variable size frames, with spill, no discard.
     * 11. TODO 0 Frames memory budget with fixed size frames, with spill, with discard.
     * 12. TODO 0 Frames memory budget with variable size frames, with spill, with discard.
     * 13. TODO Test exception handling with Open, NextFrame,Flush,Close,Fail exception throwing FrameWriter
     * 14. TODO Test exception while waiting for subscription
     */

    private static FeedPolicyAccessor createFeedPolicyAccessor(boolean spill, boolean discard, long spillBudget,
            float discardFraction) {
        FeedPolicyAccessor fpa = Mockito.mock(FeedPolicyAccessor.class);
        Mockito.when(fpa.flowControlEnabled()).thenReturn(true);
        Mockito.when(fpa.spillToDiskOnCongestion()).thenReturn(spill);
        Mockito.when(fpa.getMaxSpillOnDisk()).thenReturn(spillBudget);
        Mockito.when(fpa.discardOnCongestion()).thenReturn(discard);
        Mockito.when(fpa.getMaxFractionDiscard()).thenReturn(discardFraction);
        return fpa;
    }

    private void cleanDiskFiles() throws IOException {
        String filePrefix = "dataverse.feed(Feed)_dataset*";
        Collection<File> files = FileUtils.listFiles(new File("."), new WildcardFileFilter(filePrefix), null);
        for (File ifile : files) {
            Files.deleteIfExists(ifile.toPath());
        }
    }

    @Before
    public void testCleanBefore() throws IOException {
        cleanDiskFiles();
    }

    @After
    public void testCleanAfter() throws IOException {
        cleanDiskFiles();
    }

    @org.junit.Test
    public void testZeroMemoryVarSizeFrameWithDiskNoDiscard() {
        try {
            int numRounds = 5;
            Random random = new Random();
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            // No spill, No discard
            FeedPolicyAccessor fpa =
                    createFeedPolicyAccessor(true, false, NUM_FRAMES * DEFAULT_FRAME_SIZE, DISCARD_ALLOWANCE);
            // Non-Active Writer
            TestFrameWriter writer =
                    FrameWriterTestUtils.create(Collections.emptyList(), Collections.emptyList(), false);
            // FramePool
            ConcurrentFramePool framePool = new ConcurrentFramePool(NODE_ID, 0, DEFAULT_FRAME_SIZE);
            FeedRuntimeInputHandler handler = createInputHandler(ctx, writer, fpa, framePool);
            handler.open();
            ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FRAME_SIZE);
            handler.nextFrame(buffer);
            Assert.assertEquals(0, handler.getNumProcessedInMemory());
            Assert.assertEquals(1, handler.getNumSpilled());
            // add NUM_FRAMES times
            for (int i = 0; i < NUM_FRAMES * numRounds; i++) {
                int multiplier = random.nextInt(10) + 1;
                buffer = ByteBuffer.allocate(DEFAULT_FRAME_SIZE * multiplier);
                handler.nextFrame(buffer);
            }
            // Check that no records were discarded
            Assert.assertEquals(handler.getNumDiscarded(), 0);
            // Check that no records were spilled
            Assert.assertEquals(NUM_FRAMES * numRounds + 1, handler.getNumSpilled());
            writer.validate(false);
            handler.close();
            // Check that nextFrame was called
            Assert.assertEquals(NUM_FRAMES * numRounds + 1, writer.nextFrameCount());
            writer.validate(true);
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail();
        } finally {
            Assert.assertNull(cause);
        }
    }

    @Test
    public void testZeroMemoryFixedSizeFrameWithDiskNoDiscard() {
        try {
            int numRounds = 10;
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            // No spill, No discard
            FeedPolicyAccessor fpa =
                    createFeedPolicyAccessor(true, false, NUM_FRAMES * DEFAULT_FRAME_SIZE, DISCARD_ALLOWANCE);
            // Non-Active Writer
            TestFrameWriter writer =
                    FrameWriterTestUtils.create(Collections.emptyList(), Collections.emptyList(), false);
            // FramePool
            ConcurrentFramePool framePool = new ConcurrentFramePool(NODE_ID, 0, DEFAULT_FRAME_SIZE);
            FeedRuntimeInputHandler handler = createInputHandler(ctx, writer, fpa, framePool);
            handler.open();
            VSizeFrame frame = new VSizeFrame(ctx);
            handler.nextFrame(frame.getBuffer());
            Assert.assertEquals(0, handler.getNumProcessedInMemory());
            Assert.assertEquals(1, handler.getNumSpilled());
            // add NUM_FRAMES times
            for (int i = 0; i < NUM_FRAMES * numRounds; i++) {
                handler.nextFrame(frame.getBuffer());
            }
            // Check that no records were discarded
            Assert.assertEquals(handler.getNumDiscarded(), 0);
            // Check that no records were spilled
            Assert.assertEquals(NUM_FRAMES * numRounds + 1, handler.getNumSpilled());
            writer.validate(false);
            handler.close();
            // Check that nextFrame was called
            Assert.assertEquals(NUM_FRAMES * numRounds + 1, writer.nextFrameCount());
            writer.validate(true);
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail();
        } finally {
            Assert.assertNull(cause);
        }
    }

    /*
     * Spill = false;
     * Discard = true; discard only 5%
     * Fixed size frames
     */
    @Test
    public void testMemoryVarSizeFrameWithSpillWithDiscard() {
        try {
            int numberOfMemoryFrames = 50;
            int numberOfSpillFrames = 50;
            int notDiscarded = 0;
            int totalMinFrames = 0;
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            // Spill budget = Memory budget, No discard
            FeedPolicyAccessor fpa =
                    createFeedPolicyAccessor(true, true, DEFAULT_FRAME_SIZE * numberOfSpillFrames, DISCARD_ALLOWANCE);
            // Non-Active Writer
            TestControlledFrameWriter writer = FrameWriterTestUtils.create(DEFAULT_FRAME_SIZE, false);
            writer.freeze();
            // FramePool
            ConcurrentFramePool framePool =
                    new ConcurrentFramePool(NODE_ID, numberOfMemoryFrames * DEFAULT_FRAME_SIZE, DEFAULT_FRAME_SIZE);
            FeedRuntimeInputHandler handler = createInputHandler(ctx, writer, fpa, framePool);
            handler.open();
            ByteBuffer buffer1 = ByteBuffer.allocate(DEFAULT_FRAME_SIZE);
            ByteBuffer buffer2 = ByteBuffer.allocate(DEFAULT_FRAME_SIZE * 2);
            ByteBuffer buffer3 = ByteBuffer.allocate(DEFAULT_FRAME_SIZE * 3);
            ByteBuffer buffer4 = ByteBuffer.allocate(DEFAULT_FRAME_SIZE * 4);
            ByteBuffer buffer5 = ByteBuffer.allocate(DEFAULT_FRAME_SIZE * 5);
            while (true) {
                if (totalMinFrames + 1 < numberOfMemoryFrames) {
                    handler.nextFrame(buffer1);
                    notDiscarded++;
                    totalMinFrames++;
                } else {
                    break;
                }
                if (totalMinFrames + 2 < numberOfMemoryFrames) {
                    notDiscarded++;
                    totalMinFrames += 2;
                    handler.nextFrame(buffer2);
                } else {
                    break;
                }
                if (totalMinFrames + 3 < numberOfMemoryFrames) {
                    notDiscarded++;
                    totalMinFrames += 3;
                    handler.nextFrame(buffer3);
                } else {
                    break;
                }
            }
            // Now we need to verify that the frame pool memory has been consumed!
            Assert.assertTrue(framePool.remaining() < 3);
            Assert.assertEquals(0, handler.getNumSpilled());
            Assert.assertEquals(0, handler.getNumStalled());
            Assert.assertEquals(0, handler.getNumDiscarded());
            while (true) {
                if (handler.getNumSpilled() < numberOfSpillFrames) {
                    notDiscarded++;
                    handler.nextFrame(buffer3);
                } else {
                    break;
                }
                if (handler.getNumSpilled() < numberOfSpillFrames) {
                    notDiscarded++;
                    handler.nextFrame(buffer4);
                } else {
                    break;
                }
                if (handler.getNumSpilled() < numberOfSpillFrames) {
                    notDiscarded++;
                    handler.nextFrame(buffer5);
                } else {
                    break;
                }
            }
            Assert.assertTrue(framePool.remaining() < 3);
            Assert.assertEquals(handler.framesOnDisk(), handler.getNumSpilled());
            Assert.assertEquals(handler.framesOnDisk(), numberOfSpillFrames);
            Assert.assertEquals(0, handler.getNumStalled());
            Assert.assertEquals(0, handler.getNumDiscarded());
            // We can only discard one frame
            double numDiscarded = 0;
            boolean nextShouldDiscard =
                    ((numDiscarded + 1.0) / (handler.getTotal() + 1.0)) <= fpa.getMaxFractionDiscard();
            while (nextShouldDiscard) {
                handler.nextFrame(buffer5);
                numDiscarded++;
                nextShouldDiscard = ((numDiscarded + 1.0) / (handler.getTotal() + 1.0)) <= fpa.getMaxFractionDiscard();
            }
            Assert.assertTrue(framePool.remaining() < 3);
            Assert.assertEquals(handler.framesOnDisk(), handler.getNumSpilled());
            Assert.assertEquals(0, handler.getNumStalled());
            Assert.assertEquals((int) numDiscarded, handler.getNumDiscarded());
            // Next Call should block since we're exceeding the discard allowance
            Future<?> result = EXECUTOR.submit(new Pusher(buffer5, handler));
            if (result.isDone()) {
                Assert.fail("The producer should switch to stall mode since it is exceeding the discard allowance");
            }
            // consume memory frames
            writer.unfreeze();
            result.get();
            handler.close();
            Assert.assertEquals(writer.nextFrameCount(), notDiscarded + 1);
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail();
        }
        Assert.assertNull(cause);
    }

    /*
     * Spill = true;
     * Discard = true
     * Fixed size frames
     */
    @Test
    public void testMemoryFixedSizeFrameWithSpillWithDiscard() {
        try {
            int numberOfMemoryFrames = 50;
            int numberOfSpillFrames = 50;
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            // Spill budget = Memory budget, No discard
            FeedPolicyAccessor fpa =
                    createFeedPolicyAccessor(true, true, DEFAULT_FRAME_SIZE * numberOfSpillFrames, DISCARD_ALLOWANCE);
            // Non-Active Writer
            TestControlledFrameWriter writer = FrameWriterTestUtils.create(DEFAULT_FRAME_SIZE, false);
            writer.freeze();
            // FramePool
            ConcurrentFramePool framePool =
                    new ConcurrentFramePool(NODE_ID, numberOfMemoryFrames * DEFAULT_FRAME_SIZE, DEFAULT_FRAME_SIZE);
            FeedRuntimeInputHandler handler = createInputHandler(ctx, writer, fpa, framePool);
            handler.open();
            VSizeFrame frame = new VSizeFrame(ctx);
            for (int i = 0; i < numberOfMemoryFrames; i++) {
                handler.nextFrame(frame.getBuffer());
            }
            // Now we need to verify that the frame pool memory has been consumed!
            Assert.assertEquals(0, framePool.remaining());
            Assert.assertEquals(numberOfMemoryFrames, handler.getTotal());
            Assert.assertEquals(0, handler.getNumSpilled());
            Assert.assertEquals(0, handler.getNumStalled());
            Assert.assertEquals(0, handler.getNumDiscarded());
            for (int i = 0; i < numberOfSpillFrames; i++) {
                handler.nextFrame(frame.getBuffer());
            }
            Assert.assertEquals(0, framePool.remaining());
            Assert.assertEquals(numberOfMemoryFrames + numberOfSpillFrames, handler.getTotal());
            Assert.assertEquals(numberOfSpillFrames, handler.getNumSpilled());
            Assert.assertEquals(0, handler.getNumStalled());
            Assert.assertEquals(0, handler.getNumDiscarded());
            // We can only discard one frame
            double numDiscarded = 0;
            boolean nextShouldDiscard =
                    ((numDiscarded + 1.0) / (handler.getTotal() + 1.0)) <= fpa.getMaxFractionDiscard();
            while (nextShouldDiscard) {
                handler.nextFrame(frame.getBuffer());
                numDiscarded++;
                nextShouldDiscard = (numDiscarded + 1.0) / (handler.getTotal() + 1.0) <= fpa.getMaxFractionDiscard();
            }
            Assert.assertEquals(0, framePool.remaining());
            Assert.assertEquals((int) (numberOfMemoryFrames + numberOfSpillFrames + numDiscarded), handler.getTotal());
            Assert.assertEquals(numberOfSpillFrames, handler.getNumSpilled());
            Assert.assertEquals(0, handler.getNumStalled());
            Assert.assertEquals((int) numDiscarded, handler.getNumDiscarded());
            // Next Call should block since we're exceeding the discard allowance
            Future<?> result = EXECUTOR.submit(new Pusher(frame.getBuffer(), handler));
            if (result.isDone()) {
                Assert.fail("The producer should switch to stall mode since it is exceeding the discard allowance");
            } else {
                Assert.assertEquals((int) numDiscarded, handler.getNumDiscarded());
            }
            // consume memory frames
            writer.unfreeze();
            result.get();
            handler.close();
            Assert.assertTrue(result.isDone());
            Assert.assertEquals(writer.nextFrameCount(), numberOfMemoryFrames + numberOfSpillFrames + 1);
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail();
        }
        Assert.assertNull(cause);
    }

    /*
     * Spill = false;
     * Discard = true; discard only 5%
     * Fixed size frames
     */
    @Test
    public void testMemoryVariableSizeFrameNoSpillWithDiscard() {
        try {
            int discardTestFrames = 100;
            Random random = new Random();
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            // Spill budget = Memory budget, No discard
            FeedPolicyAccessor fpa = createFeedPolicyAccessor(false, true, DEFAULT_FRAME_SIZE, DISCARD_ALLOWANCE);
            // Non-Active Writer
            TestControlledFrameWriter writer = FrameWriterTestUtils.create(DEFAULT_FRAME_SIZE, false);
            writer.freeze();
            // FramePool
            ConcurrentFramePool framePool =
                    new ConcurrentFramePool(NODE_ID, discardTestFrames * DEFAULT_FRAME_SIZE, DEFAULT_FRAME_SIZE);
            FeedRuntimeInputHandler handler = createInputHandler(ctx, writer, fpa, framePool);
            handler.open();
            // add NUM_FRAMES times
            ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FRAME_SIZE);
            int multiplier = 1;
            int numFrames = 0;
            // add NUM_FRAMES times
            while ((multiplier <= framePool.remaining())) {
                numFrames++;
                handler.nextFrame(buffer);
                multiplier = random.nextInt(10) + 1;
                buffer = ByteBuffer.allocate(DEFAULT_FRAME_SIZE * multiplier);
            }
            // Next call should NOT block but should discard.
            double numDiscarded = 0.0;
            boolean nextShouldDiscard =
                    ((numDiscarded + 1.0) / (handler.getTotal() + 1.0)) <= fpa.getMaxFractionDiscard();
            while (nextShouldDiscard) {
                handler.nextFrame(buffer);
                numDiscarded++;
                nextShouldDiscard = ((numDiscarded + 1.0) / (handler.getTotal() + 1.0)) <= fpa.getMaxFractionDiscard();
            }
            Future<?> result = EXECUTOR.submit(new Pusher(buffer, handler));
            if (result.isDone()) {
                Assert.fail("The producer should switch to stall mode since it is exceeding the discard allowance");
            } else {
                // Check that no records were discarded
                Assert.assertEquals((int) numDiscarded, handler.getNumDiscarded());
                // Check that one frame is spilled
                Assert.assertEquals(handler.getNumSpilled(), 0);
            }
            // consume memory frames
            writer.unfreeze();
            result.get();
            handler.close();
            Assert.assertEquals(writer.nextFrameCount(), numFrames + 1);
            // exit
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail();
        }
        Assert.assertNull(cause);
    }

    /*
     * Spill = false;
     * Discard = true; discard only 5%
     * Fixed size frames
     */
    @Test
    public void testMemoryFixedSizeFrameNoSpillWithDiscard() {
        try {
            int discardTestFrames = 100;
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            // Spill budget = Memory budget, No discard
            FeedPolicyAccessor fpa = createFeedPolicyAccessor(false, true, DEFAULT_FRAME_SIZE, DISCARD_ALLOWANCE);
            // Non-Active Writer
            TestControlledFrameWriter writer = FrameWriterTestUtils.create(DEFAULT_FRAME_SIZE, false);
            writer.freeze();
            // FramePool
            ConcurrentFramePool framePool =
                    new ConcurrentFramePool(NODE_ID, discardTestFrames * DEFAULT_FRAME_SIZE, DEFAULT_FRAME_SIZE);
            FeedRuntimeInputHandler handler = createInputHandler(ctx, writer, fpa, framePool);
            handler.open();
            VSizeFrame frame = new VSizeFrame(ctx);
            // add NUM_FRAMES times
            for (int i = 0; i < discardTestFrames; i++) {
                handler.nextFrame(frame.getBuffer());
            }
            // Next 5 calls call should NOT block but should discard.
            double numDiscarded = 0.0;
            boolean nextShouldDiscard =
                    ((numDiscarded + 1.0) / (handler.getTotal() + 1.0)) <= fpa.getMaxFractionDiscard();
            while (nextShouldDiscard) {
                handler.nextFrame(frame.getBuffer());
                numDiscarded++;
                nextShouldDiscard = ((numDiscarded + 1.0) / (handler.getTotal() + 1.0)) <= fpa.getMaxFractionDiscard();
            }
            // Next Call should block since we're exceeding the discard allowance
            Future<?> result = EXECUTOR.submit(new Pusher(frame.getBuffer(), handler));
            if (result.isDone()) {
                Assert.fail("The producer should switch to stall mode since it is exceeding the discard allowance");
            } else {
                // Check that no records were discarded
                Assert.assertEquals((int) numDiscarded, handler.getNumDiscarded());
                // Check that one frame is spilled
                Assert.assertEquals(handler.getNumSpilled(), 0);
            }
            // consume memory frames
            writer.unfreeze();
            result.get();
            handler.close();
            Assert.assertEquals(writer.nextFrameCount(), discardTestFrames + 1);
            // exit
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail();
        }
        Assert.assertNull(cause);
    }

    /*
     * Spill = true;
     * Discard = false;
     * Fixed size frames
     */
    @Test
    public void testMemoryFixedSizeFrameWithSpillNoDiscard() {
        try {
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            // Spill budget = Memory budget, No discard
            FeedPolicyAccessor fpa =
                    createFeedPolicyAccessor(true, false, DEFAULT_FRAME_SIZE * NUM_FRAMES, DISCARD_ALLOWANCE);
            // Non-Active Writer
            TestControlledFrameWriter writer = FrameWriterTestUtils.create(DEFAULT_FRAME_SIZE, false);
            writer.freeze();
            // FramePool
            ConcurrentFramePool framePool = new ConcurrentFramePool(NODE_ID, FEED_MEM_BUDGET, DEFAULT_FRAME_SIZE);
            FeedRuntimeInputHandler handler = createInputHandler(ctx, writer, fpa, framePool);
            handler.open();
            VSizeFrame frame = new VSizeFrame(ctx);
            // add NUM_FRAMES times
            for (int i = 0; i < NUM_FRAMES; i++) {
                handler.nextFrame(frame.getBuffer());
            }
            // Next call should NOT block. we will do it in a different thread
            Future<?> result = EXECUTOR.submit(new Pusher(frame.getBuffer(), handler));
            result.get();
            // Check that no records were discarded
            Assert.assertEquals(handler.getNumDiscarded(), 0);
            // Check that one frame is spilled
            Assert.assertEquals(handler.getNumSpilled(), 1);
            // consume memory frames
            writer.unfreeze();
            handler.close();
            Assert.assertEquals(handler.framesOnDisk(), 0);
            // exit
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail();
        }
        Assert.assertNull(cause);
    }

    /*
     * Spill = false;
     * Discard = false;
     * Fixed size frames
     * Very fast next operator
     */
    @Test
    public void testMemoryFixedSizeFrameNoDiskNoDiscardFastConsumer() {
        try {
            int numRounds = 10;
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            // No spill, No discard
            FeedPolicyAccessor fpa = createFeedPolicyAccessor(false, false, 0L, DISCARD_ALLOWANCE);
            // Non-Active Writer
            TestFrameWriter writer =
                    FrameWriterTestUtils.create(Collections.emptyList(), Collections.emptyList(), false);
            // FramePool
            ConcurrentFramePool framePool = new ConcurrentFramePool(NODE_ID, FEED_MEM_BUDGET, DEFAULT_FRAME_SIZE);
            FeedRuntimeInputHandler handler = createInputHandler(ctx, writer, fpa, framePool);
            handler.open();
            VSizeFrame frame = new VSizeFrame(ctx);
            // add NUM_FRAMES times
            for (int i = 0; i < NUM_FRAMES * numRounds; i++) {
                handler.nextFrame(frame.getBuffer());
            }
            // Check that no records were discarded
            Assert.assertEquals(handler.getNumDiscarded(), 0);
            // Check that no records were spilled
            Assert.assertEquals(handler.getNumSpilled(), 0);
            writer.validate(false);
            handler.close();
            // Check that nextFrame was called
            Assert.assertEquals(NUM_FRAMES * numRounds, writer.nextFrameCount());
            writer.validate(true);
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail();
        }
        Assert.assertNull(cause);
    }

    /*
     * Spill = false;
     * Discard = false;
     * Fixed size frames
     * Slow next operator
     */
    @Test
    public void testMemoryFixedSizeFrameNoDiskNoDiscardSlowConsumer() {
        try {
            int numRounds = 10;
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            // No spill, No discard
            FeedPolicyAccessor fpa = createFeedPolicyAccessor(false, false, 0L, DISCARD_ALLOWANCE);
            // Non-Active Writer
            TestFrameWriter writer =
                    FrameWriterTestUtils.create(Collections.emptyList(), Collections.emptyList(), false);
            // FramePool
            ConcurrentFramePool framePool = new ConcurrentFramePool(NODE_ID, FEED_MEM_BUDGET, DEFAULT_FRAME_SIZE);
            FeedRuntimeInputHandler handler = createInputHandler(ctx, writer, fpa, framePool);
            handler.open();
            VSizeFrame frame = new VSizeFrame(ctx);
            writer.setNextDuration(1);
            // add NUM_FRAMES times
            for (int i = 0; i < NUM_FRAMES * numRounds; i++) {
                handler.nextFrame(frame.getBuffer());
            }
            // Check that no records were discarded
            Assert.assertEquals(handler.getNumDiscarded(), 0);
            // Check that no records were spilled
            Assert.assertEquals(handler.getNumSpilled(), 0);
            // Check that nextFrame was called
            writer.validate(false);
            handler.close();
            Assert.assertEquals(writer.nextFrameCount(), (NUM_FRAMES * numRounds));
            writer.validate(true);
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail();
        }
        Assert.assertNull(cause);
    }

    /*
     * Spill = false
     * Discard = false
     * VarSizeFrame
     */
    @Test
    public void testMemoryVarSizeFrameNoDiskNoDiscard() {
        try {
            Random random = new Random();
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            // No spill, No discard
            FeedPolicyAccessor fpa = createFeedPolicyAccessor(false, false, 0L, DISCARD_ALLOWANCE);
            // Non-Active Writer
            TestControlledFrameWriter writer = FrameWriterTestUtils.create(DEFAULT_FRAME_SIZE, false);
            writer.freeze();
            // FramePool
            ConcurrentFramePool framePool = new ConcurrentFramePool(NODE_ID, FEED_MEM_BUDGET, DEFAULT_FRAME_SIZE);
            FeedRuntimeInputHandler handler = createInputHandler(ctx, writer, fpa, framePool);
            handler.open();
            ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FRAME_SIZE);
            int multiplier = 1;
            // add NUM_FRAMES times
            while ((multiplier <= framePool.remaining())) {
                handler.nextFrame(buffer);
                multiplier = random.nextInt(10) + 1;
                buffer = ByteBuffer.allocate(DEFAULT_FRAME_SIZE * multiplier);
            }
            // we can't satisfy the next request
            // Next call should block we will do it in a different thread
            Future<?> result = EXECUTOR.submit(new Pusher(buffer, handler));
            // Check that the nextFrame didn't return
            if (result.isDone()) {
                Assert.fail();
            }
            // Check that no records were discarded
            Assert.assertEquals(handler.getNumDiscarded(), 0);
            // Check that no records were spilled
            Assert.assertEquals(handler.getNumSpilled(), 0);
            // Check that number of stalled is not greater than 1
            Assert.assertTrue(handler.getNumStalled() <= 1);
            writer.unfreeze();
            handler.close();
            result.get();
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail();
        }
        Assert.assertNull(cause);
    }

    /*
     * Spill = true;
     * Discard = false;
     * Variable size frames
     */
    @Test
    public void testMemoryVarSizeFrameWithSpillNoDiscard() {
        for (int k = 0; k < 1000; k++) {
            try {
                Random random = new Random();
                IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
                // Spill budget = Memory budget, No discard
                FeedPolicyAccessor fpa =
                        createFeedPolicyAccessor(true, false, DEFAULT_FRAME_SIZE * NUM_FRAMES, DISCARD_ALLOWANCE);
                // Non-Active Writer
                TestControlledFrameWriter writer = FrameWriterTestUtils.create(DEFAULT_FRAME_SIZE, false);
                writer.freeze();
                // FramePool
                ConcurrentFramePool framePool = new ConcurrentFramePool(NODE_ID, FEED_MEM_BUDGET, DEFAULT_FRAME_SIZE);
                FeedRuntimeInputHandler handler = createInputHandler(ctx, writer, fpa, framePool);
                handler.open();
                ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FRAME_SIZE);
                int multiplier = 1;
                int numOfBuffersInMemory = 0;
                // add NUM_FRAMES times
                while ((multiplier <= framePool.remaining())) {
                    numOfBuffersInMemory++;
                    handler.nextFrame(buffer);
                    multiplier = random.nextInt(10) + 1;
                    buffer = ByteBuffer.allocate(DEFAULT_FRAME_SIZE * multiplier);
                }
                // Next call should Not block. we will do it in a different thread
                Future<?> result = EXECUTOR.submit(new Pusher(buffer, handler));
                result.get();
                // Check that no records were discarded
                Assert.assertEquals(handler.getNumDiscarded(), 0);
                // Check that one frame is spilled
                Assert.assertEquals(handler.getNumSpilled(), 1);
                // consume memory frames
                while (numOfBuffersInMemory > 1) {
                    writer.kick();
                    numOfBuffersInMemory--;
                }
                // There should be 1 frame on disk
                Assert.assertEquals(1, handler.framesOnDisk());
                writer.unfreeze();
                handler.close();
                Assert.assertEquals(0, handler.framesOnDisk());
            } catch (Throwable th) {
                th.printStackTrace();
                Assert.fail();
            }
        }
        Assert.assertNull(cause);
    }

    /*
     * Spill = false;
     * Discard = false;
     * Fixed size frames
     */
    @Test
    public void testMemoryFixedSizeFrameNoDiskNoDiscard() {
        try {
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            // No spill, No discard
            FeedPolicyAccessor fpa = createFeedPolicyAccessor(false, false, 0L, DISCARD_ALLOWANCE);
            // Non-Active Writer
            TestControlledFrameWriter writer = FrameWriterTestUtils.create(DEFAULT_FRAME_SIZE, false);
            writer.freeze();
            // FramePool
            ConcurrentFramePool framePool = new ConcurrentFramePool(NODE_ID, FEED_MEM_BUDGET, DEFAULT_FRAME_SIZE);

            FeedRuntimeInputHandler handler = createInputHandler(ctx, writer, fpa, framePool);
            handler.open();
            VSizeFrame frame = new VSizeFrame(ctx);
            // add NUM_FRAMES times
            for (int i = 0; i < NUM_FRAMES; i++) {
                handler.nextFrame(frame.getBuffer());
            }
            // Next call should block we will do it in a different thread
            Future<?> result = EXECUTOR.submit(new Pusher(frame.getBuffer(), handler));
            // Check that the nextFrame didn't return
            if (result.isDone()) {
                Assert.fail();
            } else {
                // Check that no records were discarded
                Assert.assertEquals(handler.getNumDiscarded(), 0);
                // Check that no records were spilled
                Assert.assertEquals(handler.getNumSpilled(), 0);
                // Check that no records were discarded
                // Check that the inputHandler subscribed to the framePool
                // Check that number of stalled is not greater than 1
                Assert.assertTrue(handler.getNumStalled() <= 1);
                writer.kick();
            }
            result.get();
            writer.unfreeze();
            handler.close();
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail();
        }
        Assert.assertNull(cause);
    }

    private class Pusher implements Runnable {
        private final ByteBuffer buffer;
        private final IFrameWriter writer;

        public Pusher(ByteBuffer buffer, IFrameWriter writer) {
            this.buffer = buffer;
            this.writer = writer;
        }

        @Override
        public void run() {
            try {
                writer.nextFrame(buffer);
            } catch (HyracksDataException e) {
                e.printStackTrace();
                cause = e;
            }
        }
    }
}
