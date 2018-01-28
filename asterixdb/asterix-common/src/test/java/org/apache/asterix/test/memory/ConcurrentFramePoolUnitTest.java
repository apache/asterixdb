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
package org.apache.asterix.test.memory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.asterix.common.config.ActiveProperties;
import org.apache.asterix.common.memory.ConcurrentFramePool;
import org.apache.asterix.common.memory.FrameAction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Assert;
import org.mockito.Mockito;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class ConcurrentFramePoolUnitTest extends TestCase {

    private static final int DEFAULT_FRAME_SIZE = 32768;
    private static final int NUM_FRAMES = 2048;
    private static final long FEED_MEM_BUDGET = DEFAULT_FRAME_SIZE * NUM_FRAMES;
    private static final int NUM_THREADS = 8;
    private static final int MAX_SIZE = 52;
    private static final double RELEASE_PROBABILITY = 0.20;
    private volatile static HyracksDataException cause = null;

    public ConcurrentFramePoolUnitTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(ConcurrentFramePoolUnitTest.class);
    }

    @org.junit.Test
    public void testMemoryManager() {
        ActiveProperties afp = Mockito.mock(ActiveProperties.class);
        Mockito.when(afp.getMemoryComponentGlobalBudget()).thenReturn(FEED_MEM_BUDGET);
        ConcurrentFramePool fmm =
                new ConcurrentFramePool("TestNode", afp.getMemoryComponentGlobalBudget(), DEFAULT_FRAME_SIZE);
        int i = 0;
        while (fmm.get() != null) {
            i++;
        }
        Assert.assertEquals(i, NUM_FRAMES);
        Assert.assertNull(cause);
    }

    @org.junit.Test
    public void testConcurrentMemoryManager() {
        try {
            ActiveProperties afp = Mockito.mock(ActiveProperties.class);
            Mockito.when(afp.getMemoryComponentGlobalBudget()).thenReturn(FEED_MEM_BUDGET);
            ConcurrentFramePool fmm =
                    new ConcurrentFramePool("TestNode", afp.getMemoryComponentGlobalBudget(), DEFAULT_FRAME_SIZE);
            FixedSizeAllocator[] runners = new FixedSizeAllocator[NUM_THREADS];
            Thread[] threads = new Thread[NUM_THREADS];
            Arrays.parallelSetAll(runners, (int i) -> new FixedSizeAllocator(fmm));
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(runners[i]);
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].start();
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
            int i = 0;
            for (FixedSizeAllocator allocator : runners) {
                i += allocator.getAllocated();
            }
            Assert.assertEquals(NUM_FRAMES, i);
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail(th.getMessage());
        }
        Assert.assertNull(cause);
    }

    @org.junit.Test
    public void testVarSizeMemoryManager() {
        try {
            ActiveProperties afp = Mockito.mock(ActiveProperties.class);
            Mockito.when(afp.getMemoryComponentGlobalBudget()).thenReturn(FEED_MEM_BUDGET);
            ConcurrentFramePool fmm =
                    new ConcurrentFramePool("TestNode", afp.getMemoryComponentGlobalBudget(), DEFAULT_FRAME_SIZE);
            Random random = new Random();
            int i = 0;
            int req;
            while (true) {
                req = random.nextInt(MAX_SIZE) + 1;
                if (req == 1) {
                    if (fmm.get() != null) {
                        i += 1;
                    } else {
                        break;
                    }
                } else if (fmm.get(req * DEFAULT_FRAME_SIZE) != null) {
                    i += req;
                } else {
                    break;
                }
            }

            Assert.assertEquals(i <= NUM_FRAMES, true);
            Assert.assertEquals(i + req > NUM_FRAMES, true);
            Assert.assertEquals(i + fmm.remaining(), NUM_FRAMES);
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail(th.getMessage());
        }
        Assert.assertNull(cause);
    }

    @org.junit.Test
    public void testConcurrentVarSizeMemoryManager() {
        try {
            ActiveProperties afp = Mockito.mock(ActiveProperties.class);
            Mockito.when(afp.getMemoryComponentGlobalBudget()).thenReturn(FEED_MEM_BUDGET);
            ConcurrentFramePool fmm =
                    new ConcurrentFramePool("TestNode", afp.getMemoryComponentGlobalBudget(), DEFAULT_FRAME_SIZE);

            VarSizeAllocator[] runners = new VarSizeAllocator[NUM_THREADS];
            Thread[] threads = new Thread[NUM_THREADS];
            Arrays.parallelSetAll(runners, (int i) -> new VarSizeAllocator(fmm));
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(runners[i]);
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].start();
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
            int allocated = 0;
            for (int i = 0; i < threads.length; i++) {
                if (runners[i].cause() != null) {
                    runners[i].cause().printStackTrace();
                    Assert.fail(runners[i].cause().getMessage());
                }
                allocated += runners[i].getAllocated();
            }
            Assert.assertEquals(allocated <= NUM_FRAMES, true);
            for (int i = 0; i < threads.length; i++) {
                Assert.assertEquals(allocated + runners[i].getLastReq() > NUM_FRAMES, true);
            }
            Assert.assertEquals(allocated + fmm.remaining(), NUM_FRAMES);
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail(th.getMessage());
        }
        Assert.assertNull(cause);
    }

    @org.junit.Test
    public void testAcquireReleaseMemoryManager() throws HyracksDataException {
        ActiveProperties afp = Mockito.mock(ActiveProperties.class);
        Mockito.when(afp.getMemoryComponentGlobalBudget()).thenReturn(FEED_MEM_BUDGET);
        ConcurrentFramePool fmm =
                new ConcurrentFramePool("TestNode", afp.getMemoryComponentGlobalBudget(), DEFAULT_FRAME_SIZE);
        Random random = new Random();
        ArrayDeque<ByteBuffer> stack = new ArrayDeque<>();
        while (true) {
            if (random.nextDouble() < RELEASE_PROBABILITY) {
                if (!stack.isEmpty()) {
                    fmm.release(stack.pop());
                }
            } else {
                ByteBuffer buffer = fmm.get();
                if (buffer == null) {
                    break;
                } else {
                    stack.push(buffer);
                }
            }
        }
        Assert.assertEquals(stack.size(), NUM_FRAMES);
        Assert.assertEquals(fmm.remaining(), 0);
        for (ByteBuffer buffer : stack) {
            fmm.release(buffer);
        }
        stack.clear();
        Assert.assertEquals(fmm.remaining(), NUM_FRAMES);
        Assert.assertNull(cause);
    }

    @org.junit.Test
    public void testConcurrentAcquireReleaseMemoryManager() {
        try {
            ActiveProperties afp = Mockito.mock(ActiveProperties.class);
            Mockito.when(afp.getMemoryComponentGlobalBudget()).thenReturn(FEED_MEM_BUDGET);
            ConcurrentFramePool fmm =
                    new ConcurrentFramePool("TestNode", afp.getMemoryComponentGlobalBudget(), DEFAULT_FRAME_SIZE);
            FixedSizeGoodAllocator[] runners = new FixedSizeGoodAllocator[NUM_THREADS];
            Thread[] threads = new Thread[NUM_THREADS];
            Arrays.parallelSetAll(runners, (int i) -> new FixedSizeGoodAllocator(fmm));
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(runners[i]);
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].start();
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
            int i = 0;
            for (FixedSizeGoodAllocator allocator : runners) {
                i += allocator.getAllocated();
            }
            Assert.assertEquals(NUM_FRAMES, i);
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail(th.getMessage());
        }
        Assert.assertNull(cause);
    }

    @org.junit.Test
    public void testAcquireReleaseVarSizeMemoryManager() {
        try {
            ActiveProperties afp = Mockito.mock(ActiveProperties.class);
            Mockito.when(afp.getMemoryComponentGlobalBudget()).thenReturn(FEED_MEM_BUDGET);
            ConcurrentFramePool fmm =
                    new ConcurrentFramePool("TestNode", afp.getMemoryComponentGlobalBudget(), DEFAULT_FRAME_SIZE);
            Random random = new Random();
            ArrayDeque<ByteBuffer> stack = new ArrayDeque<>();
            int i = 0;
            int req;
            while (true) {
                // release
                if (random.nextDouble() < RELEASE_PROBABILITY) {
                    if (!stack.isEmpty()) {
                        ByteBuffer buffer = stack.pop();
                        i -= (buffer.capacity() / DEFAULT_FRAME_SIZE);
                        fmm.release(buffer);
                    }
                } else {
                    // acquire
                    req = random.nextInt(MAX_SIZE) + 1;
                    if (req == 1) {
                        ByteBuffer buffer = fmm.get();
                        if (buffer != null) {
                            stack.push(buffer);
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        ByteBuffer buffer = fmm.get(req * DEFAULT_FRAME_SIZE);
                        if (buffer != null) {
                            stack.push(buffer);
                            i += req;
                        } else {
                            break;
                        }
                    }
                }
            }

            Assert.assertEquals(i <= NUM_FRAMES, true);
            Assert.assertEquals(i + req > NUM_FRAMES, true);
            Assert.assertEquals(i + fmm.remaining(), NUM_FRAMES);
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail(th.getMessage());
        } finally {
            Assert.assertNull(cause);
        }
    }

    @org.junit.Test
    public void testConcurrentAcquireReleaseVarSizeMemoryManager() {
        try {
            ActiveProperties afp = Mockito.mock(ActiveProperties.class);
            Mockito.when(afp.getMemoryComponentGlobalBudget()).thenReturn(FEED_MEM_BUDGET);
            ConcurrentFramePool fmm =
                    new ConcurrentFramePool("TestNode", afp.getMemoryComponentGlobalBudget(), DEFAULT_FRAME_SIZE);
            VarSizeGoodAllocator[] runners = new VarSizeGoodAllocator[NUM_THREADS];
            Thread[] threads = new Thread[NUM_THREADS];
            Arrays.parallelSetAll(runners, (int i) -> new VarSizeGoodAllocator(fmm));
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(runners[i]);
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].start();
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
            int i = 0;
            for (VarSizeGoodAllocator allocator : runners) {
                if (allocator.cause() != null) {
                    allocator.cause().printStackTrace();
                    Assert.fail(allocator.cause().getMessage());
                }
                i += allocator.getAllocated();
            }
            Assert.assertEquals(NUM_FRAMES, i + fmm.remaining());
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail(th.getMessage());
        } finally {
            Assert.assertNull(cause);
        }
    }

    @org.junit.Test
    public void testFixedSizeSubscribtion() {
        try {
            ActiveProperties afp = Mockito.mock(ActiveProperties.class);
            Mockito.when(afp.getMemoryComponentGlobalBudget()).thenReturn(FEED_MEM_BUDGET);
            ConcurrentFramePool fmm =
                    new ConcurrentFramePool("TestNode", afp.getMemoryComponentGlobalBudget(), DEFAULT_FRAME_SIZE);
            int i = 0;
            ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FRAME_SIZE);
            LinkedBlockingDeque<ByteBuffer> buffers = new LinkedBlockingDeque<>();
            FrameAction frameAction = new FrameAction();
            frameAction.setFrame(buffer);
            while (!fmm.subscribe(frameAction)) {
                buffers.put(frameAction.retrieve());
                i++;
            }
            // One subscriber.
            // Check that all frames have been consumed
            Assert.assertEquals(i, NUM_FRAMES);
            // Release a frame (That will be handed out to the subscriber)
            fmm.release(buffers.take());
            // Check that all frames have been consumed (since the released frame have been handed to the consumer)
            Assert.assertEquals(0, fmm.remaining());
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail(th.getMessage());
        } finally {
            Assert.assertNull(cause);
        }
    }

    @org.junit.Test
    public void testLargerThanBudgetRequests() {
        HyracksDataException hde = null;
        try {
            ConcurrentFramePool fmm = new ConcurrentFramePool("TestNode", DEFAULT_FRAME_SIZE * 16, DEFAULT_FRAME_SIZE);
            fmm.get(32 * DEFAULT_FRAME_SIZE);
        } catch (HyracksDataException e) {
            hde = e;
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail(th.getMessage());
        }
        Assert.assertNotNull(hde);
        Assert.assertNull(cause);
    }

    @org.junit.Test
    public void testLargerThanBudgetSubscribe() {
        HyracksDataException hde = null;
        try {
            ConcurrentFramePool fmm = new ConcurrentFramePool("TestNode", DEFAULT_FRAME_SIZE * 16, DEFAULT_FRAME_SIZE);
            ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FRAME_SIZE * 32);
            FrameAction frameAction = new FrameAction();
            frameAction.setFrame(buffer);
            fmm.subscribe(frameAction);
        } catch (HyracksDataException e) {
            hde = e;
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail(th.getMessage());
        }
        Assert.assertNotNull(hde);
        Assert.assertNull(cause);
    }

    @org.junit.Test
    public void testgetWhileSubscribersExist() {
        try {
            ActiveProperties afp = Mockito.mock(ActiveProperties.class);
            Mockito.when(afp.getMemoryComponentGlobalBudget()).thenReturn(FEED_MEM_BUDGET);
            ConcurrentFramePool fmm =
                    new ConcurrentFramePool("TestNode", afp.getMemoryComponentGlobalBudget(), DEFAULT_FRAME_SIZE);
            int i = 0;
            ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FRAME_SIZE);
            LinkedBlockingDeque<ByteBuffer> buffers = new LinkedBlockingDeque<>();
            FrameAction frameAction = new FrameAction();
            frameAction.setFrame(buffer);
            while (!fmm.subscribe(frameAction)) {
                buffers.put(frameAction.retrieve());
                i++;
            }
            // One subscriber.
            // Check that all frames have been consumed
            Assert.assertEquals(i, NUM_FRAMES);
            // Release a frame (That will be handed out to the subscriber)
            fmm.release(buffers.take());
            // Check that all frames have been consumed (since the released frame have been handed to the consumer)
            Assert.assertEquals(fmm.remaining(), 0);
            buffers.put(frameAction.retrieve());
            // Create another subscriber that takes frames of double the size
            ByteBuffer bufferTimes2 = ByteBuffer.allocate(DEFAULT_FRAME_SIZE * 2);
            LinkedBlockingDeque<ByteBuffer> buffersTimes2 = new LinkedBlockingDeque<>();
            FrameAction frameActionTimes2 = new FrameAction();
            frameActionTimes2.setFrame(bufferTimes2);
            Assert.assertEquals(true, fmm.subscribe(frameActionTimes2));
            // release a small one
            fmm.release(buffers.take());
            Assert.assertEquals(fmm.remaining(), 1);
            // Check that a small get fails
            Assert.assertEquals(null, fmm.get());
            // release another small one
            fmm.release(buffers.take());
            // Check that no small frames exists in the pool since subscriber request was satisfied
            Assert.assertEquals(fmm.remaining(), 0);
            buffersTimes2.add(frameActionTimes2.retrieve());
            fmm.release(buffers);
            fmm.release(bufferTimes2);
            Assert.assertEquals(fmm.remaining(), NUM_FRAMES);
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail(th.getMessage());
        } finally {
            Assert.assertNull(cause);
        }
    }

    /*
     * Runnables used for unit tests
     */
    private class FixedSizeAllocator implements Runnable {
        private final ConcurrentFramePool fmm;
        private int allocated = 0;

        public FixedSizeAllocator(ConcurrentFramePool fmm) {
            this.fmm = fmm;
        }

        public int getAllocated() {
            return allocated;
        }

        @Override
        public void run() {
            while (fmm.get() != null) {
                allocated++;
            }
        }
    }

    private class FixedSizeGoodAllocator implements Runnable {
        private final ConcurrentFramePool fmm;
        private final ArrayDeque<ByteBuffer> stack = new ArrayDeque<>();
        private final Random random = new Random();

        public FixedSizeGoodAllocator(ConcurrentFramePool fmm) {
            this.fmm = fmm;
        }

        public int getAllocated() {
            return stack.size();
        }

        @Override
        public void run() {
            while (true) {
                if (random.nextDouble() < RELEASE_PROBABILITY) {
                    if (!stack.isEmpty()) {
                        try {
                            fmm.release(stack.pop());
                        } catch (HyracksDataException e) {
                            e.printStackTrace();
                            cause = e;
                        }
                    }
                } else {
                    ByteBuffer buffer = fmm.get();
                    if (buffer == null) {
                        break;
                    } else {
                        stack.push(buffer);
                    }
                }
            }
        }
    }

    private class VarSizeGoodAllocator implements Runnable {
        private final ConcurrentFramePool fmm;
        private int allocated = 0;
        private int req = 0;
        private final Random random = new Random();
        private Throwable cause;
        private final ArrayDeque<ByteBuffer> stack = new ArrayDeque<>();

        public VarSizeGoodAllocator(ConcurrentFramePool fmm) {
            this.fmm = fmm;
        }

        public int getAllocated() {
            return allocated;
        }

        public Throwable cause() {
            return cause;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    if (random.nextDouble() < RELEASE_PROBABILITY) {
                        if (!stack.isEmpty()) {
                            ByteBuffer buffer = stack.pop();
                            allocated -= (buffer.capacity() / DEFAULT_FRAME_SIZE);
                            fmm.release(buffer);
                        }
                    } else {
                        req = random.nextInt(MAX_SIZE) + 1;
                        if (req == 1) {
                            ByteBuffer buffer = fmm.get();
                            if (buffer != null) {
                                stack.push(buffer);
                                allocated += 1;
                            } else {
                                break;
                            }
                        } else {
                            ByteBuffer buffer = fmm.get(req * DEFAULT_FRAME_SIZE);
                            if (buffer != null) {
                                stack.push(buffer);
                                allocated += req;
                            } else {
                                break;
                            }
                        }
                    }
                }
            } catch (Throwable th) {
                this.cause = th;
            }
        }
    }

    private class VarSizeAllocator implements Runnable {
        private final ConcurrentFramePool fmm;
        private int allocated = 0;
        private int req = 0;
        private final Random random = new Random();
        private Throwable cause;

        public VarSizeAllocator(ConcurrentFramePool fmm) {
            this.fmm = fmm;
        }

        public int getAllocated() {
            return allocated;
        }

        public int getLastReq() {
            return req;
        }

        public Throwable cause() {
            return cause;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    req = random.nextInt(MAX_SIZE) + 1;
                    if (req == 1) {
                        if (fmm.get() != null) {
                            allocated += 1;
                        } else {
                            break;
                        }
                    } else if (fmm.get(req * DEFAULT_FRAME_SIZE) != null) {
                        allocated += req;
                    } else {
                        break;
                    }
                }
            } catch (Throwable th) {
                this.cause = th;
            }
        }
    }
}
