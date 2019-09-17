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
package org.apache.hyracks.storage.am.lsm.btree;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.SingleThreadEventProcessor;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestContext;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestUtils;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.btree.impl.CountingIoOperationCallback;
import org.apache.hyracks.storage.am.lsm.btree.impl.CountingIoOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.btree.impl.ITestOpCallback;
import org.apache.hyracks.storage.am.lsm.btree.impl.NoOpTestCallback;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestContext;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.IIoOperationFailedCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AsynchronousScheduler;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LSMBTreeComponentLifecycleTest {
    private static final Logger LOGGER = LogManager.getLogger();
    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();
    private final OrderedIndexTestUtils testUtils = new OrderedIndexTestUtils();
    private final ISerializerDeserializer[] fieldSerdes =
            { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
    private final int numKeys = 1;
    private static final int numTuplesToInsert = 100;

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    private OrderedIndexTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            ILSMIOOperationScheduler scheduler, ILSMIOOperationCallbackFactory ioCallbackFactory) throws Exception {
        return LSMBTreeTestContext.create(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), fieldSerdes, numKeys,
                harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(), harness.getOperationTracker(),
                scheduler, ioCallbackFactory, harness.getPageWriteCallbackFactory(),
                harness.getMetadataPageManagerFactory(), false, true, false);
    }

    private OrderedIndexTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys)
            throws Exception {
        return createTestContext(fieldSerdes, numKeys, harness.getIOScheduler(),
                harness.getIOOperationCallbackFactory());
    }

    @Test
    public void testFlushUnallocatedIndex() throws Exception {
        OrderedIndexTestContext ctx = createTestContext(fieldSerdes, numKeys);
        ILSMIndex index = (ILSMIndex) ctx.getIndex();
        index.create();
        index.activate();
        Assert.assertEquals(getExpectedMemoryComponentIndex(0), index.getCurrentMemoryComponentIndex());
        flush(ctx);
        Assert.assertEquals(getExpectedMemoryComponentIndex(1), index.getCurrentMemoryComponentIndex());
        Assert.assertEquals(0, index.getDiskComponents().size());
        CountingIoOperationCallback ioCallback = (CountingIoOperationCallback) index.getIOOperationCallback();
        // assert equal before, after, after were called
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        // insert into the index
        testUtils.insertIntTuples(ctx, numTuplesToInsert, harness.getRandom());
        // flush
        flush(ctx);
        Assert.assertEquals(getExpectedMemoryComponentIndex(0), index.getCurrentMemoryComponentIndex());
        Assert.assertEquals(1, index.getDiskComponents().size());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        // insert more
        testUtils.insertIntTuples(ctx, numTuplesToInsert, harness.getRandom());
        // flush
        flush(ctx);
        Assert.assertEquals(getExpectedMemoryComponentIndex(1), index.getCurrentMemoryComponentIndex());
        Assert.assertEquals(2, index.getDiskComponents().size());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    @Test
    public void testFlushUnallocatedIndexStartFromSecondComponent() throws Exception {
        CountingIoOperationCallbackFactory.STARTING_INDEX = 1;
        try {
            testFlushUnallocatedIndex();
        } finally {
            CountingIoOperationCallbackFactory.STARTING_INDEX = 0;
        }
    }

    @Test
    public void testNormalFlushOperation() throws Exception {
        OrderedIndexTestContext ctx = createTestContext(fieldSerdes, numKeys);
        ILSMIndex index = (ILSMIndex) ctx.getIndex();
        index.create();
        index.activate();
        Assert.assertEquals(getExpectedMemoryComponentIndex(0), index.getCurrentMemoryComponentIndex());
        testUtils.insertIntTuples(ctx, numTuplesToInsert, harness.getRandom());
        flush(ctx);
        Assert.assertEquals(getExpectedMemoryComponentIndex(1), index.getCurrentMemoryComponentIndex());
        Assert.assertEquals(1, index.getDiskComponents().size());
        CountingIoOperationCallback ioCallback = (CountingIoOperationCallback) index.getIOOperationCallback();
        // assert equal before, after, after were called
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        // insert into the index
        testUtils.insertIntTuples(ctx, numTuplesToInsert, harness.getRandom());
        // flush
        flush(ctx);
        Assert.assertEquals(getExpectedMemoryComponentIndex(0), index.getCurrentMemoryComponentIndex());
        Assert.assertEquals(2, index.getDiskComponents().size());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        // insert more
        testUtils.insertIntTuples(ctx, numTuplesToInsert, harness.getRandom());
        // flush
        flush(ctx);
        Assert.assertEquals(getExpectedMemoryComponentIndex(1), index.getCurrentMemoryComponentIndex());
        Assert.assertEquals(3, index.getDiskComponents().size());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    @Test
    public void testNormalFlushOperationStartFromSecondComponent() throws Exception {
        CountingIoOperationCallbackFactory.STARTING_INDEX = 1;
        try {
            testNormalFlushOperation();
        } finally {
            CountingIoOperationCallbackFactory.STARTING_INDEX = 0;
        }
    }

    @Test
    public void testFlushUnModifiedComponent() throws Exception {
        OrderedIndexTestContext ctx = createTestContext(fieldSerdes, numKeys);
        ILSMIndex index = (ILSMIndex) ctx.getIndex();
        index.create();
        index.activate();
        Assert.assertEquals(getExpectedMemoryComponentIndex(0), index.getCurrentMemoryComponentIndex());
        testUtils.insertIntTuples(ctx, numTuplesToInsert, harness.getRandom());
        flush(ctx);
        Assert.assertEquals(getExpectedMemoryComponentIndex(1), index.getCurrentMemoryComponentIndex());
        Assert.assertEquals(1, index.getDiskComponents().size());
        CountingIoOperationCallback ioCallback = (CountingIoOperationCallback) index.getIOOperationCallback();
        // assert equal before, after, finalize were called
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        // flush, there was no insert before
        flush(ctx);
        Assert.assertEquals(getExpectedMemoryComponentIndex(0), index.getCurrentMemoryComponentIndex());
        Assert.assertEquals(1, index.getDiskComponents().size());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        // insert more
        testUtils.insertIntTuples(ctx, numTuplesToInsert, harness.getRandom());
        // flush
        flush(ctx);
        Assert.assertEquals(getExpectedMemoryComponentIndex(1), index.getCurrentMemoryComponentIndex());
        Assert.assertEquals(2, index.getDiskComponents().size());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        // insert more
        testUtils.insertIntTuples(ctx, numTuplesToInsert, harness.getRandom());
        // flush
        flush(ctx);
        Assert.assertEquals(getExpectedMemoryComponentIndex(0), index.getCurrentMemoryComponentIndex());
        Assert.assertEquals(3, index.getDiskComponents().size());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    @Test
    public void testFlushUnModifiedComponentStartFromSecondComponent() throws Exception {
        CountingIoOperationCallbackFactory.STARTING_INDEX = 1;
        try {
            testFlushUnModifiedComponent();
        } finally {
            CountingIoOperationCallbackFactory.STARTING_INDEX = 0;
        }
    }

    public int getExpectedMemoryComponentIndex(int expectedIndex) {
        return (CountingIoOperationCallbackFactory.STARTING_INDEX + expectedIndex) % 2;
    }

    @Test
    public void testScheduleMoreFlushesThanComponents() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        final Semaphore flushSemaphore = new Semaphore(0);
        OrderedIndexTestContext ctx = createTestContext(fieldSerdes, numKeys, new AsynchronousScheduler(
                r -> new Thread(r, "LsmIoThread-" + counter.getAndIncrement()), new IIoOperationFailedCallback() {
                    @Override
                    public void schedulerFailed(ILSMIOOperationScheduler scheduler, Throwable failure) {
                        LOGGER.log(Level.ERROR, "Scheduler failed", failure);
                    }

                    @Override
                    public void operationFailed(ILSMIOOperation operation, Throwable failure) {
                        LOGGER.log(Level.ERROR, "Operation {} failed", operation, failure);
                    }
                }), new EncapsulatingIoCallbackFactory(harness.getIOOperationCallbackFactory(), NoOpTestCallback.get(),
                        NoOpTestCallback.get(), new ITestOpCallback<ILSMIOOperation>() {
                            @Override
                            public void before(ILSMIOOperation t) throws HyracksDataException {
                                try {
                                    flushSemaphore.acquire();
                                } catch (InterruptedException e) {
                                    throw new IllegalStateException(e);
                                }
                            }

                            @Override
                            public void after(ILSMIOOperation t) throws HyracksDataException {
                            }
                        }, NoOpTestCallback.get(), NoOpTestCallback.get()));
        ILSMIndex index = (ILSMIndex) ctx.getIndex();
        index.create();
        index.activate();
        Assert.assertEquals(getExpectedMemoryComponentIndex(0), index.getCurrentMemoryComponentIndex());
        int numMemoryComponents = index.getNumberOfAllMemoryComponents();
        // create a flusher that will schedule 13 flushes.
        // wait for all flushes to be scheduled.
        // create an inserter that will insert some records.
        // one by one allow flushes until one flush remains, and ensure no record went in.
        // allow the last flush, then wait for the inserts to succeed, and ensure they went to
        // the expected memory component
        final int numFlushes = 13;
        User firstUser = new User("FirstUser");
        User secondUser = new User("SecondUser");
        Request flushRequest = new Request(Request.Statement.FLUSH, ctx, numFlushes);
        firstUser.add(flushRequest);
        firstUser.step();
        // wait until all flushes have been scheduled.. Not yet performed
        flushRequest.await(1);
        // create an inserter and allow it to go all the way
        Request insertRequest = new Request(Request.Statement.INSERT, ctx, 1);
        secondUser.add(insertRequest);
        secondUser.step();
        secondUser.step();
        ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        ILSMIndexOperationContext opCtx = accessor.getOpContext();
        assertCorrectSearchComponents(opCtx, index, 0);
        // Allow one flush at a time and ensure that inserter didn't succeed
        for (int i = 0; i < numFlushes - index.getNumberOfAllMemoryComponents(); i++) {
            flushSemaphore.release();
            firstUser.step();
            flushRequest.await(2 + i);
            Assert.assertEquals(0, insertRequest.getSteps());
            // also ensure that you get the correct components when searching
            assertCorrectSearchComponents(opCtx, index, i + 1);
        }
        flushSemaphore.release();
        firstUser.step();
        // wait for the insert to complete
        insertRequest.await();
        // Allow last flush to proceed
        flushSemaphore.release();
        firstUser.step();
        firstUser.step();
        flushRequest.await();
        firstUser.stop();
        secondUser.stop();
        int expectedMemoryComponent = numFlushes % numMemoryComponents;
        Assert.assertEquals(getExpectedMemoryComponentIndex(expectedMemoryComponent),
                index.getCurrentMemoryComponentIndex());
        Assert.assertEquals(0, index.getDiskComponents().size());
        EncapsulatingIoCallback encapsulating = (EncapsulatingIoCallback) index.getIOOperationCallback();
        CountingIoOperationCallback ioCallback = (CountingIoOperationCallback) encapsulating.getEncapsulated();
        // assert equal before, after, finalize were called
        Assert.assertEquals(numFlushes, ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        // flush, there was no insert before
        flushSemaphore.release();
        flush(ctx);
        Assert.assertEquals(getExpectedMemoryComponentIndex((expectedMemoryComponent + 1) % numMemoryComponents),
                index.getCurrentMemoryComponentIndex());
        Assert.assertEquals(1, index.getDiskComponents().size());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        // deactivate will cause a flush
        flushSemaphore.release();
        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    private void assertCorrectSearchComponents(ILSMIndexOperationContext opCtx, ILSMIndex index,
            int numSuccesfullyCompletedFlushes) throws HyracksDataException {
        opCtx.reset();
        opCtx.setOperation(IndexOperation.SEARCH);
        index.getOperationalComponents(opCtx);
        List<ILSMMemoryComponent> memComponents = index.getMemoryComponents();
        int first = numSuccesfullyCompletedFlushes % memComponents.size();
        Assert.assertEquals(memComponents.get(first), getFirstMemoryComponent(opCtx));
    }

    private ILSMComponent getFirstMemoryComponent(ILSMIndexOperationContext opCtx) {
        List<ILSMComponent> components = opCtx.getComponentHolder();
        // backward
        for (int i = components.size() - 1; i >= 0; i--) {
            ILSMComponent next = components.get(i);
            if (next.getType() == LSMComponentType.MEMORY) {
                return next;
            }
        }
        return null;
    }

    private void flush(OrderedIndexTestContext ctx) throws HyracksDataException, InterruptedException {
        ILSMIOOperation flush = scheduleFlush(ctx);
        flush.sync();
        if (flush.getStatus() == LSMIOOperationStatus.FAILURE) {
            throw HyracksDataException.create(flush.getFailure());
        }
    }

    private ILSMIOOperation scheduleFlush(OrderedIndexTestContext ctx)
            throws HyracksDataException, InterruptedException {
        ILSMIndexAccessor accessor =
                (ILSMIndexAccessor) ctx.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        return accessor.scheduleFlush();
    }

    private static class Request {
        private enum Statement {
            FLUSH,
            INSERT
        }

        private final Statement statement;
        private final OrderedIndexTestContext ctx;
        private final int repeats;
        private boolean done = false;
        private int step = 0;

        public Request(Statement statement, OrderedIndexTestContext ctx, int repeats) {
            this.statement = statement;
            this.ctx = ctx;
            this.repeats = repeats;
        }

        Statement statement() {
            return statement;
        }

        synchronized void complete() {
            done = true;
            notifyAll();
        }

        synchronized void await() throws InterruptedException {
            while (!done) {
                wait();
            }
        }

        synchronized void step() {
            step++;
            notifyAll();
        }

        synchronized int getSteps() {
            return step;
        }

        synchronized void await(int step) throws InterruptedException {
            while (this.step < step) {
                wait();
            }
        }
    }

    private class User extends SingleThreadEventProcessor<Request> {

        private Semaphore step = new Semaphore(0);

        public User(String username) {
            super(username);
        }

        public void step() {
            step.release();
        }

        @Override
        protected void handle(Request req) throws Exception {
            try {
                step.acquire();
                switch (req.statement()) {
                    case FLUSH:
                        List<ILSMIOOperation> flushes = new ArrayList<>(req.repeats);
                        for (int i = 0; i < req.repeats; i++) {
                            flushes.add(scheduleFlush(req.ctx));
                        }
                        req.step();
                        for (ILSMIOOperation op : flushes) {
                            step.acquire();
                            op.sync();
                            if (op.getStatus() == LSMIOOperationStatus.FAILURE) {
                                throw HyracksDataException.create(op.getFailure());
                            }
                            req.step(); // report after completion of each flush
                        }
                        break;
                    case INSERT:
                        testUtils.insertIntTuples(req.ctx, numTuplesToInsert, harness.getRandom());
                        break;
                    default:
                        break;
                }
                req.step();
                step.acquire();
            } finally {
                req.step();
                req.complete();
            }
        }
    }
}
