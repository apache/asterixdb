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

import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.AbstractSearchOperationCallbackTest;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.btree.utils.LSMBTreeUtil;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpOperationTrackerFactory;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.util.trace.ITracer;
import org.junit.Assert;
import org.junit.Test;

public class LSMBTreeSearchOperationCallbackTest extends AbstractSearchOperationCallbackTest {
    private final LSMBTreeTestHarness harness;
    private final HashSet<Integer> deleteSet;

    public LSMBTreeSearchOperationCallbackTest() {
        harness = new LSMBTreeTestHarness();
        deleteSet = new HashSet<>();
    }

    @Override
    protected void createIndexInstance() throws Exception {
        index = LSMBTreeUtil.createLSMTree(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), SerdeUtils.serdesToTypeTraits(keySerdes),
                SerdeUtils.serdesToComparatorFactories(keySerdes, keySerdes.length), bloomFilterKeyFields,
                harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(),
                NoOpOperationTrackerFactory.INSTANCE.getOperationTracker(null, null), harness.getIOScheduler(),
                harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(), true, null, null, null,
                null, true, harness.getMetadataPageManagerFactory(), false, ITracer.NONE,
                NoOpCompressorDecompressorFactory.INSTANCE);
    }

    @Override
    public void setup() throws Exception {
        harness.setUp();
        super.setup();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        harness.tearDown();
    }

    @Override
    @Test
    public void searchCallbackTest() throws Exception {
        Future<Boolean> insertFuture = executor.submit(new InsertionTask());
        Future<Boolean> searchFuture = executor.submit(new SearchTask());
        Assert.assertTrue(searchFuture.get());
        Assert.assertTrue(insertFuture.get());
    }

    private class SearchTask implements Callable<Boolean> {
        private final ISearchOperationCallback cb;
        private final IIndexAccessor accessor;
        private final IIndexCursor cursor;
        private final RangePredicate predicate;
        private final ArrayTupleBuilder builder;
        private final ArrayTupleReference tuple;
        private final ArrayTupleBuilder expectedTupleToBeLockedBuilder;
        private final ArrayTupleReference expectedTupleToBeLocked;
        private final ArrayTupleBuilder expectedTupleToBeCanceledBuilder;
        private final ArrayTupleReference expectedTupleToBeCanceled;

        private boolean blockOnHigh;
        private int expectedAfterBlock;
        private int expectedTupleToBeLockedValue;

        public SearchTask() throws HyracksDataException {
            this.cb = new SynchronizingSearchOperationCallback();
            IndexAccessParameters actx = new IndexAccessParameters(NoOpOperationCallback.INSTANCE, cb);
            this.accessor = index.createAccessor(actx);
            this.cursor = accessor.createSearchCursor(false);
            this.predicate = new RangePredicate();
            this.builder = new ArrayTupleBuilder(NUM_KEY_FIELDS);
            this.tuple = new ArrayTupleReference();
            this.expectedTupleToBeLockedBuilder = new ArrayTupleBuilder(NUM_KEY_FIELDS);
            this.expectedTupleToBeLocked = new ArrayTupleReference();
            this.expectedTupleToBeCanceledBuilder = new ArrayTupleBuilder(NUM_KEY_FIELDS);
            this.expectedTupleToBeCanceled = new ArrayTupleReference();

            this.blockOnHigh = false;
            this.expectedAfterBlock = -1;
            this.expectedTupleToBeLockedValue = -1;
        }

        @Override
        public Boolean call() throws Exception {
            lock.lock();
            try {
                if (!insertTaskStarted) {
                    condition.await();
                }
                // begin a search on [50, +inf), blocking on 75
                TupleUtils.createIntegerTuple(builder, tuple, 50);
                predicate.setLowKey(tuple, true);
                predicate.setHighKey(null, true);
                accessor.search(cursor, predicate);
                try {
                    expectedTupleToBeLockedValue = 50;
                    TupleUtils.createIntegerTuple(builder, expectedTupleToBeLocked, expectedTupleToBeLockedValue);
                    consumeIntTupleRange(50, 75, true, 76);

                    // consume tuples [77, 150], blocking on 151
                    consumeIntTupleRange(77, 150, true, 150);

                    // consume tuples [152, 300]
                    consumeIntTupleRange(152, 300, false, -1);
                } finally {
                    cursor.close();
                }
            } finally {
                try {
                    cursor.destroy();
                } finally {
                    lock.unlock();
                }
            }

            return true;
        }

        private void consumeIntTupleRange(int begin, int end, boolean blockOnHigh, int expectedAfterBlock)
                throws Exception {
            if (end < begin) {
                throw new IllegalArgumentException("Invalid range: [" + begin + ", " + end + "]");
            }

            for (int i = begin; i <= end; i++) {
                if (blockOnHigh == true && i == end) {
                    this.blockOnHigh = true;
                    this.expectedAfterBlock = expectedAfterBlock;
                }
                TupleUtils.createIntegerTuple(builder, tuple, i);
                if (!cursor.hasNext()) {
                    Assert.fail("Failed to consume entire tuple range since cursor is exhausted.");
                }
                cursor.next();
                Assert.assertEquals(0, cmp.compare(tuple, cursor.getTuple()));
            }
        }

        private class SynchronizingSearchOperationCallback implements ISearchOperationCallback {

            @Override
            public boolean proceed(ITupleReference tuple) throws HyracksDataException {
                Assert.assertEquals(0, cmp.compare(SearchTask.this.expectedTupleToBeLocked, tuple));
                return false;
            }

            @Override
            public void reconcile(ITupleReference tuple) throws HyracksDataException {
                Assert.assertEquals(0, cmp.compare(SearchTask.this.expectedTupleToBeLocked, tuple));
                if (blockOnHigh) {
                    TupleUtils.createIntegerTuple(builder, SearchTask.this.tuple, expectedAfterBlock);
                    condition.signal();
                    condition.awaitUninterruptibly();
                    blockOnHigh = false;
                }
            }

            @Override
            public void cancel(ITupleReference tuple) throws HyracksDataException {
                boolean found = false;
                for (int i : deleteSet) {
                    TupleUtils.createIntegerTuple(expectedTupleToBeCanceledBuilder, expectedTupleToBeCanceled, i);
                    if (cmp.compare(SearchTask.this.expectedTupleToBeCanceled, tuple) == 0) {
                        found = true;
                        break;
                    }
                }
                Assert.assertTrue(found);
            }

            @Override
            public void complete(ITupleReference tuple) throws HyracksDataException {
                Assert.assertEquals(0, cmp.compare(SearchTask.this.expectedTupleToBeLocked, tuple));
                expectedTupleToBeLockedValue++;
                TupleUtils.createIntegerTuple(expectedTupleToBeLockedBuilder, expectedTupleToBeLocked,
                        expectedTupleToBeLockedValue);
            }

            @Override
            public void before(ITupleReference tuple) throws HyracksDataException {
            }
        }
    }

    private class InsertionTask implements Callable<Boolean> {
        private final IIndexAccessor accessor;
        private final ArrayTupleBuilder builder;
        private final ArrayTupleReference tuple;

        public InsertionTask() throws HyracksDataException {
            this.accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            this.builder = new ArrayTupleBuilder(NUM_KEY_FIELDS);
            this.tuple = new ArrayTupleReference();
        }

        @Override
        public Boolean call() throws Exception {
            lock.lock();
            try {
                insertTaskStarted = true;

                // bulkload [101, 150] and then insert [151, 200] and make sure it reaches disk, thus we will have two separate disk components
                // insert [50, 100] & [301, 350] to the in-memory component
                // delete tuple 151
                bulkloadIntTupleRange(101, 150);
                insertIntTupleRange(151, 200);
                // Deactivate and the re-activate the index to force it flush its in memory component
                index.deactivate();
                index.activate();

                insertIntTupleRange(50, 100);
                insertIntTupleRange(301, 350);
                int tupleTobeDeletedValue = 151;
                deleteSet.add(tupleTobeDeletedValue);
                TupleUtils.createIntegerTuple(builder, tuple, tupleTobeDeletedValue);
                accessor.delete(tuple);
                condition.signal();
                condition.await();

                // delete tuple 75
                tupleTobeDeletedValue = 75;
                deleteSet.add(tupleTobeDeletedValue);
                TupleUtils.createIntegerTuple(builder, tuple, tupleTobeDeletedValue);
                accessor.delete(tuple);
                condition.signal();
                condition.await();

                // insert tuples [201, 300] and delete tuple 151
                insertIntTupleRange(201, 300);
                condition.signal();
            } finally {
                lock.unlock();
            }

            return true;
        }

        private void insertIntTupleRange(int begin, int end) throws Exception {
            if (end < begin) {
                throw new IllegalArgumentException("Invalid range: [" + begin + ", " + end + "]");
            }

            for (int i = begin; i <= end; i++) {
                TupleUtils.createIntegerTuple(builder, tuple, i);
                accessor.insert(tuple);
            }
        }

        private void bulkloadIntTupleRange(int begin, int end) throws Exception {
            if (end < begin) {
                throw new IllegalArgumentException("Invalid range: [" + begin + ", " + end + "]");
            }

            IIndexBulkLoader bulkloader = index.createBulkLoader(1.0f, false, end - begin, true,
                    harness.getPageWriteCallbackFactory().createPageWriteCallback());
            for (int i = begin; i <= end; i++) {
                TupleUtils.createIntegerTuple(builder, tuple, i);
                bulkloader.add(tuple);
            }
            bulkloader.end();
        }

    }
}
