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
package edu.uci.ics.hyracks.storage.am.btree;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;

public abstract class AbstractSearchOperationCallbackTest extends AbstractOperationCallbackTest {
    private static final int NUM_TASKS = 2;

    protected final Lock lock;
    protected final Condition condition;

    protected ExecutorService executor;
    protected boolean insertTaskStarted;

    public AbstractSearchOperationCallbackTest() {
        this.lock = new ReentrantLock(true);
        this.condition = lock.newCondition();
        this.insertTaskStarted = false;
    }

    @Before
    public void setup() throws Exception {
        executor = Executors.newFixedThreadPool(NUM_TASKS);
        super.setup();
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdown();
        super.tearDown();
    }

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

        private boolean blockOnHigh;
        private int blockingValue;
        private int expectedAfterBlock;

        public SearchTask() throws HyracksDataException {
            this.cb = new SynchronizingSearchOperationCallback();
            this.accessor = index.createAccessor(NoOpOperationCallback.INSTANCE, cb);
            this.cursor = accessor.createSearchCursor();
            this.predicate = new RangePredicate();
            this.builder = new ArrayTupleBuilder(NUM_KEY_FIELDS);
            this.tuple = new ArrayTupleReference();

            this.blockOnHigh = false;
            this.blockingValue = -1;
            this.expectedAfterBlock = -1;
        }

        @Override
        public Boolean call() throws Exception {
            lock.lock();
            try {
                if (!insertTaskStarted) {
                    condition.await();
                }

                // begin a search on [101, +inf), blocking on 101
                TupleUtils.createIntegerTuple(builder, tuple, 101);
                predicate.setLowKey(tuple, true);
                predicate.setHighKey(null, true);
                accessor.search(cursor, predicate);
                consumeIntTupleRange(101, 101, true, 101);

                // consume tuples [102, 152], blocking on 151
                consumeIntTupleRange(102, 151, true, 152);

                // consume tuples [153, 300]
                consumeIntTupleRange(153, 300, false, -1);

                cursor.close();
            } finally {
                lock.unlock();
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
                    this.blockingValue = end;
                    this.expectedAfterBlock = expectedAfterBlock;
                }
                TupleUtils.createIntegerTuple(builder, tuple, i);
                if (!cursor.hasNext()) {
                    Assert.fail("Failed to consume entire tuple range since cursor is exhausted.");
                }
                cursor.next();

                if (this.blockOnHigh) {
                    TupleUtils.createIntegerTuple(builder, tuple, expectedAfterBlock);
                }
                Assert.assertEquals(0, cmp.compare(tuple, cursor.getTuple()));
            }
        }

        private class SynchronizingSearchOperationCallback implements ISearchOperationCallback {

            @Override
            public boolean proceed(ITupleReference tuple) {
                Assert.assertEquals(0, cmp.compare(SearchTask.this.tuple, tuple));
                return false;
            }

            @Override
            public void reconcile(ITupleReference tuple) {
                Assert.assertEquals(0, cmp.compare(SearchTask.this.tuple, tuple));
                if (blockOnHigh) {
                    try {
                        TupleUtils.createIntegerTuple(builder, SearchTask.this.tuple, expectedAfterBlock);
                    } catch (HyracksDataException e) {
                        e.printStackTrace();
                    }
                    condition.signal();
                    condition.awaitUninterruptibly();
                    blockOnHigh = false;
                }
            }

            @Override
            public void cancel(ITupleReference tuple) {
                try {
                    TupleUtils.createIntegerTuple(builder, SearchTask.this.tuple, blockingValue);
                    Assert.assertEquals(0, cmp.compare(tuple, SearchTask.this.tuple));
                    TupleUtils.createIntegerTuple(builder, SearchTask.this.tuple, expectedAfterBlock);
                } catch (HyracksDataException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void complete(ITupleReference tuple) throws HyracksDataException {

            }

        }
    }

    private class InsertionTask implements Callable<Boolean> {
        private final IIndexAccessor accessor;
        private final ArrayTupleBuilder builder;
        private final ArrayTupleReference tuple;

        public InsertionTask() throws HyracksDataException {
            this.accessor = index.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            this.builder = new ArrayTupleBuilder(NUM_KEY_FIELDS);
            this.tuple = new ArrayTupleReference();
        }

        @Override
        public Boolean call() throws Exception {
            lock.lock();
            try {
                insertTaskStarted = true;

                // insert tuples [101, 200]
                insertIntTupleRange(101, 200);
                condition.signal();
                condition.await();

                // insert tuples [1, 100]
                insertIntTupleRange(1, 100);
                condition.signal();
                condition.await();

                // insert tuples [201, 300] and delete tuple 151
                insertIntTupleRange(201, 300);
                TupleUtils.createIntegerTuple(builder, tuple, 151);
                accessor.delete(tuple);
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

    }

}
