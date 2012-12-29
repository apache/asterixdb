package edu.uci.ics.hyracks.storage.am.lsm.btree;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.AbstractSearchOperationCallbackTest;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpOperationTrackerFactory;

public class LSMBTreeSearchOperationCallbackTest extends AbstractSearchOperationCallbackTest {
    private final LSMBTreeTestHarness harness;

    public LSMBTreeSearchOperationCallbackTest() {
        harness = new LSMBTreeTestHarness();
    }

    @Override
    protected void createIndexInstance() throws Exception {
        index = LSMBTreeUtils.createLSMTree(harness.getMemBufferCache(), harness.getMemFreePageManager(),
                harness.getIOManager(), harness.getFileReference(), harness.getDiskBufferCache(),
                harness.getDiskFileMapProvider(), SerdeUtils.serdesToTypeTraits(keySerdes),
                SerdeUtils.serdesToComparatorFactories(keySerdes, keySerdes.length), harness.getMergePolicy(),
                NoOpOperationTrackerFactory.INSTANCE, harness.getIOScheduler());
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
        private int expectedAfterBlock;

        public SearchTask() {
            this.cb = new SynchronizingSearchOperationCallback();
            this.accessor = index.createAccessor(NoOpOperationCallback.INSTANCE, cb);
            this.cursor = accessor.createSearchCursor();
            this.predicate = new RangePredicate();
            this.builder = new ArrayTupleBuilder(NUM_KEY_FIELDS);
            this.tuple = new ArrayTupleReference();

            this.blockOnHigh = false;
            this.expectedAfterBlock = -1;
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
                consumeIntTupleRange(50, 75, true, 76);

                // consume tuples [77, 150], blocking on 151
                consumeIntTupleRange(77, 150, true, 150);

                // consume tuples [152, 300]
                consumeIntTupleRange(152, 300, false, -1);

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
                // Do nothing.
            }

        }
    }

    private class InsertionTask implements Callable<Boolean> {
        private final IIndexAccessor accessor;
        private final ArrayTupleBuilder builder;
        private final ArrayTupleReference tuple;

        public InsertionTask() {
            this.accessor = index.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            this.builder = new ArrayTupleBuilder(NUM_KEY_FIELDS);
            this.tuple = new ArrayTupleReference();
        }

        @Override
        public Boolean call() throws Exception {
            lock.lock();
            try {
                insertTaskStarted = true;

                // bulkload [101, 150] & [151, 200] as two separate disk components 
                // insert [50, 100] & [301, 350] to the in-memory component
                // delete tuple 151
                bulkloadIntTupleRange(101, 150);
                bulkloadIntTupleRange(151, 200);
                insertIntTupleRange(50, 100);
                insertIntTupleRange(301, 350);
                TupleUtils.createIntegerTuple(builder, tuple, 151);
                accessor.delete(tuple);
                condition.signal();
                condition.await();

                // delete tuple 75
                TupleUtils.createIntegerTuple(builder, tuple, 75);
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

            IIndexBulkLoader bulkloader = index.createBulkLoader(1.0f, false);
            for (int i = begin; i <= end; i++) {
                TupleUtils.createIntegerTuple(builder, tuple, i);
                bulkloader.add(tuple);
            }
            bulkloader.end();
        }

    }
}
