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
package org.apache.asterix.test.dataflow;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.data.gen.RecordTupleGenerator;
import org.apache.asterix.app.data.gen.RecordTupleGenerator.GenerationFunction;
import org.apache.asterix.app.data.gen.TestTupleCounterFrameWriter;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.dataflow.LSMInsertDeleteOperatorNodePushable;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.test.CountAnswer;
import org.apache.hyracks.api.test.FrameWriterTestUtils;
import org.apache.hyracks.api.test.FrameWriterTestUtils.FrameWriterOperation;
import org.apache.hyracks.storage.am.lsm.btree.impl.AllowTestOpCallback;
import org.apache.hyracks.storage.am.lsm.btree.impl.ITestOpCallback;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.junit.Assert;

public class StorageTestUtils {

    public static final IAType[] KEY_TYPES = { BuiltinType.AINT32 };
    public static final ARecordType RECORD_TYPE = new ARecordType("TestRecordType", new String[] { "key", "value" },
            new IAType[] { BuiltinType.AINT32, BuiltinType.AINT64 }, false);
    public static final GenerationFunction[] RECORD_GEN_FUNCTION =
            { GenerationFunction.DETERMINISTIC, GenerationFunction.DETERMINISTIC };
    public static final boolean[] UNIQUE_RECORD_FIELDS = { true, false };
    public static final ARecordType META_TYPE = null;
    public static final GenerationFunction[] META_GEN_FUNCTION = null;
    public static final boolean[] UNIQUE_META_FIELDS = null;
    public static final int[] KEY_INDEXES = { 0 };
    public static final int[] KEY_INDICATORS = { Index.RECORD_INDICATOR };
    public static final List<Integer> KEY_INDICATORS_LIST = Arrays.asList(new Integer[] { Index.RECORD_INDICATOR });
    public static final int TOTAL_NUM_OF_RECORDS = 10000;
    public static final int RECORDS_PER_COMPONENT = 1000;
    public static final int DATASET_ID = 101;
    public static final String DATAVERSE_NAME = "TestDV";
    public static final String DATASET_NAME = "TestDS";
    public static final String DATA_TYPE_NAME = "DUMMY";
    public static final String NODE_GROUP_NAME = "DEFAULT";
    public static final StorageComponentProvider STORAGE_MANAGER = new StorageComponentProvider();
    public static final List<List<String>> PARTITIONING_KEYS =
            new ArrayList<>(Collections.singletonList(Collections.singletonList(RECORD_TYPE.getFieldNames()[0])));
    public static final TestDataset DATASET =
            new TestDataset(DATAVERSE_NAME, DATASET_NAME, DATAVERSE_NAME, DATA_TYPE_NAME, NODE_GROUP_NAME,
                    NoMergePolicyFactory.NAME, null, new InternalDatasetDetails(null, PartitioningStrategy.HASH,
                            PARTITIONING_KEYS, null, null, null, false, null),
                    null, DatasetType.INTERNAL, DATASET_ID, 0);

    private StorageTestUtils() {
    }

    public static void allowAllOps(TestLsmBtree lsmBtree) {
        lsmBtree.clearModifyCallbacks();
        lsmBtree.clearFlushCallbacks();
        lsmBtree.clearSearchCallbacks();
        lsmBtree.clearMergeCallbacks();

        lsmBtree.addModifyCallback(AllowTestOpCallback.INSTANCE);
        lsmBtree.addFlushCallback(AllowTestOpCallback.INSTANCE);
        lsmBtree.addSearchCallback(AllowTestOpCallback.INSTANCE);
        lsmBtree.addMergeCallback(AllowTestOpCallback.INSTANCE);
    }

    public static PrimaryIndexInfo createPrimaryIndex(TestNodeController nc, int partition)
            throws HyracksDataException, RemoteException, ACIDException, AlgebricksException {
        return nc.createPrimaryIndex(DATASET, KEY_TYPES, RECORD_TYPE, META_TYPE, null, STORAGE_MANAGER, KEY_INDEXES,
                KEY_INDICATORS_LIST, partition);
    }

    public static PrimaryIndexInfo createPrimaryIndex(TestNodeController nc, Dataset dataset, int partition)
            throws HyracksDataException, RemoteException, ACIDException, AlgebricksException {
        return nc.createPrimaryIndex(dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null, STORAGE_MANAGER, KEY_INDEXES,
                KEY_INDICATORS_LIST, partition);
    }

    public static LSMPrimaryInsertOperatorNodePushable getInsertPipeline(TestNodeController nc, IHyracksTaskContext ctx)
            throws HyracksDataException, RemoteException, ACIDException, AlgebricksException {
        return getInsertPipeline(nc, ctx, null);
    }

    public static LSMPrimaryInsertOperatorNodePushable getInsertPipeline(TestNodeController nc, IHyracksTaskContext ctx,
            Index secondaryIndex) throws HyracksDataException, RemoteException, ACIDException, AlgebricksException {
        return nc.getInsertPipeline(ctx, DATASET, KEY_TYPES, RECORD_TYPE, META_TYPE, null, KEY_INDEXES,
                KEY_INDICATORS_LIST, STORAGE_MANAGER, secondaryIndex, null).getLeft();
    }

    public static LSMPrimaryInsertOperatorNodePushable getInsertPipeline(TestNodeController nc, IHyracksTaskContext ctx,
            Index secondaryIndex, Index primaryKeyIndex)
            throws HyracksDataException, RemoteException, ACIDException, AlgebricksException {
        return nc.getInsertPipeline(ctx, DATASET, KEY_TYPES, RECORD_TYPE, META_TYPE, null, KEY_INDEXES,
                KEY_INDICATORS_LIST, STORAGE_MANAGER, secondaryIndex, primaryKeyIndex).getLeft();
    }

    public static LSMPrimaryInsertOperatorNodePushable getInsertPipeline(TestNodeController nc, IHyracksTaskContext ctx,
            Dataset dataset, Index secondaryIndex)
            throws HyracksDataException, RemoteException, ACIDException, AlgebricksException {
        return nc.getInsertPipeline(ctx, dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null, KEY_INDEXES,
                KEY_INDICATORS_LIST, STORAGE_MANAGER, secondaryIndex, null).getLeft();
    }

    public static LSMInsertDeleteOperatorNodePushable getDeletePipeline(TestNodeController nc, IHyracksTaskContext ctx,
            Index secondaryIndex) throws HyracksDataException, RemoteException, ACIDException, AlgebricksException {
        return nc.getDeletePipeline(ctx, DATASET, KEY_TYPES, RECORD_TYPE, META_TYPE, null, KEY_INDEXES,
                KEY_INDICATORS_LIST, STORAGE_MANAGER, secondaryIndex).getLeft();
    }

    public static LSMInsertDeleteOperatorNodePushable getDeletePipeline(TestNodeController nc, IHyracksTaskContext ctx,
            Dataset dataset, Index secondaryIndex)
            throws HyracksDataException, RemoteException, ACIDException, AlgebricksException {
        return nc.getDeletePipeline(ctx, dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null, KEY_INDEXES,
                KEY_INDICATORS_LIST, STORAGE_MANAGER, secondaryIndex).getLeft();
    }

    public static RecordTupleGenerator getTupleGenerator() {
        return new RecordTupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS, RECORD_GEN_FUNCTION,
                UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
    }

    public static void searchAndAssertCount(TestNodeController nc, int partition, int numOfRecords)
            throws HyracksDataException, AlgebricksException {
        searchAndAssertCount(nc, partition, DATASET, STORAGE_MANAGER, numOfRecords);
    }

    public static void searchAndAssertCount(TestNodeController nc, Dataset dataset, int partition, int numOfRecords)
            throws HyracksDataException, AlgebricksException {
        searchAndAssertCount(nc, partition, dataset, STORAGE_MANAGER, numOfRecords);
    }

    public static void searchAndAssertCount(TestNodeController nc, int partition, Dataset dataset,
            StorageComponentProvider storageManager, int numOfRecords)
            throws HyracksDataException, AlgebricksException {
        JobId jobId = nc.newJobId();
        IHyracksTaskContext ctx = nc.createTestContext(jobId, partition, false);
        TestTupleCounterFrameWriter countOp = create(nc.getSearchOutputDesc(KEY_TYPES, RECORD_TYPE, META_TYPE),
                Collections.emptyList(), Collections.emptyList(), false);
        IPushRuntime emptyTupleOp = nc.getFullScanPipeline(countOp, ctx, dataset, KEY_TYPES, RECORD_TYPE, META_TYPE,
                new NoMergePolicyFactory(), null, null, KEY_INDEXES, KEY_INDICATORS_LIST, storageManager);
        emptyTupleOp.open();
        emptyTupleOp.close();
        Assert.assertEquals(numOfRecords, countOp.getCount());
    }

    public static TestTupleCounterFrameWriter create(RecordDescriptor recordDescriptor,
            Collection<FrameWriterOperation> exceptionThrowingOperations,
            Collection<FrameWriterOperation> errorThrowingOperations, boolean deepCopyInputFrames) {
        CountAnswer openAnswer = FrameWriterTestUtils.createAnswer(FrameWriterOperation.Open,
                exceptionThrowingOperations, errorThrowingOperations);
        CountAnswer nextAnswer = FrameWriterTestUtils.createAnswer(FrameWriterOperation.NextFrame,
                exceptionThrowingOperations, errorThrowingOperations);
        CountAnswer flushAnswer = FrameWriterTestUtils.createAnswer(FrameWriterOperation.Flush,
                exceptionThrowingOperations, errorThrowingOperations);
        CountAnswer failAnswer = FrameWriterTestUtils.createAnswer(FrameWriterOperation.Fail,
                exceptionThrowingOperations, errorThrowingOperations);
        CountAnswer closeAnswer = FrameWriterTestUtils.createAnswer(FrameWriterOperation.Close,
                exceptionThrowingOperations, errorThrowingOperations);
        return new TestTupleCounterFrameWriter(recordDescriptor, openAnswer, nextAnswer, flushAnswer, failAnswer,
                closeAnswer, deepCopyInputFrames);
    }

    public static void flushPartition(IDatasetLifecycleManager dslLifecycleMgr, TestLsmBtree lsmBtree, boolean async)
            throws Exception {
        flushPartition(dslLifecycleMgr, lsmBtree, DATASET, async);
    }

    public static void flushPartition(IDatasetLifecycleManager dslLifecycleMgr, Dataset dataset, TestLsmBtree lsmBtree,
            boolean async) throws Exception {
        flushPartition(dslLifecycleMgr, lsmBtree, dataset, async);
    }

    public static void flushPartition(IDatasetLifecycleManager dslLifecycleMgr, TestLsmBtree lsmBtree, Dataset dataset,
            boolean async) throws Exception {
        waitForOperations(lsmBtree);
        PrimaryIndexOperationTracker opTracker = (PrimaryIndexOperationTracker) lsmBtree.getOperationTracker();
        opTracker.setFlushOnExit(true);
        opTracker.flushIfNeeded();

        long maxWaitTime = TimeUnit.MINUTES.toNanos(1); // 1min
        // wait for log record is flushed, i.e., the flush is scheduled
        long before = System.nanoTime();
        while (opTracker.isFlushLogCreated()) {
            Thread.sleep(5); // NOSONAR: Test code with a timeout
            if (System.nanoTime() - before > maxWaitTime) {
                throw new IllegalStateException(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before)
                        + "ms passed without scheduling the flush operation");
            }
        }

        if (!async) {
            DatasetInfo dsInfo = dslLifecycleMgr.getDatasetInfo(dataset.getDatasetId());
            dsInfo.waitForIO();
        }
    }

    public static void flush(IDatasetLifecycleManager dsLifecycleMgr, TestLsmBtree lsmBtree, boolean async)
            throws Exception {
        flush(dsLifecycleMgr, lsmBtree, DATASET, async);
    }

    public static void flush(IDatasetLifecycleManager dsLifecycleMgr, Dataset dataset, TestLsmBtree lsmBtree,
            boolean async) throws Exception {
        flush(dsLifecycleMgr, lsmBtree, dataset, async);
    }

    public static void flush(IDatasetLifecycleManager dsLifecycleMgr, TestLsmBtree lsmBtree, Dataset dataset,
            boolean async) throws Exception {
        waitForOperations(lsmBtree);
        dsLifecycleMgr.flushDataset(dataset.getDatasetId(), async);
    }

    public static void waitForOperations(ILSMIndex index) throws InterruptedException {
        // wait until number of activeOperation reaches 0
        PrimaryIndexOperationTracker opTracker = (PrimaryIndexOperationTracker) index.getOperationTracker();
        long maxWaitTime = 60000L; // 1 minute
        long before = System.currentTimeMillis();
        while (opTracker.getNumActiveOperations() > 0) {
            Thread.sleep(5); // NOSONAR: Test code with a timeout
            if (System.currentTimeMillis() - before > maxWaitTime) {
                throw new IllegalStateException(
                        (System.currentTimeMillis() - before) + "ms passed without completing the frame operation");
            }
        }
    }

    public static class Searcher {
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private Future<Boolean> task;
        private volatile boolean entered = false;

        public Searcher(TestNodeController nc, int partition, TestLsmBtree lsmBtree, int numOfRecords) {
            this(nc, partition, DATASET, STORAGE_MANAGER, lsmBtree, numOfRecords);
        }

        public Searcher(TestNodeController nc, Dataset dataset, int partition, TestLsmBtree lsmBtree,
                int numOfRecords) {
            this(nc, partition, dataset, STORAGE_MANAGER, lsmBtree, numOfRecords);
        }

        public Searcher(TestNodeController nc, int partition, Dataset dataset, StorageComponentProvider storageManager,
                TestLsmBtree lsmBtree, int numOfRecords) {
            lsmBtree.addSearchCallback(new ITestOpCallback<Semaphore>() {

                @Override
                public void before(Semaphore sem) {
                    synchronized (Searcher.this) {
                        entered = true;
                        Searcher.this.notifyAll();
                    }
                }

                @Override
                public void after(Semaphore t) {
                }
            });
            Callable<Boolean> callable = new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    searchAndAssertCount(nc, partition, dataset, storageManager, numOfRecords);
                    return true;
                }
            };
            task = executor.submit(callable);
        }

        public boolean result() throws Exception {
            return task.get();
        }

        public synchronized void waitUntilEntered() throws InterruptedException {
            while (!entered) {
                this.wait();
            }
        }
    }

    public static class Merger {
        private volatile int count = 0;

        public Merger(TestLsmBtree lsmBtree) {
            lsmBtree.addMergeCallback(new ITestOpCallback<Semaphore>() {

                @Override
                public void before(Semaphore smeaphore) {
                    synchronized (Merger.this) {
                        count++;
                        Merger.this.notifyAll();
                    }
                }

                @Override
                public void after(Semaphore t) {
                }
            });
        }

        public synchronized void waitUntilCount(int count) throws InterruptedException {
            while (this.count != count) {
                this.wait();
            }
        }
    }

    public static class Flusher {
        private volatile int count = 0;

        public Flusher(TestLsmBtree lsmBtree) {
            lsmBtree.addFlushCallback(new ITestOpCallback<Semaphore>() {

                @Override
                public void before(Semaphore smeaphore) {
                    synchronized (Flusher.this) {
                        count++;
                        Flusher.this.notifyAll();
                    }
                }

                @Override
                public void after(Semaphore t) {
                }
            });
        }

        public synchronized void waitUntilCount(int count) throws InterruptedException {
            while (this.count != count) {
                this.wait();
            }
        }
    }
}
