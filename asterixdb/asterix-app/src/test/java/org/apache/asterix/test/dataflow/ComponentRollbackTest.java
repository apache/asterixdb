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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.data.gen.TestTupleCounterFrameWriter;
import org.apache.asterix.app.data.gen.TupleGenerator;
import org.apache.asterix.app.data.gen.TupleGenerator.GenerationFunction;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.dataflow.LSMInsertDeleteOperatorNodePushable;
import org.apache.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.test.common.TestHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.test.CountAnswer;
import org.apache.hyracks.api.test.FrameWriterTestUtils;
import org.apache.hyracks.api.test.FrameWriterTestUtils.FrameWriterOperation;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ComponentRollbackTest {

    private static final IAType[] KEY_TYPES = { BuiltinType.AINT32 };
    private static final ARecordType RECORD_TYPE = new ARecordType("TestRecordType", new String[] { "key", "value" },
            new IAType[] { BuiltinType.AINT32, BuiltinType.AINT64 }, false);
    private static final GenerationFunction[] RECORD_GEN_FUNCTION =
            { GenerationFunction.DETERMINISTIC, GenerationFunction.DETERMINISTIC };
    private static final boolean[] UNIQUE_RECORD_FIELDS = { true, false };
    private static final ARecordType META_TYPE = null;
    private static final GenerationFunction[] META_GEN_FUNCTION = null;
    private static final boolean[] UNIQUE_META_FIELDS = null;
    private static final int[] KEY_INDEXES = { 0 };
    private static final int[] KEY_INDICATORS = { Index.RECORD_INDICATOR };
    private static final List<Integer> KEY_INDICATORS_LIST = Arrays.asList(new Integer[] { Index.RECORD_INDICATOR });
    private static final int TOTAL_NUM_OF_RECORDS = 10000;
    private static final int RECORDS_PER_COMPONENT = 1000;
    private static final int DATASET_ID = 101;
    private static final String DATAVERSE_NAME = "TestDV";
    private static final String DATASET_NAME = "TestDS";
    private static final String DATA_TYPE_NAME = "DUMMY";
    private static final String NODE_GROUP_NAME = "DEFAULT";
    private static final Predicate<ILSMComponent> memoryComponentsPredicate = c -> c instanceof ILSMMemoryComponent;
    private static final StorageComponentProvider storageManager = new StorageComponentProvider();
    private static TestNodeController nc;
    private static TestLsmBtree lsmBtree;
    private static NCAppRuntimeContext ncAppCtx;
    private static IDatasetLifecycleManager dsLifecycleMgr;
    private static Dataset dataset;
    private static IHyracksTaskContext ctx;
    private static IIndexDataflowHelper indexDataflowHelper;
    private static ITransactionContext txnCtx;
    private static LSMInsertDeleteOperatorNodePushable insertOp;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("SetUp: ");
        TestHelper.deleteExistingInstanceFiles();
        nc = new TestNodeController(null, false);
        nc.init();
        ncAppCtx = nc.getAppRuntimeContext();
        dsLifecycleMgr = ncAppCtx.getDatasetLifecycleManager();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.out.println("TearDown");
        nc.deInit();
        TestHelper.deleteExistingInstanceFiles();
    }

    @Before
    public void createIndex() throws Exception {
        List<List<String>> partitioningKeys = new ArrayList<>();
        partitioningKeys.add(Collections.singletonList("key"));
        dataset = new TestDataset(DATAVERSE_NAME, DATASET_NAME, DATAVERSE_NAME, DATA_TYPE_NAME, NODE_GROUP_NAME,
                NoMergePolicyFactory.NAME, null, new InternalDatasetDetails(null, PartitioningStrategy.HASH,
                        partitioningKeys, null, null, null, false, null, false),
                null, DatasetType.INTERNAL, DATASET_ID, 0);
        PrimaryIndexInfo primaryIndexInfo = nc.createPrimaryIndex(dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null,
                storageManager, KEY_INDEXES, KEY_INDICATORS_LIST);
        IndexDataflowHelperFactory iHelperFactory =
                new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
        ctx = nc.createTestContext(false);
        indexDataflowHelper = iHelperFactory.create(ctx.getJobletContext().getServiceContext(), 0);
        indexDataflowHelper.open();
        lsmBtree = (TestLsmBtree) indexDataflowHelper.getIndexInstance();
        indexDataflowHelper.close();
        nc.newJobId();
        txnCtx = nc.getTransactionManager().getTransactionContext(nc.getTxnJobId(ctx), true);
        insertOp = nc.getInsertPipeline(ctx, dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null, KEY_INDEXES,
                KEY_INDICATORS_LIST, storageManager).getLeft();
    }

    @After
    public void destroyIndex() throws Exception {
        indexDataflowHelper.destroy();
    }

    private void allowAllOps(TestLsmBtree lsmBtree) {
        lsmBtree.addModifyCallback(sem -> sem.release());
        lsmBtree.addFlushCallback(sem -> sem.release());
        lsmBtree.addSearchCallback(sem -> sem.release());
        lsmBtree.addMergeCallback(sem -> sem.release());
    }

    @Test
    public void testRollbackWhileNoOp() {
        try {
            // allow all operations
            allowAllOps(lsmBtree);
            insertOp.open();
            TupleGenerator tupleGenerator = new TupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS,
                    RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % RECORDS_PER_COMPONENT == 0 && j + 1 != TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    dsLifecycleMgr.flushDataset(dataset.getDatasetId(), false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().completedTransaction(txnCtx, DatasetId.NULL, -1, true);

            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            ILSMIndexAccessor lsmAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            dsLifecycleMgr.getComponentIdGenerator(DATASET_ID).refresh();
            // rollback a memory component
            lsmAccessor.deleteComponents(memoryComponentsPredicate);
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS - RECORDS_PER_COMPONENT);
            // rollback the last disk component
            lsmAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            long lsn = AbstractLSMIOOperationCallback.getTreeIndexLSN(diskComponents.get(0).getMetadata());
            DiskComponentLsnPredicate pred = new DiskComponentLsnPredicate(lsn);
            dsLifecycleMgr.getComponentIdGenerator(DATASET_ID).refresh();
            lsmAccessor.deleteComponents(pred);
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS - (2 * RECORDS_PER_COMPONENT));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackThenInsert() {
        try {
            // allow all operations
            allowAllOps(lsmBtree);
            insertOp.open();
            TupleGenerator tupleGenerator = new TupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS,
                    RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % RECORDS_PER_COMPONENT == 0 && j + 1 != TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    dsLifecycleMgr.flushDataset(dataset.getDatasetId(), false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().completedTransaction(txnCtx, DatasetId.NULL, -1, true);

            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            ILSMIndexAccessor lsmAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            dsLifecycleMgr.getComponentIdGenerator(DATASET_ID).refresh();
            // rollback a memory component
            lsmAccessor.deleteComponents(memoryComponentsPredicate);
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS - RECORDS_PER_COMPONENT);

            // insert again
            nc.newJobId();
            txnCtx = nc.getTransactionManager().getTransactionContext(nc.getTxnJobId(ctx), true);
            insertOp = nc.getInsertPipeline(ctx, dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null, KEY_INDEXES,
                    KEY_INDICATORS_LIST, storageManager).getLeft();
            insertOp.open();
            for (int j = 0; j < RECORDS_PER_COMPONENT; j++) {
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().completedTransaction(txnCtx, DatasetId.NULL, -1, true);
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            // rollback the last disk component
            lsmAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            long lsn = AbstractLSMIOOperationCallback.getTreeIndexLSN(diskComponents.get(0).getMetadata());
            DiskComponentLsnPredicate pred = new DiskComponentLsnPredicate(lsn);
            dsLifecycleMgr.getComponentIdGenerator(DATASET_ID).refresh();
            lsmAccessor.deleteComponents(pred);
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS - (2 * RECORDS_PER_COMPONENT));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackWhileSearch() {
        try {
            // allow all operations but search
            allowAllOps(lsmBtree);
            lsmBtree.clearSearchCallbacks();
            insertOp.open();
            TupleGenerator tupleGenerator = new TupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS,
                    RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % RECORDS_PER_COMPONENT == 0 && j + 1 != TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    dsLifecycleMgr.flushDataset(dataset.getDatasetId(), false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().completedTransaction(txnCtx, DatasetId.NULL, -1, true);

            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            Searcher firstSearcher = new Searcher(nc, ctx, dataset, storageManager, lsmBtree, TOTAL_NUM_OF_RECORDS);
            // wait till firstSearcher enter the components
            firstSearcher.waitUntilEntered();
            // now that we enetered, we will rollback
            ILSMIndexAccessor lsmAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            dsLifecycleMgr.getComponentIdGenerator(DATASET_ID).refresh();
            // rollback a memory component
            lsmAccessor.deleteComponents(
                    c -> (c instanceof ILSMMemoryComponent && ((ILSMMemoryComponent) c).isModified()));
            // now that the rollback has completed, we will unblock the search
            lsmBtree.addSearchCallback(sem -> sem.release());
            lsmBtree.allowSearch(1);
            Assert.assertTrue(firstSearcher.result());
            // search now and ensure
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS - RECORDS_PER_COMPONENT);
            // rollback the last disk component
            // re-block searches
            lsmBtree.clearSearchCallbacks();
            Searcher secondSearcher = new Searcher(nc, ctx, dataset, storageManager, lsmBtree,
                    TOTAL_NUM_OF_RECORDS - RECORDS_PER_COMPONENT);
            // wait till firstSearcher enter the components
            secondSearcher.waitUntilEntered();
            lsmAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            long lsn = AbstractLSMIOOperationCallback.getTreeIndexLSN(diskComponents.get(0).getMetadata());
            DiskComponentLsnPredicate pred = new DiskComponentLsnPredicate(lsn);
            dsLifecycleMgr.getComponentIdGenerator(DATASET_ID).refresh();
            lsmAccessor.deleteComponents(pred);
            // now that the rollback has completed, we will unblock the search
            lsmBtree.addSearchCallback(sem -> sem.release());
            lsmBtree.allowSearch(1);
            Assert.assertTrue(secondSearcher.result());
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS - (2 * RECORDS_PER_COMPONENT));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackWhileFlush() {
        try {
            // allow all operations
            allowAllOps(lsmBtree);
            insertOp.open();
            TupleGenerator tupleGenerator = new TupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS,
                    RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % RECORDS_PER_COMPONENT == 0 && j + 1 != TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    dsLifecycleMgr.flushDataset(dataset.getDatasetId(), false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().completedTransaction(txnCtx, DatasetId.NULL, -1, true);
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            // disable flushes
            lsmBtree.clearFlushCallbacks();
            Flusher firstFlusher = new Flusher(lsmBtree);
            dsLifecycleMgr.flushDataset(dataset.getDatasetId(), true);
            firstFlusher.waitUntilCount(1);
            // now that we enetered, we will rollback. This will not proceed since it is waiting for the flush to complete
            Rollerback rollerback = new Rollerback(lsmBtree, memoryComponentsPredicate);
            // now that the rollback has completed, we will search
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            //unblock the flush
            lsmBtree.allowFlush(1);
            // ensure rollback completed
            rollerback.complete();
            // ensure current mem component is not modified
            Assert.assertFalse(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            // search now and ensure
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackWhileMerge() {
        try {
            // allow all operations but merge
            allowAllOps(lsmBtree);
            lsmBtree.clearMergeCallbacks();
            insertOp.open();
            TupleGenerator tupleGenerator = new TupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS,
                    RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % RECORDS_PER_COMPONENT == 0 && j + 1 != TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    dsLifecycleMgr.flushDataset(dataset.getDatasetId(), false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().completedTransaction(txnCtx, DatasetId.NULL, -1, true);
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            // Now, we will start a full merge
            Merger merger = new Merger(lsmBtree);
            ILSMIndexAccessor mergeAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            // select the components to merge... the last three
            int numMergedComponents = 3;
            List<ILSMDiskComponent> mergedComponents = new ArrayList<>();
            long lsn = AbstractLSMIOOperationCallback.getTreeIndexLSN(diskComponents.get(0).getMetadata());
            for (int i = 0; i < numMergedComponents; i++) {
                mergedComponents.add(diskComponents.get(i));
            }
            mergeAccessor.scheduleMerge(lsmBtree.getIOOperationCallback(), mergedComponents);
            merger.waitUntilCount(1);
            // now that we enetered, we will rollback
            Rollerback rollerback = new Rollerback(lsmBtree, new DiskComponentLsnPredicate(lsn));
            // rollback is now waiting for the merge to complete
            // we will search
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            //unblock the merge
            lsmBtree.allowMerge(1);
            // ensure rollback completes
            rollerback.complete();
            // ensure current mem component is not modified
            Assert.assertFalse(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            // search now and ensure that we rolled back the merged component
            searchAndAssertCount(nc, ctx, dataset, storageManager,
                    TOTAL_NUM_OF_RECORDS - ((numMergedComponents + 1/*memory component*/) * RECORDS_PER_COMPONENT));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackWhileFlushAndSearchFlushExistsFirst() {
        try {
            // allow all operations
            allowAllOps(lsmBtree);
            insertOp.open();
            TupleGenerator tupleGenerator = new TupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS,
                    RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % RECORDS_PER_COMPONENT == 0 && j + 1 != TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    dsLifecycleMgr.flushDataset(dataset.getDatasetId(), false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().completedTransaction(txnCtx, DatasetId.NULL, -1, true);
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            // disable flushes
            // disable searches
            lsmBtree.clearFlushCallbacks();
            lsmBtree.clearSearchCallbacks();
            Flusher firstFlusher = new Flusher(lsmBtree);
            dsLifecycleMgr.flushDataset(dataset.getDatasetId(), true);
            firstFlusher.waitUntilCount(1);
            Searcher firstSearcher = new Searcher(nc, ctx, dataset, storageManager, lsmBtree, TOTAL_NUM_OF_RECORDS);
            // wait till firstSearcher enter the components
            firstSearcher.waitUntilEntered();
            // now that we enetered, we will rollback rollback a memory component
            Rollerback rollerback = new Rollerback(lsmBtree, memoryComponentsPredicate);
            //unblock the flush
            lsmBtree.allowFlush(1);
            lsmBtree.addSearchCallback(sem -> sem.release());
            lsmBtree.allowSearch(1);
            Assert.assertTrue(firstSearcher.result());
            // ensure current mem component is not modified
            rollerback.complete();
            Assert.assertFalse(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            // search now and ensure the rollback was no op since it waits for ongoing flushes
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackWhileFlushAndSearchSearchExistsFirst() {
        try {
            // allow all operations
            allowAllOps(lsmBtree);
            insertOp.open();
            TupleGenerator tupleGenerator = new TupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS,
                    RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % RECORDS_PER_COMPONENT == 0 && j + 1 != TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    dsLifecycleMgr.flushDataset(dataset.getDatasetId(), false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().completedTransaction(txnCtx, DatasetId.NULL, -1, true);
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            // disable flushes
            // disable searches
            lsmBtree.clearFlushCallbacks();
            Flusher firstFlusher = new Flusher(lsmBtree);
            dsLifecycleMgr.flushDataset(dataset.getDatasetId(), true);
            firstFlusher.waitUntilCount(1);
            lsmBtree.clearSearchCallbacks();
            Searcher firstSearcher = new Searcher(nc, ctx, dataset, storageManager, lsmBtree, TOTAL_NUM_OF_RECORDS);
            // wait till firstSearcher enter the components
            firstSearcher.waitUntilEntered();
            // now that we enetered, we will rollback
            Rollerback rollerback = new Rollerback(lsmBtree, memoryComponentsPredicate);
            // The rollback will be waiting for the flush to complete
            lsmBtree.addSearchCallback(sem -> sem.release());
            lsmBtree.allowSearch(1);
            Assert.assertTrue(firstSearcher.result());
            //unblock the flush
            lsmBtree.allowFlush(1);
            // ensure current mem component is not modified
            rollerback.complete();
            Assert.assertFalse(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            // search now and ensure
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackWhileMergeAndSearchMergeExitsFirst() {
        try {
            // allow all operations except merge
            allowAllOps(lsmBtree);
            lsmBtree.clearMergeCallbacks();
            insertOp.open();
            TupleGenerator tupleGenerator = new TupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS,
                    RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % RECORDS_PER_COMPONENT == 0 && j + 1 != TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    dsLifecycleMgr.flushDataset(dataset.getDatasetId(), false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().completedTransaction(txnCtx, DatasetId.NULL, -1, true);
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            // Now, we will start a merge
            Merger merger = new Merger(lsmBtree);
            ILSMIndexAccessor mergeAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            // select the components to merge... the last three
            int numMergedComponents = 3;
            List<ILSMDiskComponent> mergedComponents = new ArrayList<>();
            long lsn = AbstractLSMIOOperationCallback.getTreeIndexLSN(diskComponents.get(0).getMetadata());
            for (int i = 0; i < numMergedComponents; i++) {
                mergedComponents.add(diskComponents.get(i));
            }
            mergeAccessor.scheduleMerge(lsmBtree.getIOOperationCallback(), mergedComponents);
            merger.waitUntilCount(1);
            // we will block search
            lsmBtree.clearSearchCallbacks();
            Searcher firstSearcher = new Searcher(nc, ctx, dataset, storageManager, lsmBtree, TOTAL_NUM_OF_RECORDS);
            // wait till firstSearcher enter the components
            firstSearcher.waitUntilEntered();
            // now that we enetered, we will rollback
            Rollerback rollerback = new Rollerback(lsmBtree, new DiskComponentLsnPredicate(lsn));
            // the rollback is waiting for all flushes and merges to complete before it proceeds
            // unblock the merge
            lsmBtree.allowMerge(1);
            // unblock the search
            lsmBtree.addSearchCallback(sem -> sem.release());
            lsmBtree.allowSearch(1);
            Assert.assertTrue(firstSearcher.result());
            rollerback.complete();
            // now that the rollback has completed, we will search
            searchAndAssertCount(nc, ctx, dataset, storageManager,
                    TOTAL_NUM_OF_RECORDS - ((numMergedComponents + 1/*memory component*/) * RECORDS_PER_COMPONENT));
            // ensure current mem component is not modified
            Assert.assertFalse(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackWhileMergeAndSearchSearchExitsFirst() {
        try {
            // allow all operations except merge
            allowAllOps(lsmBtree);
            lsmBtree.clearMergeCallbacks();
            insertOp.open();
            TupleGenerator tupleGenerator = new TupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS,
                    RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % RECORDS_PER_COMPONENT == 0 && j + 1 != TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    dsLifecycleMgr.flushDataset(dataset.getDatasetId(), false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().completedTransaction(txnCtx, DatasetId.NULL, -1, true);
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            // Now, we will start a merge
            Merger merger = new Merger(lsmBtree);
            ILSMIndexAccessor mergeAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            // select the components to merge... the last three
            List<ILSMDiskComponent> mergedComponents = new ArrayList<>();
            long lsn = AbstractLSMIOOperationCallback.getTreeIndexLSN(diskComponents.get(0).getMetadata());
            int numMergedComponents = 3;
            for (int i = 0; i < numMergedComponents; i++) {
                mergedComponents.add(diskComponents.get(i));
            }
            mergeAccessor.scheduleMerge(lsmBtree.getIOOperationCallback(), mergedComponents);
            merger.waitUntilCount(1);
            // we will block search
            lsmBtree.clearSearchCallbacks();
            Searcher firstSearcher = new Searcher(nc, ctx, dataset, storageManager, lsmBtree, TOTAL_NUM_OF_RECORDS);
            // wait till firstSearcher enter the components
            firstSearcher.waitUntilEntered();
            // now that we enetered, we will rollback
            Rollerback rollerBack = new Rollerback(lsmBtree, new DiskComponentLsnPredicate(lsn));
            // unblock the search
            lsmBtree.addSearchCallback(sem -> sem.release());
            lsmBtree.allowSearch(1);
            Assert.assertTrue(firstSearcher.result());
            // even though rollback has been called, it is still waiting for the merge to complete
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            //unblock the merge
            lsmBtree.allowMerge(1);
            rollerBack.complete();
            searchAndAssertCount(nc, ctx, dataset, storageManager,
                    TOTAL_NUM_OF_RECORDS - ((numMergedComponents + 1/*memory component*/) * RECORDS_PER_COMPONENT));
            // ensure current mem component is not modified
            Assert.assertFalse(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private class Rollerback {
        private final Thread task;
        private Exception failure;

        public Rollerback(TestLsmBtree lsmBtree, Predicate<ILSMComponent> predicate) {
            // now that we enetered, we will rollback
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    ILSMIndexAccessor lsmAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                    dsLifecycleMgr.getComponentIdGenerator(DATASET_ID).refresh();
                    try {
                        lsmAccessor.deleteComponents(predicate);
                    } catch (HyracksDataException e) {
                        failure = e;
                    }
                }
            };
            task = new Thread(runnable);
            task.start();
        }

        void complete() throws Exception {
            task.join();
            if (failure != null) {
                throw failure;
            }
        }
    }

    private class Searcher {
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private Future<Boolean> task;
        private volatile boolean entered = false;

        public Searcher(TestNodeController nc, IHyracksTaskContext ctx, Dataset dataset,
                StorageComponentProvider storageManager, TestLsmBtree lsmBtree, int numOfRecords) {
            lsmBtree.addSearchCallback(sem -> {
                synchronized (Searcher.this) {
                    entered = true;
                    Searcher.this.notifyAll();
                }
            });
            Callable<Boolean> callable = new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    searchAndAssertCount(nc, ctx, dataset, storageManager, numOfRecords);
                    return true;
                }
            };
            task = executor.submit(callable);
        }

        boolean result() throws Exception {
            return task.get();
        }

        synchronized void waitUntilEntered() throws InterruptedException {
            while (!entered) {
                this.wait();
            }
        }
    }

    private class Merger {
        private volatile int count = 0;

        public Merger(TestLsmBtree lsmBtree) {
            lsmBtree.addMergeCallback(sem -> {
                synchronized (Merger.this) {
                    count++;
                    Merger.this.notifyAll();
                }
            });
        }

        synchronized void waitUntilCount(int count) throws InterruptedException {
            while (this.count != count) {
                this.wait();
            }
        }
    }

    private class Flusher {
        private volatile int count = 0;

        public Flusher(TestLsmBtree lsmBtree) {
            lsmBtree.addFlushCallback(sem -> {
                synchronized (Flusher.this) {
                    count++;
                    Flusher.this.notifyAll();
                }
            });
        }

        synchronized void waitUntilCount(int count) throws InterruptedException {
            while (this.count != count) {
                this.wait();
            }
        }
    }

    private class DiskComponentLsnPredicate implements Predicate<ILSMComponent> {
        private final long lsn;

        public DiskComponentLsnPredicate(long lsn) {
            this.lsn = lsn;
        }

        @Override
        public boolean test(ILSMComponent c) {
            try {
                return c instanceof ILSMMemoryComponent
                        || (c instanceof ILSMDiskComponent && AbstractLSMIOOperationCallback
                                .getTreeIndexLSN(((ILSMDiskComponent) c).getMetadata()) >= lsn);
            } catch (HyracksDataException e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    private void searchAndAssertCount(TestNodeController nc, IHyracksTaskContext ctx, Dataset dataset,
            StorageComponentProvider storageManager, int numOfRecords)
            throws HyracksDataException, AlgebricksException {
        nc.newJobId();
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
}