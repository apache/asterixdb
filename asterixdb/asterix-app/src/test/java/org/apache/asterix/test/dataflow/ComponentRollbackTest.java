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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.data.gen.RecordTupleGenerator;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
import org.apache.asterix.test.common.TestHelper;
import org.apache.asterix.test.dataflow.StorageTestUtils.Flusher;
import org.apache.asterix.test.dataflow.StorageTestUtils.Merger;
import org.apache.asterix.test.dataflow.StorageTestUtils.Searcher;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.btree.impl.AllowTestOpCallback;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ComponentRollbackTest {

    private static final Predicate<ILSMComponent> memoryComponentsPredicate = c -> c instanceof ILSMMemoryComponent;
    private static TestNodeController nc;
    private static TestLsmBtree lsmBtree;
    private static NCAppRuntimeContext ncAppCtx;
    private static IDatasetLifecycleManager dsLifecycleMgr;
    private static IHyracksTaskContext ctx;
    private static IIndexDataflowHelper indexDataflowHelper;
    private static ITransactionContext txnCtx;
    private static LSMPrimaryInsertOperatorNodePushable insertOp;
    private static final int PARTITION = 0;
    private static String indexPath;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("SetUp: ");
        TestHelper.deleteExistingInstanceFiles();
        String configPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "test"
                + File.separator + "resources" + File.separator + "cc.conf";
        nc = new TestNodeController(configPath, false);
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
        PrimaryIndexInfo primaryIndexInfo = StorageTestUtils.createPrimaryIndex(nc, PARTITION);
        IndexDataflowHelperFactory iHelperFactory =
                new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
        JobId jobId = nc.newJobId();
        ctx = nc.createTestContext(jobId, PARTITION, false);
        indexDataflowHelper = iHelperFactory.create(ctx.getJobletContext().getServiceContext(), PARTITION);
        indexDataflowHelper.open();
        lsmBtree = (TestLsmBtree) indexDataflowHelper.getIndexInstance();
        indexDataflowHelper.close();
        txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(ctx),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
        insertOp = StorageTestUtils.getInsertPipeline(nc, ctx);
        indexPath = indexDataflowHelper.getResource().getPath();
    }

    @After
    public void destroyIndex() throws Exception {
        indexDataflowHelper.destroy();
    }

    @Test
    public void testRollbackWhileNoOp() {
        try {
            // allow all operations
            StorageTestUtils.allowAllOps(lsmBtree);
            insertOp.open();
            RecordTupleGenerator tupleGenerator = StorageTestUtils.getTupleGenerator();
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < StorageTestUtils.TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % StorageTestUtils.RECORDS_PER_COMPONENT == 0 && j + 1 != StorageTestUtils.TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    flush(false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            ILSMIndexAccessor lsmAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            dsLifecycleMgr.getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath).refresh();
            ILSMComponentId next =
                    dsLifecycleMgr.getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath).getId();
            long flushLsn = nc.getTransactionSubsystem().getLogManager().getAppendLSN();
            Map<String, Object> flushMap = new HashMap<>();
            flushMap.put(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN, flushLsn);
            flushMap.put(LSMIOOperationCallback.KEY_NEXT_COMPONENT_ID, next);
            lsmAccessor.getOpContext().setParameters(flushMap);
            // rollback a memory component
            lsmAccessor.deleteComponents(memoryComponentsPredicate);
            StorageTestUtils.searchAndAssertCount(nc, PARTITION,
                    StorageTestUtils.TOTAL_NUM_OF_RECORDS - StorageTestUtils.RECORDS_PER_COMPONENT);
            // rollback the last disk component
            lsmAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            long lsn = LSMIOOperationCallback.getTreeIndexLSN(diskComponents.get(0).getMetadata());
            dsLifecycleMgr.getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath).refresh();
            next = dsLifecycleMgr.getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath).getId();
            flushLsn = nc.getTransactionSubsystem().getLogManager().getAppendLSN();
            flushMap = new HashMap<>();
            flushMap.put(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN, flushLsn);
            flushMap.put(LSMIOOperationCallback.KEY_NEXT_COMPONENT_ID, next);
            lsmAccessor.getOpContext().setParameters(flushMap);
            DiskComponentLsnPredicate pred = new DiskComponentLsnPredicate(lsn);
            lsmAccessor.deleteComponents(pred);
            StorageTestUtils.searchAndAssertCount(nc, PARTITION,
                    StorageTestUtils.TOTAL_NUM_OF_RECORDS - (2 * StorageTestUtils.RECORDS_PER_COMPONENT));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    public void flush(boolean async) throws Exception {
        StorageTestUtils.flush(dsLifecycleMgr, lsmBtree, async);
    }

    @Test
    public void testRollbackThenInsert() {
        try {
            // allow all operations
            StorageTestUtils.allowAllOps(lsmBtree);
            insertOp.open();
            RecordTupleGenerator tupleGenerator = StorageTestUtils.getTupleGenerator();
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < StorageTestUtils.TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % StorageTestUtils.RECORDS_PER_COMPONENT == 0 && j + 1 != StorageTestUtils.TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    flush(false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            ILSMIndexAccessor lsmAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            dsLifecycleMgr.getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath).refresh();
            ILSMComponentId next =
                    dsLifecycleMgr.getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath).getId();
            long flushLsn = nc.getTransactionSubsystem().getLogManager().getAppendLSN();
            Map<String, Object> flushMap = new HashMap<>();
            flushMap.put(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN, flushLsn);
            flushMap.put(LSMIOOperationCallback.KEY_NEXT_COMPONENT_ID, next);
            lsmAccessor.getOpContext().setParameters(flushMap);
            // rollback a memory component
            lsmAccessor.deleteComponents(memoryComponentsPredicate);
            StorageTestUtils.searchAndAssertCount(nc, PARTITION,
                    StorageTestUtils.TOTAL_NUM_OF_RECORDS - StorageTestUtils.RECORDS_PER_COMPONENT);

            // insert again
            nc.newJobId();
            txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(ctx),
                    new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
            insertOp = StorageTestUtils.getInsertPipeline(nc, ctx);
            insertOp.open();
            for (int j = 0; j < StorageTestUtils.RECORDS_PER_COMPONENT; j++) {
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            // rollback the last disk component
            lsmAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            long lsn = LSMIOOperationCallback.getTreeIndexLSN(diskComponents.get(0).getMetadata());
            dsLifecycleMgr.getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath).refresh();
            next = dsLifecycleMgr.getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath).getId();
            flushLsn = nc.getTransactionSubsystem().getLogManager().getAppendLSN();
            flushMap = new HashMap<>();
            flushMap.put(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN, flushLsn);
            flushMap.put(LSMIOOperationCallback.KEY_NEXT_COMPONENT_ID, next);
            lsmAccessor.getOpContext().setParameters(flushMap);
            DiskComponentLsnPredicate pred = new DiskComponentLsnPredicate(lsn);
            lsmAccessor.deleteComponents(pred);
            StorageTestUtils.searchAndAssertCount(nc, PARTITION,
                    StorageTestUtils.TOTAL_NUM_OF_RECORDS - (2 * StorageTestUtils.RECORDS_PER_COMPONENT));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackWhileSearch() {
        try {
            // allow all operations but search
            StorageTestUtils.allowAllOps(lsmBtree);
            lsmBtree.clearSearchCallbacks();
            insertOp.open();
            RecordTupleGenerator tupleGenerator = StorageTestUtils.getTupleGenerator();
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < StorageTestUtils.TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % StorageTestUtils.RECORDS_PER_COMPONENT == 0 && j + 1 != StorageTestUtils.TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    flush(false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());

            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            Searcher firstSearcher = new Searcher(nc, PARTITION, lsmBtree, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            // wait till firstSearcher enter the components
            firstSearcher.waitUntilEntered();
            // now that we enetered, we will rollback
            ILSMIndexAccessor lsmAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            dsLifecycleMgr.getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath).refresh();
            ILSMComponentId next =
                    dsLifecycleMgr.getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath).getId();
            long flushLsn = nc.getTransactionSubsystem().getLogManager().getAppendLSN();
            Map<String, Object> flushMap = new HashMap<>();
            flushMap.put(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN, flushLsn);
            flushMap.put(LSMIOOperationCallback.KEY_NEXT_COMPONENT_ID, next);
            lsmAccessor.getOpContext().setParameters(flushMap);
            // rollback a memory component
            lsmAccessor.deleteComponents(
                    c -> (c instanceof ILSMMemoryComponent && ((ILSMMemoryComponent) c).isModified()));
            // now that the rollback has completed, we will unblock the search
            lsmBtree.addSearchCallback(AllowTestOpCallback.INSTANCE);
            lsmBtree.allowSearch(1);
            Assert.assertTrue(firstSearcher.result());
            // search now and ensure
            StorageTestUtils.searchAndAssertCount(nc, PARTITION,
                    StorageTestUtils.TOTAL_NUM_OF_RECORDS - StorageTestUtils.RECORDS_PER_COMPONENT);
            // rollback the last disk component
            // re-block searches
            lsmBtree.clearSearchCallbacks();
            Searcher secondSearcher = new Searcher(nc, PARTITION, lsmBtree,
                    StorageTestUtils.TOTAL_NUM_OF_RECORDS - StorageTestUtils.RECORDS_PER_COMPONENT);
            // wait till firstSearcher enter the components
            secondSearcher.waitUntilEntered();
            lsmAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            long lsn = LSMIOOperationCallback.getTreeIndexLSN(diskComponents.get(0).getMetadata());
            dsLifecycleMgr.getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath).refresh();
            next = dsLifecycleMgr.getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath).getId();
            flushLsn = nc.getTransactionSubsystem().getLogManager().getAppendLSN();
            flushMap = new HashMap<>();
            flushMap.put(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN, flushLsn);
            flushMap.put(LSMIOOperationCallback.KEY_NEXT_COMPONENT_ID, next);
            lsmAccessor.getOpContext().setParameters(flushMap);
            DiskComponentLsnPredicate pred = new DiskComponentLsnPredicate(lsn);
            lsmAccessor.deleteComponents(pred);
            // now that the rollback has completed, we will unblock the search
            lsmBtree.addSearchCallback(AllowTestOpCallback.INSTANCE);
            lsmBtree.allowSearch(1);
            Assert.assertTrue(secondSearcher.result());
            StorageTestUtils.searchAndAssertCount(nc, PARTITION,
                    StorageTestUtils.TOTAL_NUM_OF_RECORDS - (2 * StorageTestUtils.RECORDS_PER_COMPONENT));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackWhileFlush() {
        try {
            // allow all operations
            StorageTestUtils.allowAllOps(lsmBtree);
            insertOp.open();
            RecordTupleGenerator tupleGenerator = StorageTestUtils.getTupleGenerator();
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < StorageTestUtils.TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % StorageTestUtils.RECORDS_PER_COMPONENT == 0 && j + 1 != StorageTestUtils.TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    flush(false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            // disable flushes
            lsmBtree.clearFlushCallbacks();
            Flusher firstFlusher = new Flusher(lsmBtree);
            flush(true);
            firstFlusher.waitUntilCount(1);
            // now that we enetered, we will rollback. This will not proceed since it is waiting for the flush to complete
            Rollerback rollerback = new Rollerback(lsmBtree, memoryComponentsPredicate);
            // now that the rollback has completed, we will search
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            //unblock the flush
            lsmBtree.allowFlush(1);
            // ensure rollback completed
            rollerback.complete();
            // ensure current mem component is not modified
            Assert.assertFalse(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            // search now and ensure
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackWhileMerge() {
        try {
            // allow all operations but merge
            StorageTestUtils.allowAllOps(lsmBtree);
            lsmBtree.clearMergeCallbacks();
            insertOp.open();
            RecordTupleGenerator tupleGenerator = StorageTestUtils.getTupleGenerator();
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < StorageTestUtils.TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % StorageTestUtils.RECORDS_PER_COMPONENT == 0 && j + 1 != StorageTestUtils.TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    flush(false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            // Now, we will start a full merge
            Merger merger = new Merger(lsmBtree);
            ILSMIndexAccessor mergeAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            // select the components to merge... the last three
            int numMergedComponents = 3;
            List<ILSMDiskComponent> mergedComponents = new ArrayList<>();
            long lsn = LSMIOOperationCallback.getTreeIndexLSN(diskComponents.get(0).getMetadata());
            for (int i = 0; i < numMergedComponents; i++) {
                mergedComponents.add(diskComponents.get(i));
            }
            mergeAccessor.scheduleMerge(mergedComponents);
            merger.waitUntilCount(1);
            // now that we enetered, we will rollback
            Rollerback rollerback = new Rollerback(lsmBtree, new DiskComponentLsnPredicate(lsn));
            // rollback is now waiting for the merge to complete
            // we will search
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            //unblock the merge
            lsmBtree.allowMerge(1);
            // ensure rollback completes
            rollerback.complete();
            // ensure current mem component is not modified
            Assert.assertFalse(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            // search now and ensure that we rolled back the merged component
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS
                    - ((numMergedComponents + 1/*memory component*/) * StorageTestUtils.RECORDS_PER_COMPONENT));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackWhileFlushAndSearchFlushExistsFirst() {
        try {
            // allow all operations
            StorageTestUtils.allowAllOps(lsmBtree);
            insertOp.open();
            RecordTupleGenerator tupleGenerator = StorageTestUtils.getTupleGenerator();
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < StorageTestUtils.TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % StorageTestUtils.RECORDS_PER_COMPONENT == 0 && j + 1 != StorageTestUtils.TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    flush(false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            // disable flushes
            // disable searches
            lsmBtree.clearFlushCallbacks();
            lsmBtree.clearSearchCallbacks();
            Flusher firstFlusher = new Flusher(lsmBtree);
            flush(true);
            firstFlusher.waitUntilCount(1);
            Searcher firstSearcher = new Searcher(nc, PARTITION, lsmBtree, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            // wait till firstSearcher enter the components
            firstSearcher.waitUntilEntered();
            // now that we enetered, we will rollback rollback a memory component
            Rollerback rollerback = new Rollerback(lsmBtree, memoryComponentsPredicate);
            //unblock the flush
            lsmBtree.allowFlush(1);
            lsmBtree.addSearchCallback(AllowTestOpCallback.INSTANCE);
            lsmBtree.allowSearch(1);
            Assert.assertTrue(firstSearcher.result());
            // ensure current mem component is not modified
            rollerback.complete();
            Assert.assertFalse(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            // search now and ensure the rollback was no op since it waits for ongoing flushes
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackWhileFlushAndSearchSearchExistsFirst() {
        try {
            // allow all operations
            StorageTestUtils.allowAllOps(lsmBtree);
            insertOp.open();
            RecordTupleGenerator tupleGenerator = StorageTestUtils.getTupleGenerator();
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < StorageTestUtils.TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % StorageTestUtils.RECORDS_PER_COMPONENT == 0 && j + 1 != StorageTestUtils.TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    flush(false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            // disable flushes
            // disable searches
            lsmBtree.clearFlushCallbacks();
            Flusher firstFlusher = new Flusher(lsmBtree);
            flush(true);
            firstFlusher.waitUntilCount(1);
            lsmBtree.clearSearchCallbacks();
            Searcher firstSearcher = new Searcher(nc, PARTITION, lsmBtree, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            // wait till firstSearcher enter the components
            firstSearcher.waitUntilEntered();
            // now that we enetered, we will rollback
            Rollerback rollerback = new Rollerback(lsmBtree, memoryComponentsPredicate);
            // The rollback will be waiting for the flush to complete
            lsmBtree.addSearchCallback(AllowTestOpCallback.INSTANCE);
            lsmBtree.allowSearch(1);
            Assert.assertTrue(firstSearcher.result());
            //unblock the flush
            lsmBtree.allowFlush(1);
            // ensure current mem component is not modified
            rollerback.complete();
            Assert.assertFalse(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            // search now and ensure
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRollbackWhileMergeAndSearchMergeExitsFirst() {
        try {
            // allow all operations except merge
            StorageTestUtils.allowAllOps(lsmBtree);
            lsmBtree.clearMergeCallbacks();
            insertOp.open();
            RecordTupleGenerator tupleGenerator = StorageTestUtils.getTupleGenerator();
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < StorageTestUtils.TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % StorageTestUtils.RECORDS_PER_COMPONENT == 0 && j + 1 != StorageTestUtils.TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    flush(false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            // Now, we will start a merge
            Merger merger = new Merger(lsmBtree);
            ILSMIndexAccessor mergeAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            // select the components to merge... the last three
            int numMergedComponents = 3;
            List<ILSMDiskComponent> mergedComponents = new ArrayList<>();
            long lsn = LSMIOOperationCallback.getTreeIndexLSN(diskComponents.get(0).getMetadata());
            for (int i = 0; i < numMergedComponents; i++) {
                mergedComponents.add(diskComponents.get(i));
            }
            mergeAccessor.scheduleMerge(mergedComponents);
            merger.waitUntilCount(1);
            // we will block search
            lsmBtree.clearSearchCallbacks();
            Searcher firstSearcher = new Searcher(nc, PARTITION, lsmBtree, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            // wait till firstSearcher enter the components
            firstSearcher.waitUntilEntered();
            // now that we enetered, we will rollback
            Rollerback rollerback = new Rollerback(lsmBtree, new DiskComponentLsnPredicate(lsn));
            // the rollback is waiting for all flushes and merges to complete before it proceeds
            // unblock the merge
            lsmBtree.allowMerge(1);
            // unblock the search
            lsmBtree.addSearchCallback(AllowTestOpCallback.INSTANCE);
            lsmBtree.allowSearch(1);
            Assert.assertTrue(firstSearcher.result());
            rollerback.complete();
            // now that the rollback has completed, we will search
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS
                    - ((numMergedComponents + 1/*memory component*/) * StorageTestUtils.RECORDS_PER_COMPONENT));
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
            StorageTestUtils.allowAllOps(lsmBtree);
            lsmBtree.clearMergeCallbacks();
            insertOp.open();
            RecordTupleGenerator tupleGenerator = StorageTestUtils.getTupleGenerator();
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            for (int j = 0; j < StorageTestUtils.TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % StorageTestUtils.RECORDS_PER_COMPONENT == 0 && j + 1 != StorageTestUtils.TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    flush(false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            // get all components
            List<ILSMMemoryComponent> memComponents = lsmBtree.getMemoryComponents();
            List<ILSMDiskComponent> diskComponents = lsmBtree.getDiskComponents();
            Assert.assertEquals(9, diskComponents.size());
            Assert.assertTrue(memComponents.get(lsmBtree.getCurrentMemoryComponentIndex()).isModified());
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            // Now, we will start a merge
            Merger merger = new Merger(lsmBtree);
            ILSMIndexAccessor mergeAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            // select the components to merge... the last three
            List<ILSMDiskComponent> mergedComponents = new ArrayList<>();
            long lsn = LSMIOOperationCallback.getTreeIndexLSN(diskComponents.get(0).getMetadata());
            int numMergedComponents = 3;
            for (int i = 0; i < numMergedComponents; i++) {
                mergedComponents.add(diskComponents.get(i));
            }
            mergeAccessor.scheduleMerge(mergedComponents);
            merger.waitUntilCount(1);
            // we will block search
            lsmBtree.clearSearchCallbacks();
            Searcher firstSearcher = new Searcher(nc, PARTITION, lsmBtree, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            // wait till firstSearcher enter the components
            firstSearcher.waitUntilEntered();
            // now that we enetered, we will rollback
            Rollerback rollerBack = new Rollerback(lsmBtree, new DiskComponentLsnPredicate(lsn));
            // unblock the search
            lsmBtree.addSearchCallback(AllowTestOpCallback.INSTANCE);
            lsmBtree.allowSearch(1);
            Assert.assertTrue(firstSearcher.result());
            // even though rollback has been called, it is still waiting for the merge to complete
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS);
            //unblock the merge
            lsmBtree.allowMerge(1);
            rollerBack.complete();
            StorageTestUtils.searchAndAssertCount(nc, PARTITION, StorageTestUtils.TOTAL_NUM_OF_RECORDS
                    - ((numMergedComponents + 1/*memory component*/) * StorageTestUtils.RECORDS_PER_COMPONENT));
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
                    try {
                        dsLifecycleMgr.getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath)
                                .refresh();
                        ILSMComponentId next = dsLifecycleMgr
                                .getComponentIdGenerator(StorageTestUtils.DATASET_ID, PARTITION, indexPath).getId();
                        long flushLsn = nc.getTransactionSubsystem().getLogManager().getAppendLSN();
                        Map<String, Object> flushMap = new HashMap<>();
                        flushMap.put(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN, flushLsn);
                        flushMap.put(LSMIOOperationCallback.KEY_NEXT_COMPONENT_ID, next);
                        lsmAccessor.getOpContext().setParameters(flushMap);
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

    private static class DiskComponentLsnPredicate implements Predicate<ILSMComponent> {
        private final long lsn;

        public DiskComponentLsnPredicate(long lsn) {
            this.lsn = lsn;
        }

        @Override
        public boolean test(ILSMComponent c) {
            try {
                return c instanceof ILSMMemoryComponent || (c instanceof ILSMDiskComponent
                        && LSMIOOperationCallback.getTreeIndexLSN(((ILSMDiskComponent) c).getMetadata()) >= lsn);
            } catch (HyracksDataException e) {
                e.printStackTrace();
                return false;
            }
        }
    }
}
