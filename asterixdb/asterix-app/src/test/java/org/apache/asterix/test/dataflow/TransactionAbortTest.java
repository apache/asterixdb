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

import java.nio.file.Paths;
import java.util.concurrent.Semaphore;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.data.gen.RecordTupleGenerator;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.dataflow.LSMInsertDeleteOperatorNodePushable;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
import org.apache.asterix.test.base.TestMethodTracer;
import org.apache.asterix.test.common.TestHelper;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impl.ITestOpCallback;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TransactionAbortTest {
    private static TestNodeController nc;
    private static TestLsmBtree lsmBtree;
    private static NCAppRuntimeContext ncAppCtx;
    private static IDatasetLifecycleManager dsLifecycleMgr;
    private static IHyracksTaskContext ctx;
    private static IIndexDataflowHelper indexDataflowHelper;
    private static final int PARTITION = 0;
    private static LSMPrimaryInsertOperatorNodePushable insertOp;
    private static int NUM_INSERT_RECORDS = 1000;
    private static ITransactionContext txnCtx;

    private static IHyracksTaskContext abortCtx;
    private static ITransactionContext abortTxnCtx;
    private static LSMInsertDeleteOperatorNodePushable abortOp;
    private static RecordTupleGenerator tupleGenerator;

    @Rule
    public TestRule watcher = new TestMethodTracer();

    @BeforeClass
    public static void setUp() throws Exception {
        TestHelper.deleteExistingInstanceFiles();
        String configPath = Paths.get(System.getProperty("user.dir"), "src", "test", "resources", "cc.conf").toString();
        nc = new TestNodeController(configPath, false);
        nc.init();
        ncAppCtx = nc.getAppRuntimeContext();
        dsLifecycleMgr = ncAppCtx.getDatasetLifecycleManager();
    }

    @AfterClass
    public static void tearDown() throws Exception {
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
        insertOp = StorageTestUtils.getInsertPipeline(nc, ctx, null);

        JobId abortJobId = nc.newJobId();
        abortCtx = nc.createTestContext(abortJobId, PARTITION, false);
        abortTxnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(abortCtx),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
        // abortOp is initialized by each test separately
        tupleGenerator = StorageTestUtils.getTupleGenerator();
    }

    @Test
    public void testAbortDeleteFromDiskComponent() throws Exception {
        ITupleReference lastTuple = insertRecords(NUM_INSERT_RECORDS);
        StorageTestUtils.flush(dsLifecycleMgr, lsmBtree, false);

        Assert.assertEquals(1, lsmBtree.getDiskComponents().size());
        StorageTestUtils.searchAndAssertCount(nc, PARTITION, NUM_INSERT_RECORDS);

        abortOp = StorageTestUtils.getDeletePipeline(nc, abortCtx, null);
        testAbort(lastTuple);
        StorageTestUtils.searchAndAssertCount(nc, PARTITION, NUM_INSERT_RECORDS);
    }

    @Test
    public void testAbortDeleteFromMemoryComponent() throws Exception {
        ITupleReference lastTuple = insertRecords(NUM_INSERT_RECORDS);
        Assert.assertEquals(0, lsmBtree.getDiskComponents().size());
        StorageTestUtils.searchAndAssertCount(nc, PARTITION, NUM_INSERT_RECORDS);

        abortOp = StorageTestUtils.getDeletePipeline(nc, abortCtx, null);
        testAbort(lastTuple);
        StorageTestUtils.searchAndAssertCount(nc, PARTITION, NUM_INSERT_RECORDS);
    }

    @Test
    public void testAbortInsert() throws Exception {
        insertRecords(NUM_INSERT_RECORDS);
        Assert.assertEquals(0, lsmBtree.getDiskComponents().size());
        StorageTestUtils.searchAndAssertCount(nc, PARTITION, NUM_INSERT_RECORDS);

        abortOp = StorageTestUtils.getDeletePipeline(nc, abortCtx, null);
        testAbort(tupleGenerator.next());
        StorageTestUtils.searchAndAssertCount(nc, PARTITION, NUM_INSERT_RECORDS);
    }

    @After
    public void destroyIndex() throws Exception {
        indexDataflowHelper.destroy();
    }

    private ITupleReference insertRecords(int numRecords) throws Exception {
        StorageTestUtils.allowAllOps(lsmBtree);
        insertOp.open();
        VSizeFrame frame = new VSizeFrame(ctx);
        FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
        ITupleReference tuple = null;
        for (int i = 0; i < numRecords; i++) {
            tuple = tupleGenerator.next();
            DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
        }
        if (tupleAppender.getTupleCount() > 0) {
            tupleAppender.write(insertOp, true);
        }
        insertOp.close();
        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
        return tuple;
    }

    private void testAbort(ITupleReference tuple) throws Exception {
        setFailModificationCallback(lsmBtree);
        abortOp.open();
        boolean aborted = false;
        VSizeFrame frame = new VSizeFrame(ctx);
        FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
        try {
            DataflowUtils.addTupleToFrame(tupleAppender, tuple, abortOp);
            tupleAppender.write(abortOp, true);
        } catch (HyracksDataException e) {
            StorageTestUtils.allowAllOps(lsmBtree);
            nc.getTransactionManager().abortTransaction(abortTxnCtx.getTxnId());
            aborted = true;
        } finally {
            abortOp.close();
        }
        Assert.assertTrue(aborted);
    }

    private void setFailModificationCallback(TestLsmBtree index) {
        index.clearModifyCallbacks();
        index.addModifyCallback(new ITestOpCallback<Semaphore>() {
            @Override
            public void before(Semaphore t) throws HyracksDataException {
                t.release();
            }

            @Override
            public void after(Semaphore t) throws HyracksDataException {
                // manually set the current memory component as modified
                index.getCurrentMemoryComponent().setModified();
                throw new HyracksDataException("Fail the job");
            }
        });
    }

}
