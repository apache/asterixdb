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

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.data.gen.RecordTupleGenerator;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class IoCallbackFailureTest {

    private static final int PARTITION = 0;
    private static TestNodeController nc;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("SetUp: ");
        TestHelper.deleteExistingInstanceFiles();
        String configPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "test"
                + File.separator + "resources" + File.separator + "cc.conf";
        nc = new TestNodeController(configPath, false);
        nc.init();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.out.println("TearDown");
        nc.deInit();
        TestHelper.deleteExistingInstanceFiles();
    }

    @Test
    public void testTempFailureInAllocateCallback() throws Exception {
        PrimaryIndexInfo primaryIndexInfo = StorageTestUtils.createPrimaryIndex(nc, PARTITION);
        IndexDataflowHelperFactory iHelperFactory =
                new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
        JobId jobId = nc.newJobId();
        IHyracksTaskContext ctx = nc.createTestContext(jobId, PARTITION, false);
        IIndexDataflowHelper indexDataflowHelper =
                iHelperFactory.create(ctx.getJobletContext().getServiceContext(), PARTITION);
        indexDataflowHelper.open();
        TestLsmBtree lsmBtree = (TestLsmBtree) indexDataflowHelper.getIndexInstance();
        indexDataflowHelper.close();
        LSMPrimaryInsertOperatorNodePushable insertOp = StorageTestUtils.getInsertPipeline(nc, ctx);
        StorageTestUtils.allowAllOps(lsmBtree);
        ITestOpCallback<ILSMMemoryComponent> failCallback = new ITestOpCallback<ILSMMemoryComponent>() {
            @SuppressWarnings("deprecation")
            @Override
            public void before(ILSMMemoryComponent c) throws HyracksDataException {
                throw new HyracksDataException("Fail on allocate callback");
            }

            @Override
            public void after(ILSMMemoryComponent c) throws HyracksDataException {
                // No Op
            }
        };
        lsmBtree.addIoAllocateCallback(failCallback);
        boolean expectedExceptionThrown = false;
        try {
            insert(nc, lsmBtree, ctx, insertOp, StorageTestUtils.TOTAL_NUM_OF_RECORDS,
                    StorageTestUtils.RECORDS_PER_COMPONENT);
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        Assert.assertTrue(expectedExceptionThrown);
        // Clear the callback and retry
        lsmBtree.clearIoAllocateCallback();
        jobId = nc.newJobId();
        ctx = nc.createTestContext(jobId, PARTITION, false);
        insertOp = StorageTestUtils.getInsertPipeline(nc, ctx);
        insert(nc, lsmBtree, ctx, insertOp, StorageTestUtils.TOTAL_NUM_OF_RECORDS,
                StorageTestUtils.RECORDS_PER_COMPONENT);
    }

    private static void insert(TestNodeController nc, TestLsmBtree lsmBtree, IHyracksTaskContext ctx,
            LSMPrimaryInsertOperatorNodePushable insertOp, int totalNumRecords, int recordsPerComponent)
            throws Exception {
        NCAppRuntimeContext ncAppCtx = nc.getAppRuntimeContext();
        IDatasetLifecycleManager dsLifecycleMgr = ncAppCtx.getDatasetLifecycleManager();
        RecordTupleGenerator tupleGenerator = StorageTestUtils.getTupleGenerator();
        ITransactionContext txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(ctx),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
        boolean failed = false;
        try {
            try {
                insertOp.open();
                VSizeFrame frame = new VSizeFrame(ctx);
                FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
                for (int j = 0; j < totalNumRecords; j++) {
                    // flush every recordsPerComponent records
                    if (j % recordsPerComponent == 0 && j + 1 != totalNumRecords) {
                        if (tupleAppender.getTupleCount() > 0) {
                            tupleAppender.write(insertOp, true);
                        }
                        StorageTestUtils.flush(dsLifecycleMgr, lsmBtree, false);
                    }
                    ITupleReference tuple = tupleGenerator.next();
                    DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
                }
                if (tupleAppender.getTupleCount() > 0) {
                    tupleAppender.write(insertOp, true);
                }
            } catch (Throwable th) {
                failed = true;
                insertOp.fail();
                throw th;
            } finally {
                insertOp.close();
            }
        } finally {
            if (failed) {
                nc.getTransactionManager().abortTransaction(txnCtx.getTxnId());
            } else {
                nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            }
        }
    }
}
