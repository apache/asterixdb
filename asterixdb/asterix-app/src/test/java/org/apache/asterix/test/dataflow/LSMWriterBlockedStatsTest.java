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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.asterix.test.dataflow.StorageTestUtils.Flusher;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMWriterBlockedStats;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMWriterBlockedStats.Reason;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Checks that a writer blocked on a full memory component is counted, and timed, under the right reason.
 */
public class LSMWriterBlockedStatsTest {

    private static final int PARTITION = 0;
    private static final long BLOCKED_MILLIS = 500;
    private static TestNodeController nc;
    private static IDatasetLifecycleManager dsLifecycleMgr;
    private IHyracksTaskContext ctx;
    private IIndexDataflowHelper indexDataflowHelper;
    private TestLsmBtree lsmBtree;
    private ITransactionContext txnCtx;
    private LSMPrimaryInsertOperatorNodePushable insertOp;

    @BeforeClass
    public static void setUp() throws Exception {
        TestHelper.deleteExistingInstanceFiles();
        String configPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "test"
                + File.separator + "resources" + File.separator + "cc.conf";
        nc = new TestNodeController(configPath, false);
        nc.init();
        NCAppRuntimeContext ncAppCtx = nc.getAppRuntimeContext();
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
        insertOp = StorageTestUtils.getInsertPipeline(nc, ctx);
    }

    @After
    public void destroyIndex() throws Exception {
        indexDataflowHelper.destroy();
    }

    @Test
    public void writerBlockedByFlushingMemoryComponents() throws Exception {
        StorageTestUtils.allowAllOps(lsmBtree);
        // hold every flush until released
        lsmBtree.clearFlushCallbacks();
        Flusher flusher = new Flusher(lsmBtree);
        insertOp.open();
        RecordTupleGenerator tupleGenerator = StorageTestUtils.getTupleGenerator();
        FrameTupleAppender tupleAppender = new FrameTupleAppender(new VSizeFrame(ctx));

        // fill the first memory component and start flushing it; the flush holds it
        insert(tupleGenerator, tupleAppender, StorageTestUtils.RECORDS_PER_COMPONENT);
        StorageTestUtils.flush(dsLifecycleMgr, lsmBtree, true);
        flusher.waitUntilCount(1);
        // fill the second and schedule its flush, which leaves no writable memory component
        insert(tupleGenerator, tupleAppender, StorageTestUtils.RECORDS_PER_COMPONENT);
        StorageTestUtils.flush(dsLifecycleMgr, lsmBtree, true);

        long nanosBefore = LSMWriterBlockedStats.getBlockedNanos(Reason.MEMORY_COMPONENT);
        long countBefore = LSMWriterBlockedStats.getBlockedCount(Reason.MEMORY_COMPONENT);
        long budgetCountBefore = LSMWriterBlockedStats.getBlockedCount(Reason.MEMORY_BUDGET);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                insert(tupleGenerator, tupleAppender, 1);
            } catch (Throwable th) {
                failure.set(th);
            }
        }, "blocked-writer");
        writer.start();
        writer.join(BLOCKED_MILLIS);
        Assert.assertTrue("the writer should be blocked while both memory components flush", writer.isAlive());

        lsmBtree.allowFlush(2);
        writer.join(TimeUnit.MINUTES.toMillis(1));
        Assert.assertFalse("the writer should complete once the flushes do", writer.isAlive());
        Assert.assertNull(failure.get());
        insertOp.close();
        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());

        Assert.assertTrue(LSMWriterBlockedStats.getBlockedCount(Reason.MEMORY_COMPONENT) > countBefore);
        long blockedNanos = LSMWriterBlockedStats.getBlockedNanos(Reason.MEMORY_COMPONENT) - nanosBefore;
        Assert.assertTrue("blocked for " + blockedNanos + "ns",
                blockedNanos >= TimeUnit.MILLISECONDS.toNanos(BLOCKED_MILLIS));
        Assert.assertEquals(budgetCountBefore, LSMWriterBlockedStats.getBlockedCount(Reason.MEMORY_BUDGET));
        StorageTestUtils.searchAndAssertCount(nc, PARTITION, 2 * StorageTestUtils.RECORDS_PER_COMPONENT + 1);
    }

    private void insert(RecordTupleGenerator tupleGenerator, FrameTupleAppender tupleAppender, int records)
            throws Exception {
        for (int i = 0; i < records; i++) {
            DataflowUtils.addTupleToFrame(tupleAppender, tupleGenerator.next(), insertOp);
        }
        if (tupleAppender.getTupleCount() > 0) {
            tupleAppender.write(insertOp, true);
        }
    }
}
