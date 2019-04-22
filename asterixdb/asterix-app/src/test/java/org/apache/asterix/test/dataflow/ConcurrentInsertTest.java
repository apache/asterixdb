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
import java.util.Arrays;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.bootstrap.TestNodeController.SecondaryIndexInfo;
import org.apache.asterix.app.data.gen.RecordTupleGenerator;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.external.feed.dataflow.SyncFeedRuntimeInputHandler;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
import org.apache.asterix.test.base.TestMethodTracer;
import org.apache.asterix.test.common.TestHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * Two inserters run concurrently to insert data with duplicates (inter/intra-inserter).
 * Each inserter simulates a feed that removes duplicate tuples upon exception.
 *
 */
public class ConcurrentInsertTest {
    private static TestNodeController nc;
    private static TestLsmBtree lsmBtree;
    private static TestLsmBtree secondaryIndex;
    private static TestLsmBtree primaryKeyIndex;
    private static NCAppRuntimeContext ncAppCtx;
    private static IDatasetLifecycleManager dsLifecycleMgr;
    private static IIndexDataflowHelper indexDataflowHelper;
    private static IIndexDataflowHelper secondaryDataflowHelper;
    private static IIndexDataflowHelper primaryKeyIndexDataflowHelper;
    private static final int PARTITION = 0;
    private static LSMPrimaryInsertOperatorNodePushable insertOp1;
    private static LSMPrimaryInsertOperatorNodePushable insertOp2;

    private static int NUM_INSERT_RECORDS = 1000;

    private static IHyracksTaskContext ctx1;
    private static ITransactionContext txnCtx1;

    private static IHyracksTaskContext ctx2;
    private static ITransactionContext txnCtx2;

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
        JobId jobId = nc.newJobId();
        ctx1 = nc.createTestContext(jobId, PARTITION, false);
        txnCtx1 = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(ctx1),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));

        JobId newJobId = nc.newJobId();
        ctx2 = nc.createTestContext(newJobId, PARTITION, false);
        txnCtx2 = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(ctx2),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));

        PrimaryIndexInfo primaryIndexInfo = StorageTestUtils.createPrimaryIndex(nc, PARTITION);
        IndexDataflowHelperFactory iHelperFactory =
                new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
        indexDataflowHelper = iHelperFactory.create(ctx1.getJobletContext().getServiceContext(), PARTITION);
        indexDataflowHelper.open();
        lsmBtree = (TestLsmBtree) indexDataflowHelper.getIndexInstance();
        indexDataflowHelper.close();

        Index secondaryIndexEntity = new Index(StorageTestUtils.DATASET.getDataverseName(),
                StorageTestUtils.DATASET.getDatasetName(), "TestIndex", IndexType.BTREE,
                Arrays.asList(Arrays.asList(StorageTestUtils.RECORD_TYPE.getFieldNames()[1])),
                Arrays.asList(Index.RECORD_INDICATOR), Arrays.asList(BuiltinType.AINT64), false, false, false, 0);

        SecondaryIndexInfo secondaryIndexInfo =
                nc.createSecondaryIndex(primaryIndexInfo, secondaryIndexEntity, StorageTestUtils.STORAGE_MANAGER, 0);
        IndexDataflowHelperFactory secondaryIHelperFactory =
                new IndexDataflowHelperFactory(nc.getStorageManager(), secondaryIndexInfo.getFileSplitProvider());
        secondaryDataflowHelper =
                secondaryIHelperFactory.create(ctx1.getJobletContext().getServiceContext(), PARTITION);
        secondaryDataflowHelper.open();
        secondaryIndex = (TestLsmBtree) secondaryDataflowHelper.getIndexInstance();
        secondaryDataflowHelper.close();

        Index primaryKeyIndexEntity = new Index(StorageTestUtils.DATASET.getDataverseName(),
                StorageTestUtils.DATASET.getDatasetName(), "PrimaryKeyIndex", IndexType.BTREE, Arrays.asList(),
                Arrays.asList(), Arrays.asList(), false, false, false, 0);

        SecondaryIndexInfo primaryKeyIndexInfo =
                nc.createSecondaryIndex(primaryIndexInfo, primaryKeyIndexEntity, StorageTestUtils.STORAGE_MANAGER, 0);
        IndexDataflowHelperFactory primaryKeyIHelperFactory =
                new IndexDataflowHelperFactory(nc.getStorageManager(), primaryKeyIndexInfo.getFileSplitProvider());
        primaryKeyIndexDataflowHelper =
                primaryKeyIHelperFactory.create(ctx1.getJobletContext().getServiceContext(), PARTITION);
        primaryKeyIndexDataflowHelper.open();
        primaryKeyIndex = (TestLsmBtree) secondaryDataflowHelper.getIndexInstance();
        primaryKeyIndexDataflowHelper.close();

        insertOp1 = StorageTestUtils.getInsertPipeline(nc, ctx1, secondaryIndexEntity, primaryKeyIndexEntity);
        insertOp2 = StorageTestUtils.getInsertPipeline(nc, ctx2, secondaryIndexEntity, primaryKeyIndexEntity);
    }

    @After
    public void destroyIndex() throws Exception {
        indexDataflowHelper.destroy();
    }

    @Test
    public void test() throws InterruptedException, HyracksDataException, AlgebricksException {
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    insertRecords(NUM_INSERT_RECORDS, ctx1, insertOp1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    insertRecords(NUM_INSERT_RECORDS, ctx2, insertOp2);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        nc.getTransactionManager().commitTransaction(txnCtx1.getTxnId());
        nc.getTransactionManager().commitTransaction(txnCtx2.getTxnId());

        StorageTestUtils.searchAndAssertCount(nc, 0, NUM_INSERT_RECORDS / 2);
    }

    private ITupleReference insertRecords(int numRecords, IHyracksTaskContext ctx,
            LSMPrimaryInsertOperatorNodePushable insertOp) throws Exception {
        StorageTestUtils.allowAllOps(lsmBtree);
        StorageTestUtils.allowAllOps(primaryKeyIndex);
        StorageTestUtils.allowAllOps(secondaryIndex);
        insertOp.open();
        VSizeFrame frame = new VSizeFrame(ctx);
        FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
        ITupleReference tuple = null;
        RecordTupleGenerator tupleGenerator = StorageTestUtils.getTupleGenerator();
        RecordTupleGenerator backupGenerator = StorageTestUtils.getTupleGenerator();
        SyncFeedRuntimeInputHandler inputHandler =
                new SyncFeedRuntimeInputHandler(ctx, insertOp, new FrameTupleAccessor(new RecordDescriptor(
                        new ISerializerDeserializer[StorageTestUtils.RECORD_TYPE.getFieldTypes().length])));
        for (int i = 0; i < numRecords; i++) {
            if (i % 2 == 0) {
                tuple = tupleGenerator.next();
            } else {
                tuple = backupGenerator.next();
            }
            DataflowUtils.addTupleToFrame(tupleAppender, tuple, inputHandler);
        }
        if (tupleAppender.getTupleCount() > 0) {
            tupleAppender.write(inputHandler, true);
        }
        insertOp.close();
        return tuple;
    }

}
