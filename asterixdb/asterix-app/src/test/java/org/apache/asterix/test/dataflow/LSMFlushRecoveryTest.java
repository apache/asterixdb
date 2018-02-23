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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.bootstrap.TestNodeController.SecondaryIndexInfo;
import org.apache.asterix.app.data.gen.TupleGenerator;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.StorageProperties.Option;
import org.apache.asterix.common.dataflow.LSMInsertDeleteOperatorNodePushable;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.test.common.TestHelper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impl.AllowTestOpCallback;
import org.apache.hyracks.storage.am.lsm.btree.impl.ITestOpCallback;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class LSMFlushRecoveryTest {

    private static TestNodeController nc;
    private static Dataset dataset;
    private static PrimaryIndexInfo primaryIndexInfo;
    private static SecondaryIndexInfo secondaryIndexInfo;
    private static TestLsmBtree primaryIndex;
    private static TestLsmBtree secondaryIndex;
    private static Index secondaryIndexEntity;
    private static NCAppRuntimeContext ncAppCtx;
    private static IDatasetLifecycleManager dsLifecycleMgr;

    private static IHyracksTaskContext ctx;
    private static IIndexDataflowHelper primaryIndexDataflowHelper;
    private static IIndexDataflowHelper secondaryIndexDataflowHelper;
    private static ITransactionContext txnCtx;
    private static LSMInsertDeleteOperatorNodePushable insertOp;
    private static final int PARTITION = 0;
    private static TupleGenerator tupleGenerator;

    private static final String SECONDARY_INDEX_NAME = "TestIdx";
    private static final IndexType SECONDARY_INDEX_TYPE = IndexType.BTREE;
    private static final List<List<String>> SECONDARY_INDEX_FIELD_NAMES =
            Arrays.asList(Arrays.asList(StorageTestUtils.RECORD_TYPE.getFieldNames()[1]));
    private static final List<Integer> SECONDARY_INDEX_FIELD_INDICATORS = Arrays.asList(Index.RECORD_INDICATOR);
    private static final List<IAType> SECONDARY_INDEX_FIELD_TYPES = Arrays.asList(BuiltinType.AINT64);

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("SetUp: ");
        TestHelper.deleteExistingInstanceFiles();
        String configPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "test"
                + File.separator + "resources" + File.separator + "cc.conf";
        nc = new TestNodeController(configPath, false);
    }

    @Before
    public void initializeTest() throws Exception {
        // initialize NC before each test
        initializeNc(true);
        initializeTestCtx();
        createIndex();
        readIndex();
        insertOp = StorageTestUtils.getInsertPipeline(nc, ctx, secondaryIndexEntity);
        tupleGenerator = StorageTestUtils.getTupleGenerator();
    }

    @After
    public void testRecovery() {
        try {
            // right now we've inserted 1000 records to the index, and each record is at least 12 bytes.
            // thus, the memory component size is at least 12KB.
            List<Pair<IOption, Object>> opts = new ArrayList<>();
            opts.add(Pair.of(Option.STORAGE_MEMORYCOMPONENT_GLOBALBUDGET, "128MB"));
            opts.add(Pair.of(Option.STORAGE_MAX_ACTIVE_WRITABLE_DATASETS, "10000"));
            nc.setOpts(opts);
            nc.init(false);
            initializeTestCtx();
            readIndex();
            checkComponentIds();
            insertOp = StorageTestUtils.getInsertPipeline(nc, ctx, secondaryIndexEntity);
            // insert more records
            insertRecords(StorageTestUtils.TOTAL_NUM_OF_RECORDS, StorageTestUtils.RECORDS_PER_COMPONENT);
            checkComponentIds();

            dropIndex();
            // cleanup after each test case
            nc.deInit(true);
            nc.clearOpts();
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void initializeNc(boolean cleanUpOnStart) throws Exception {
        nc.init(cleanUpOnStart);
        ncAppCtx = nc.getAppRuntimeContext();
        dsLifecycleMgr = ncAppCtx.getDatasetLifecycleManager();
    }

    private void createIndex() throws Exception {
        primaryIndexInfo = StorageTestUtils.createPrimaryIndex(nc, PARTITION);
        dataset = primaryIndexInfo.getDataset();
        secondaryIndexEntity = new Index(dataset.getDataverseName(), dataset.getDatasetName(), SECONDARY_INDEX_NAME,
                SECONDARY_INDEX_TYPE, SECONDARY_INDEX_FIELD_NAMES, SECONDARY_INDEX_FIELD_INDICATORS,
                SECONDARY_INDEX_FIELD_TYPES, false, false, false, 0);
        secondaryIndexInfo = nc.createSecondaryIndex(primaryIndexInfo, secondaryIndexEntity,
                StorageTestUtils.STORAGE_MANAGER, PARTITION);
    }

    private void initializeTestCtx() throws Exception {
        JobId jobId = nc.newJobId();
        ctx = nc.createTestContext(jobId, PARTITION, false);
        txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(ctx),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
    }

    private void readIndex() throws HyracksDataException {
        IndexDataflowHelperFactory primaryHelperFactory =
                new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
        primaryIndexDataflowHelper = primaryHelperFactory.create(ctx.getJobletContext().getServiceContext(), PARTITION);
        primaryIndexDataflowHelper.open();
        primaryIndex = (TestLsmBtree) primaryIndexDataflowHelper.getIndexInstance();
        primaryIndexDataflowHelper.close();

        IndexDataflowHelperFactory secodnaryIHelperFactory =
                new IndexDataflowHelperFactory(nc.getStorageManager(), secondaryIndexInfo.getFileSplitProvider());
        secondaryIndexDataflowHelper =
                secodnaryIHelperFactory.create(ctx.getJobletContext().getServiceContext(), PARTITION);
        secondaryIndexDataflowHelper.open();
        secondaryIndex = (TestLsmBtree) secondaryIndexDataflowHelper.getIndexInstance();
        secondaryIndexDataflowHelper.close();
    }

    private void dropIndex() throws HyracksDataException {
        primaryIndexDataflowHelper.destroy();
        secondaryIndexDataflowHelper.destroy();
    }

    @Test
    public void testBothFlushSucceed() throws Exception {
        insertRecords(StorageTestUtils.TOTAL_NUM_OF_RECORDS, StorageTestUtils.RECORDS_PER_COMPONENT);
        // shutdown the server
        nc.deInit(false);
    }

    @Test
    public void testSecondaryFlushFails() throws Exception {
        insertRecords(StorageTestUtils.TOTAL_NUM_OF_RECORDS, StorageTestUtils.RECORDS_PER_COMPONENT);

        primaryIndex.clearFlushCallbacks();
        secondaryIndex.clearFlushCallbacks();

        Semaphore primaryFlushSemaphore = new Semaphore(0);
        secondaryIndex.addFlushCallback(new ITestOpCallback<Semaphore>() {
            @Override
            public void before(Semaphore t) throws HyracksDataException {
                throw new HyracksDataException("Kill the flush thread");
            }

            @Override
            public void after() throws HyracksDataException {

            }
        });

        primaryIndex.addFlushCallback(AllowTestOpCallback.INSTANCE);
        primaryIndex.addIoAfterFinalizeCallback(new ITestOpCallback<Void>() {
            @Override
            public void before(Void t) throws HyracksDataException {

            }

            @Override
            public void after() throws HyracksDataException {
                primaryFlushSemaphore.release();
            }
        });
        StorageTestUtils.flush(dsLifecycleMgr, primaryIndex, true);

        primaryFlushSemaphore.acquire();
        List<ILSMDiskComponent> primaryComponents = primaryIndex.getDiskComponents();
        List<ILSMDiskComponent> secondaryComponents = secondaryIndex.getDiskComponents();
        Assert.assertEquals(primaryComponents.size(), secondaryComponents.size() + 1);
        // shutdown the NC
        nc.deInit(false);
    }

    @Test
    public void testPrimaryFlushFails() throws Exception {
        insertRecords(StorageTestUtils.TOTAL_NUM_OF_RECORDS, StorageTestUtils.RECORDS_PER_COMPONENT);

        primaryIndex.clearFlushCallbacks();
        secondaryIndex.clearFlushCallbacks();

        Semaphore secondaryFlushSemaphore = new Semaphore(0);

        primaryIndex.addFlushCallback(new ITestOpCallback<Semaphore>() {
            @Override
            public void before(Semaphore t) throws HyracksDataException {
                throw new HyracksDataException("Kill the flush thread");
            }

            @Override
            public void after() throws HyracksDataException {

            }
        });

        secondaryIndex.addFlushCallback(AllowTestOpCallback.INSTANCE);
        secondaryIndex.addIoAfterFinalizeCallback(new ITestOpCallback<Void>() {
            @Override
            public void before(Void t) throws HyracksDataException {

            }

            @Override
            public void after() throws HyracksDataException {
                secondaryFlushSemaphore.release();
            }
        });
        StorageTestUtils.flush(dsLifecycleMgr, primaryIndex, true);

        secondaryFlushSemaphore.acquire();
        List<ILSMDiskComponent> primaryComponents = primaryIndex.getDiskComponents();
        List<ILSMDiskComponent> secondaryComponents = secondaryIndex.getDiskComponents();
        Assert.assertEquals(secondaryComponents.size(), primaryComponents.size() + 1);
        // shutdown the NC
        nc.deInit(false);
    }

    @Test
    public void testBothFlushFail() throws Exception {
        insertRecords(StorageTestUtils.TOTAL_NUM_OF_RECORDS, StorageTestUtils.RECORDS_PER_COMPONENT);

        primaryIndex.clearFlushCallbacks();
        secondaryIndex.clearFlushCallbacks();

        Semaphore primaryFlushSemaphore = new Semaphore(0);
        Semaphore secondaryFlushSemaphore = new Semaphore(0);

        primaryIndex.addFlushCallback(new ITestOpCallback<Semaphore>() {
            @Override
            public void before(Semaphore t) throws HyracksDataException {
                primaryFlushSemaphore.release();
                throw new HyracksDataException("Kill the flush thread");
            }

            @Override
            public void after() throws HyracksDataException {

            }
        });

        secondaryIndex.addFlushCallback(new ITestOpCallback<Semaphore>() {
            @Override
            public void before(Semaphore t) throws HyracksDataException {
                secondaryFlushSemaphore.release();
                throw new HyracksDataException("Kill the fluhs thread");
            }

            @Override
            public void after() throws HyracksDataException {

            }
        });
        StorageTestUtils.flush(dsLifecycleMgr, primaryIndex, true);

        primaryFlushSemaphore.acquire();
        secondaryFlushSemaphore.acquire();
        List<ILSMDiskComponent> primaryComponents = primaryIndex.getDiskComponents();
        List<ILSMDiskComponent> secondaryComponents = secondaryIndex.getDiskComponents();
        Assert.assertEquals(secondaryComponents.size(), primaryComponents.size());
        // shutdown the NC
        nc.deInit(false);
    }

    private void insertRecords(int totalNumRecords, int recordsPerComponent) throws Exception {
        StorageTestUtils.allowAllOps(primaryIndex);
        StorageTestUtils.allowAllOps(secondaryIndex);
        insertOp.open();
        VSizeFrame frame = new VSizeFrame(ctx);
        FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
        for (int i = 0; i < totalNumRecords; i++) {
            // flush every RECORDS_PER_COMPONENT records
            if (i % recordsPerComponent == 0 && i + 1 != totalNumRecords) {
                if (tupleAppender.getTupleCount() > 0) {
                    tupleAppender.write(insertOp, true);
                }
                StorageTestUtils.flush(dsLifecycleMgr, primaryIndex, false);
            }
            ITupleReference tuple = tupleGenerator.next();
            DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
        }
        if (tupleAppender.getTupleCount() > 0) {
            tupleAppender.write(insertOp, true);
        }
        insertOp.close();
        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
    }

    private void checkComponentIds() throws HyracksDataException {
        // check memory component
        if (primaryIndex.isMemoryComponentsAllocated()) {
            ILSMMemoryComponent primaryMemComponent = primaryIndex.getCurrentMemoryComponent();
            ILSMMemoryComponent secondaryMemComponent = secondaryIndex.getCurrentMemoryComponent();
            Assert.assertEquals(primaryMemComponent.getId(), secondaryMemComponent.getId());
        }

        List<ILSMDiskComponent> primaryDiskComponents = primaryIndex.getDiskComponents();
        List<ILSMDiskComponent> secondaryDiskComponents = secondaryIndex.getDiskComponents();

        Assert.assertEquals(primaryDiskComponents.size(), secondaryDiskComponents.size());
        for (int i = 0; i < primaryDiskComponents.size(); i++) {
            Assert.assertEquals(primaryDiskComponents.get(i).getId(), secondaryDiskComponents.get(i).getId());
        }
    }

}