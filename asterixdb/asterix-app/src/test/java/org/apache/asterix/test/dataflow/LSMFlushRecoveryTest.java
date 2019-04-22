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
import java.lang.reflect.Field;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.bootstrap.TestNodeController.SecondaryIndexInfo;
import org.apache.asterix.app.data.gen.RecordTupleGenerator;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.StorageProperties.Option;
import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
import org.apache.asterix.test.common.TestHelper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impl.AllowTestOpCallback;
import org.apache.hyracks.storage.am.lsm.btree.impl.ITestOpCallback;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.api.IIoOperationFailedCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AsynchronousScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class LSMFlushRecoveryTest {
    public static final Logger LOGGER = LogManager.getLogger();
    private static TestNodeController nc;
    private static Dataset dataset;
    private static PrimaryIndexInfo[] primaryIndexInfos;
    private static SecondaryIndexInfo[] secondaryIndexInfo;
    private static TestLsmBtree[] primaryIndexes;
    private static TestLsmBtree[] secondaryIndexes;
    private static Index secondaryIndexEntity;
    private static NCAppRuntimeContext ncAppCtx;
    private static IDatasetLifecycleManager dsLifecycleMgr;

    private static IHyracksTaskContext[] testCtxs;
    private static IIndexDataflowHelper[] primaryIndexDataflowHelpers;
    private static IIndexDataflowHelper[] secondaryIndexDataflowHelpers;
    private static ITransactionContext txnCtx;
    private static LSMPrimaryInsertOperatorNodePushable[] insertOps;
    private static RecordTupleGenerator tupleGenerator;

    private static final int NUM_PARTITIONS = 2;
    private static final int PARTITION_0 = 0;
    private static final int PARTITION_1 = 1;

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
        createInsertOps();
        tupleGenerator = StorageTestUtils.getTupleGenerator();
    }

    @After
    public void testRecovery() throws Exception {
        // right now we've inserted 1000 records to the index, and each record is at least 12 bytes.
        // thus, the memory component size is at least 12KB.
        List<Pair<IOption, Object>> opts = new ArrayList<>();
        opts.add(Pair.of(Option.STORAGE_MEMORYCOMPONENT_GLOBALBUDGET, 20 * 1024 * 1024L));
        opts.add(Pair.of(Option.STORAGE_MEMORYCOMPONENT_PAGESIZE, 1 * 1024));
        // each memory component only gets 4 pages (we have 2 partitions, 2 memory components/partition)
        // and some reserved memory for metadata dataset
        opts.add(Pair.of(Option.STORAGE_MAX_ACTIVE_WRITABLE_DATASETS, 1024));
        nc.setOpts(opts);
        initializeNc(false);
        initializeTestCtx();
        readIndex();
        // wait for ongoing flushes triggered by recovery to finish
        DatasetInfo dsInfo = dsLifecycleMgr.getDatasetInfo(dataset.getDatasetId());
        dsInfo.waitForIO();

        checkComponentIds();
        // insert more records
        createInsertOps();
        insertRecords(PARTITION_0, StorageTestUtils.RECORDS_PER_COMPONENT, StorageTestUtils.RECORDS_PER_COMPONENT,
                true);

        dsInfo.waitForIO();
        checkComponentIds();

        dropIndex();
        // cleanup after each test case
        nc.deInit(true);
        nc.clearOpts();
    }

    private void initializeNc(boolean cleanUpOnStart) throws Exception {
        nc.init(cleanUpOnStart);
        ncAppCtx = nc.getAppRuntimeContext();
        // Override the LSMIOScheduler to avoid halting on failure and enable
        // testing failure scenario in a unit test setting
        Field ioScheduler = ncAppCtx.getClass().getDeclaredField("lsmIOScheduler");
        ioScheduler.setAccessible(true);
        ioScheduler.set(ncAppCtx, new AsynchronousScheduler(ncAppCtx.getServiceContext().getThreadFactory(),
                new IIoOperationFailedCallback() {
                    @Override
                    public void schedulerFailed(ILSMIOOperationScheduler scheduler, Throwable failure) {
                        LOGGER.error("Scheduler Failed", failure);
                    }

                    @Override
                    public void operationFailed(ILSMIOOperation operation, Throwable t) {
                        LOGGER.warn("IO Operation failed", t);
                    }
                }));
        dsLifecycleMgr = ncAppCtx.getDatasetLifecycleManager();
    }

    private void createIndex() throws Exception {
        dataset = StorageTestUtils.DATASET;
        secondaryIndexEntity = new Index(dataset.getDataverseName(), dataset.getDatasetName(), SECONDARY_INDEX_NAME,
                SECONDARY_INDEX_TYPE, SECONDARY_INDEX_FIELD_NAMES, SECONDARY_INDEX_FIELD_INDICATORS,
                SECONDARY_INDEX_FIELD_TYPES, false, false, false, 0);

        primaryIndexInfos = new PrimaryIndexInfo[NUM_PARTITIONS];
        secondaryIndexInfo = new SecondaryIndexInfo[NUM_PARTITIONS];
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            primaryIndexInfos[i] = StorageTestUtils.createPrimaryIndex(nc, i);
            secondaryIndexInfo[i] = nc.createSecondaryIndex(primaryIndexInfos[i], secondaryIndexEntity,
                    StorageTestUtils.STORAGE_MANAGER, i);
        }

    }

    private void initializeTestCtx() throws Exception {
        JobId jobId = nc.newJobId();
        testCtxs = new IHyracksTaskContext[NUM_PARTITIONS];
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            testCtxs[i] = nc.createTestContext(jobId, i, false);
        }
        txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(jobId),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
    }

    private void readIndex() throws HyracksDataException {
        primaryIndexDataflowHelpers = new IIndexDataflowHelper[NUM_PARTITIONS];
        primaryIndexes = new TestLsmBtree[NUM_PARTITIONS];
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            IIndexDataflowHelperFactory factory =
                    new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfos[i].getFileSplitProvider());
            primaryIndexDataflowHelpers[i] = factory.create(testCtxs[i].getJobletContext().getServiceContext(), i);
            primaryIndexDataflowHelpers[i].open();
            primaryIndexes[i] = (TestLsmBtree) primaryIndexDataflowHelpers[i].getIndexInstance();
            primaryIndexDataflowHelpers[i].close();
        }

        secondaryIndexDataflowHelpers = new IIndexDataflowHelper[NUM_PARTITIONS];
        secondaryIndexes = new TestLsmBtree[NUM_PARTITIONS];
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            IIndexDataflowHelperFactory factory = new IndexDataflowHelperFactory(nc.getStorageManager(),
                    secondaryIndexInfo[i].getFileSplitProvider());
            secondaryIndexDataflowHelpers[i] = factory.create(testCtxs[i].getJobletContext().getServiceContext(), i);
            secondaryIndexDataflowHelpers[i].open();
            secondaryIndexes[i] = (TestLsmBtree) secondaryIndexDataflowHelpers[i].getIndexInstance();
            secondaryIndexDataflowHelpers[i].close();
        }
    }

    private void createInsertOps() throws HyracksDataException, RemoteException, ACIDException, AlgebricksException {
        insertOps = new LSMPrimaryInsertOperatorNodePushable[NUM_PARTITIONS];
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            insertOps[i] = StorageTestUtils.getInsertPipeline(nc, testCtxs[i], secondaryIndexEntity);
        }
    }

    private void dropIndex() throws HyracksDataException {
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            primaryIndexDataflowHelpers[i].destroy();
            secondaryIndexDataflowHelpers[i].destroy();
        }
    }

    @Test
    public void testBothFlushSucceed() throws Exception {
        insertRecords(PARTITION_0, StorageTestUtils.TOTAL_NUM_OF_RECORDS, StorageTestUtils.RECORDS_PER_COMPONENT, true);
        // shutdown the server
        nc.deInit(false);
    }

    @Test
    public void testSecondaryFlushFails() throws Exception {
        insertRecords(PARTITION_0, StorageTestUtils.TOTAL_NUM_OF_RECORDS, StorageTestUtils.RECORDS_PER_COMPONENT, true);

        primaryIndexes[PARTITION_0].clearFlushCallbacks();
        secondaryIndexes[PARTITION_0].clearFlushCallbacks();

        secondaryIndexes[PARTITION_0].addFlushCallback(new ITestOpCallback<Semaphore>() {
            @Override
            public void before(Semaphore t) throws HyracksDataException {
                throw new HyracksDataException("Kill the flush thread");
            }

            @Override
            public void after(Semaphore t) throws HyracksDataException {

            }
        });

        Semaphore primaryFlushSemaphore = new Semaphore(0);
        primaryIndexes[PARTITION_0].addFlushCallback(AllowTestOpCallback.INSTANCE);
        primaryIndexes[PARTITION_0].addIoCompletedCallback(new ITestOpCallback<Void>() {
            @Override
            public void before(Void t) throws HyracksDataException {

            }

            @Override
            public void after(Void t) throws HyracksDataException {
                primaryFlushSemaphore.release();
            }
        });
        StorageTestUtils.flush(dsLifecycleMgr, primaryIndexes[PARTITION_0], true);

        primaryFlushSemaphore.acquire();
        List<ILSMDiskComponent> primaryComponents = primaryIndexes[PARTITION_0].getDiskComponents();
        List<ILSMDiskComponent> secondaryComponents = secondaryIndexes[PARTITION_0].getDiskComponents();
        Assert.assertEquals(primaryComponents.size(), secondaryComponents.size() + 1);
        // shutdown the NC
        nc.deInit(false);
    }

    @Test
    public void testPrimaryFlushFails() throws Exception {
        insertRecords(PARTITION_0, StorageTestUtils.TOTAL_NUM_OF_RECORDS, StorageTestUtils.RECORDS_PER_COMPONENT, true);

        primaryIndexes[PARTITION_0].clearFlushCallbacks();
        secondaryIndexes[PARTITION_0].clearFlushCallbacks();

        primaryIndexes[PARTITION_0].addFlushCallback(new ITestOpCallback<Semaphore>() {
            @Override
            public void before(Semaphore t) throws HyracksDataException {
                throw new HyracksDataException("Kill the flush thread");
            }

            @Override
            public void after(Semaphore t) throws HyracksDataException {

            }
        });

        Semaphore secondaryFlushSemaphore = new Semaphore(0);
        secondaryIndexes[PARTITION_0].addFlushCallback(AllowTestOpCallback.INSTANCE);
        secondaryIndexes[PARTITION_0].addIoCompletedCallback(new ITestOpCallback<Void>() {
            @Override
            public void before(Void t) throws HyracksDataException {

            }

            @Override
            public void after(Void t) throws HyracksDataException {
                secondaryFlushSemaphore.release();
            }
        });
        StorageTestUtils.flush(dsLifecycleMgr, primaryIndexes[PARTITION_0], true);

        secondaryFlushSemaphore.acquire();
        List<ILSMDiskComponent> primaryComponents = primaryIndexes[PARTITION_0].getDiskComponents();
        List<ILSMDiskComponent> secondaryComponents = secondaryIndexes[PARTITION_0].getDiskComponents();
        Assert.assertEquals(secondaryComponents.size(), primaryComponents.size() + 1);
        // shutdown the NC
        nc.deInit(false);
    }

    @Test
    public void testMultiPartition() throws Exception {
        // insert records to partition 0
        insertRecords(PARTITION_0, StorageTestUtils.TOTAL_NUM_OF_RECORDS, StorageTestUtils.RECORDS_PER_COMPONENT,
                false);
        // insert records to partition 1
        insertRecords(PARTITION_1, StorageTestUtils.TOTAL_NUM_OF_RECORDS, StorageTestUtils.RECORDS_PER_COMPONENT, true);
        StorageTestUtils.waitForOperations(primaryIndexes[PARTITION_0]);
        StorageTestUtils.waitForOperations(primaryIndexes[PARTITION_1]);

        // now both partitions have some extra records in memory component
        Assert.assertTrue(primaryIndexes[PARTITION_0].getCurrentMemoryComponent().isModified());
        Assert.assertTrue(primaryIndexes[PARTITION_1].getCurrentMemoryComponent().isModified());

        primaryIndexes[PARTITION_0].clearFlushCallbacks();
        secondaryIndexes[PARTITION_0].clearFlushCallbacks();

        primaryIndexes[PARTITION_0].addFlushCallback(new ITestOpCallback<Semaphore>() {
            @Override
            public void before(Semaphore t) throws HyracksDataException {
                throw new HyracksDataException("Kill the flush thread");
            }

            @Override
            public void after(Semaphore t) throws HyracksDataException {

            }
        });

        Semaphore flushSemaphore = new Semaphore(0);
        secondaryIndexes[PARTITION_0].addFlushCallback(AllowTestOpCallback.INSTANCE);
        secondaryIndexes[PARTITION_0].addIoCompletedCallback(new ITestOpCallback<Void>() {
            @Override
            public void before(Void t) throws HyracksDataException {

            }

            @Override
            public void after(Void t) throws HyracksDataException {
                flushSemaphore.release();
            }
        });

        // only flush the partition 0
        StorageTestUtils.flushPartition(dsLifecycleMgr, primaryIndexes[PARTITION_0], true);
        flushSemaphore.acquire(1);

        List<ILSMDiskComponent> primaryComponents = primaryIndexes[PARTITION_0].getDiskComponents();
        List<ILSMDiskComponent> secondaryComponents = secondaryIndexes[PARTITION_0].getDiskComponents();
        Assert.assertEquals(secondaryComponents.size(), primaryComponents.size() + 1);

        Assert.assertEquals(secondaryIndexes[PARTITION_1].getDiskComponents().size(),
                primaryIndexes[PARTITION_1].getDiskComponents().size());
        // shutdown the NC
        // upon recovery, it's expected that the FLUSH record on partition 0 does not affect partition 1
        nc.deInit(false);
    }

    @Test
    public void testBothFlushFail() throws Exception {
        insertRecords(PARTITION_0, StorageTestUtils.TOTAL_NUM_OF_RECORDS, StorageTestUtils.RECORDS_PER_COMPONENT, true);

        primaryIndexes[PARTITION_0].clearFlushCallbacks();
        secondaryIndexes[PARTITION_0].clearFlushCallbacks();

        Semaphore primaryFlushSemaphore = new Semaphore(0);
        Semaphore secondaryFlushSemaphore = new Semaphore(0);

        primaryIndexes[PARTITION_0].addFlushCallback(new ITestOpCallback<Semaphore>() {
            @Override
            public void before(Semaphore t) throws HyracksDataException {
                primaryFlushSemaphore.release();
                throw new HyracksDataException("Kill the flush thread");
            }

            @Override
            public void after(Semaphore t) throws HyracksDataException {

            }
        });

        secondaryIndexes[PARTITION_0].addFlushCallback(new ITestOpCallback<Semaphore>() {
            @Override
            public void before(Semaphore t) throws HyracksDataException {
                secondaryFlushSemaphore.release();
                throw new HyracksDataException("Kill the fluhs thread");
            }

            @Override
            public void after(Semaphore t) throws HyracksDataException {

            }
        });
        StorageTestUtils.flush(dsLifecycleMgr, primaryIndexes[PARTITION_0], true);

        primaryFlushSemaphore.acquire();
        secondaryFlushSemaphore.acquire();
        List<ILSMDiskComponent> primaryComponents = primaryIndexes[PARTITION_0].getDiskComponents();
        List<ILSMDiskComponent> secondaryComponents = secondaryIndexes[PARTITION_0].getDiskComponents();
        Assert.assertEquals(secondaryComponents.size(), primaryComponents.size());
        // shutdown the NC
        nc.deInit(false);
    }

    private void insertRecords(int partition, int totalNumRecords, int recordsPerComponent, boolean commit)
            throws Exception {
        StorageTestUtils.allowAllOps(primaryIndexes[partition]);
        StorageTestUtils.allowAllOps(secondaryIndexes[partition]);
        insertOps[partition].open();
        VSizeFrame frame = new VSizeFrame(testCtxs[partition]);
        FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
        for (int i = 0; i < totalNumRecords; i++) {
            // flush every RECORDS_PER_COMPONENT records
            if (i % recordsPerComponent == 0 && i != 0 && i + 1 != totalNumRecords) {
                if (tupleAppender.getTupleCount() > 0) {
                    tupleAppender.write(insertOps[partition], true);
                }
                StorageTestUtils.flushPartition(dsLifecycleMgr, primaryIndexes[partition], false);
            }
            ITupleReference tuple = tupleGenerator.next();
            DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOps[partition]);
        }
        if (tupleAppender.getTupleCount() > 0) {
            tupleAppender.write(insertOps[partition], true);
        }
        insertOps[partition].close();
        if (commit) {
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
        }
    }

    private void checkComponentIds() throws HyracksDataException {
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            checkComponentIds(i);
        }
    }

    private void checkComponentIds(int partitionIndex) throws HyracksDataException {
        // check memory component
        if (primaryIndexes[partitionIndex].isMemoryComponentsAllocated()) {
            ILSMMemoryComponent primaryMemComponent = primaryIndexes[partitionIndex].getCurrentMemoryComponent();
            ILSMMemoryComponent secondaryMemComponent = secondaryIndexes[partitionIndex].getCurrentMemoryComponent();
            Assert.assertEquals(primaryMemComponent.getId(), secondaryMemComponent.getId());
            Assert.assertEquals(primaryIndexes[partitionIndex].getCurrentMemoryComponentIndex(),
                    secondaryIndexes[partitionIndex].getCurrentMemoryComponentIndex());
        }

        List<ILSMDiskComponent> primaryDiskComponents = primaryIndexes[partitionIndex].getDiskComponents();
        List<ILSMDiskComponent> secondaryDiskComponents = secondaryIndexes[partitionIndex].getDiskComponents();

        Assert.assertEquals(primaryDiskComponents.size(), secondaryDiskComponents.size());
        for (int i = 0; i < primaryDiskComponents.size(); i++) {
            Assert.assertEquals(primaryDiskComponents.get(i).getId(), secondaryDiskComponents.get(i).getId());
        }
    }

}
