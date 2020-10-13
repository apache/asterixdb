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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.data.gen.RecordTupleGenerator;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.StorageProperties.Option;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
import org.apache.asterix.test.common.TestHelper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GlobalVirtualBufferCacheTest {
    public static final Logger LOGGER = LogManager.getLogger();
    private static TestNodeController nc;
    private static Dataset dataset;
    private static Dataset filteredDataset;
    private static PrimaryIndexInfo[] primaryIndexInfos;
    private static PrimaryIndexInfo[] filteredPrimaryIndexInfos;
    private static TestLsmBtree[] primaryIndexes;
    private static TestLsmBtree[] filteredPrimaryIndexes;
    private static NCAppRuntimeContext ncAppCtx;
    private static IDatasetLifecycleManager dsLifecycleMgr;

    private static IHyracksTaskContext[] testCtxs;
    private static IHyracksTaskContext[] filteredTestCtxs;
    private static IIndexDataflowHelper[] primaryIndexDataflowHelpers;
    private static IIndexDataflowHelper[] filteredPrimaryIndexDataflowHelpers;
    private static ITransactionContext txnCtx;
    private static ITransactionContext filteredTxnCtx;
    private static RecordTupleGenerator tupleGenerator;

    private static final int NUM_PARTITIONS = 2;
    private static final long FILTERED_MEMORY_COMPONENT_SIZE = 16 * 1024l;

    @BeforeClass
    public static void setUp() {
        try {
            System.out.println("SetUp: ");
            TestHelper.deleteExistingInstanceFiles();
            String configPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "test"
                    + File.separator + "resources" + File.separator + "cc.conf";
            nc = new TestNodeController(configPath, false);
        } catch (Throwable e) {
            LOGGER.error(e);
            Assert.fail(e.getMessage());
        }
    }

    @Before
    public void initializeTest() {
        // initialize NC before each test
        try {
            initializeNc();
            initializeTestCtx();
            createIndex();
            readIndex();
            tupleGenerator = StorageTestUtils.getTupleGenerator();
        } catch (Throwable e) {
            LOGGER.error(e);
            Assert.fail(e.getMessage());
        }
    }

    @After
    public void deinitializeTest() {
        try {
            dropIndex();
            // cleanup after each test case
            nc.deInit(true);
            nc.clearOpts();
        } catch (Throwable e) {
            LOGGER.error(e);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFlushes() {
        try {
            List<Thread> threads = new ArrayList<>();
            int records = 16 * 1024;
            int threadsPerPartition = 2;
            AtomicReference<Exception> exceptionRef = new AtomicReference<>();
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                for (int t = 0; t < threadsPerPartition; t++) {
                    threads.add(insertRecords(records, p, false, exceptionRef));
                    threads.add(insertRecords(records, p, true, exceptionRef));
                }
            }
            for (Thread thread : threads) {
                thread.join();
            }
            if (exceptionRef.get() != null) {
                exceptionRef.get().printStackTrace();
                Assert.fail();
            }
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                List<ILSMDiskComponent> diskComponents = new ArrayList<>(primaryIndexes[i].getDiskComponents());
                Assert.assertFalse(diskComponents.isEmpty());
                Assert.assertTrue(diskComponents.stream().anyMatch(c -> ((AbstractTreeIndex) c.getIndex())
                        .getFileReference().getFile().length() > FILTERED_MEMORY_COMPONENT_SIZE));

                List<ILSMDiskComponent> filteredDiskComponents =
                        new ArrayList<>(filteredPrimaryIndexes[i].getDiskComponents());
                Assert.assertFalse(filteredDiskComponents.isEmpty());
                Assert.assertTrue(filteredDiskComponents.stream().allMatch(c -> ((AbstractTreeIndex) c.getIndex())
                        .getFileReference().getFile().length() <= FILTERED_MEMORY_COMPONENT_SIZE));
            }

            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            nc.getTransactionManager().commitTransaction(filteredTxnCtx.getTxnId());
        } catch (Throwable e) {
            LOGGER.error("testFlushes failed", e);
            Assert.fail(e.getMessage());
        }
    }

    private void initializeNc() throws Exception {
        List<Pair<IOption, Object>> opts = new ArrayList<>();
        opts.add(Pair.of(Option.STORAGE_MEMORYCOMPONENT_GLOBALBUDGET, 128 * 1024L));
        opts.add(Pair.of(Option.STORAGE_MEMORYCOMPONENT_PAGESIZE, 1 * 1024));
        opts.add(Pair.of(Option.STORAGE_BUFFERCACHE_PAGESIZE, 1 * 1024));
        opts.add(Pair.of(Option.STORAGE_FILTERED_MEMORYCOMPONENT_MAX_SIZE, FILTERED_MEMORY_COMPONENT_SIZE));
        opts.add(Pair.of(Option.STORAGE_FILTERED_MEMORYCOMPONENT_MAX_SIZE, FILTERED_MEMORY_COMPONENT_SIZE));

        nc.setOpts(opts);

        nc.init(true);
        ncAppCtx = nc.getAppRuntimeContext();
        dsLifecycleMgr = ncAppCtx.getDatasetLifecycleManager();
    }

    private void createIndex() throws Exception {
        dataset = new TestDataset(StorageTestUtils.DATAVERSE_NAME, "ds", StorageTestUtils.DATAVERSE_NAME,
                StorageTestUtils.DATA_TYPE_NAME, StorageTestUtils.NODE_GROUP_NAME, NoMergePolicyFactory.NAME,
                null, new InternalDatasetDetails(null, PartitioningStrategy.HASH, StorageTestUtils.PARTITIONING_KEYS,
                        null, null, null, false, null, null),
                null, DatasetType.INTERNAL, StorageTestUtils.DATASET_ID, 0);

        filteredDataset = new TestDataset(StorageTestUtils.DATAVERSE_NAME, "filtered_ds",
                StorageTestUtils.DATAVERSE_NAME, StorageTestUtils.DATA_TYPE_NAME, StorageTestUtils.NODE_GROUP_NAME,
                NoMergePolicyFactory.NAME, null,
                new InternalDatasetDetails(null, PartitioningStrategy.HASH, StorageTestUtils.PARTITIONING_KEYS, null,
                        null, null, false, 0, Collections.singletonList("value")),
                null, DatasetType.INTERNAL, StorageTestUtils.DATASET_ID + 1, 0);

        primaryIndexInfos = new PrimaryIndexInfo[NUM_PARTITIONS];
        filteredPrimaryIndexInfos = new PrimaryIndexInfo[NUM_PARTITIONS];
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            primaryIndexInfos[i] = StorageTestUtils.createPrimaryIndex(nc, dataset, i);
            filteredPrimaryIndexInfos[i] = StorageTestUtils.createPrimaryIndex(nc, filteredDataset, i);
        }
    }

    private void initializeTestCtx() throws Exception {
        JobId jobId = nc.newJobId();
        JobId filteredJobId = nc.newJobId();

        testCtxs = new IHyracksTaskContext[NUM_PARTITIONS];
        filteredTestCtxs = new IHyracksTaskContext[NUM_PARTITIONS];

        for (int i = 0; i < NUM_PARTITIONS; i++) {
            testCtxs[i] = nc.createTestContext(jobId, i, false);
            filteredTestCtxs[i] = nc.createTestContext(filteredJobId, i, false);
        }
        txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(jobId),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
        filteredTxnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(filteredJobId),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
    }

    private void readIndex() throws HyracksDataException {
        primaryIndexDataflowHelpers = new IIndexDataflowHelper[NUM_PARTITIONS];
        primaryIndexes = new TestLsmBtree[NUM_PARTITIONS];

        filteredPrimaryIndexDataflowHelpers = new IIndexDataflowHelper[NUM_PARTITIONS];
        filteredPrimaryIndexes = new TestLsmBtree[NUM_PARTITIONS];

        for (int i = 0; i < NUM_PARTITIONS; i++) {
            IIndexDataflowHelperFactory factory =
                    new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfos[i].getFileSplitProvider());
            primaryIndexDataflowHelpers[i] = factory.create(testCtxs[i].getJobletContext().getServiceContext(), i);
            primaryIndexDataflowHelpers[i].open();
            primaryIndexes[i] = (TestLsmBtree) primaryIndexDataflowHelpers[i].getIndexInstance();
            primaryIndexDataflowHelpers[i].close();

            IIndexDataflowHelperFactory filteredFactory = new IndexDataflowHelperFactory(nc.getStorageManager(),
                    filteredPrimaryIndexInfos[i].getFileSplitProvider());
            filteredPrimaryIndexDataflowHelpers[i] =
                    filteredFactory.create(filteredTestCtxs[i].getJobletContext().getServiceContext(), i);
            filteredPrimaryIndexDataflowHelpers[i].open();
            filteredPrimaryIndexes[i] = (TestLsmBtree) filteredPrimaryIndexDataflowHelpers[i].getIndexInstance();
            filteredPrimaryIndexDataflowHelpers[i].close();
        }
    }

    private void dropIndex() throws HyracksDataException {
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            primaryIndexDataflowHelpers[i].destroy();
            filteredPrimaryIndexDataflowHelpers[i].destroy();
        }
    }

    private Thread insertRecords(int records, int partition, boolean filtered, AtomicReference<Exception> exceptionRef)
            throws Exception {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    LSMPrimaryInsertOperatorNodePushable insertOp = filtered ? nc
                            .getInsertPipeline(filteredTestCtxs[partition], filteredDataset, StorageTestUtils.KEY_TYPES,
                                    StorageTestUtils.RECORD_TYPE, StorageTestUtils.META_TYPE,
                                    filteredPrimaryIndexes[partition].getFilterFields(), StorageTestUtils.KEY_INDEXES,
                                    StorageTestUtils.KEY_INDICATORS_LIST, StorageTestUtils.STORAGE_MANAGER, null, null)
                            .getLeft()
                            : nc.getInsertPipeline(testCtxs[partition], dataset, StorageTestUtils.KEY_TYPES,
                                    StorageTestUtils.RECORD_TYPE, StorageTestUtils.META_TYPE, null,
                                    StorageTestUtils.KEY_INDEXES, StorageTestUtils.KEY_INDICATORS_LIST,
                                    StorageTestUtils.STORAGE_MANAGER, null, null).getLeft();
                    insertOp.open();
                    FrameTupleAppender tupleAppender =
                            new FrameTupleAppender(new FixedSizeFrame(ByteBuffer.allocate(512)));

                    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(filtered ? 3 : 2);
                    ArrayTupleReference tupleRef = new ArrayTupleReference();
                    for (int i = 0; i < records; i++) {
                        synchronized (tupleGenerator) {
                            ITupleReference tuple = tupleGenerator.next();
                            TupleUtils.copyTuple(tupleBuilder, tuple, 2);
                            if (filtered) {
                                // append the filter field
                                tupleBuilder.getDataOutput().writeByte(ATypeTag.SERIALIZED_INT64_TYPE_TAG);
                                tupleBuilder.getDataOutput().writeLong(0l);
                                tupleBuilder.addFieldEndOffset();
                            }
                            tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
                        }
                        DataflowUtils.addTupleToFrame(tupleAppender, tupleRef, insertOp);
                    }
                    tupleAppender.write(insertOp, true);
                    insertOp.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    exceptionRef.compareAndSet(null, e);
                }
            }
        });
        thread.start();
        return thread;
    }

}
