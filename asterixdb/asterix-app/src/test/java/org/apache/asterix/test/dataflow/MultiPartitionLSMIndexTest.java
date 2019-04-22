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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.bootstrap.TestNodeController.SecondaryIndexInfo;
import org.apache.asterix.app.data.gen.RecordTupleGenerator;
import org.apache.asterix.app.data.gen.RecordTupleGenerator.GenerationFunction;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
import org.apache.asterix.test.common.TestHelper;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.util.SingleThreadEventProcessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impl.ITestOpCallback;
import org.apache.hyracks.storage.am.lsm.btree.impl.IVirtualBufferCacheCallback;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MultiPartitionLSMIndexTest {
    static final int REPREAT_TEST_COUNT = 1;

    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Arrays.asList(new Object[REPREAT_TEST_COUNT][0]);
    }

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
    private static final int TOTAL_NUM_OF_RECORDS = 5000;
    private static final int RECORDS_PER_COMPONENT = 500;
    private static final int DATASET_ID = 101;
    private static final String DATAVERSE_NAME = "TestDV";
    private static final String DATASET_NAME = "TestDS";
    private static final String INDEX_NAME = "TestIdx";
    private static final String DATA_TYPE_NAME = "DUMMY";
    private static final String NODE_GROUP_NAME = "DEFAULT";
    private static final IndexType INDEX_TYPE = IndexType.BTREE;
    private static final List<List<String>> INDEX_FIELD_NAMES =
            Arrays.asList(Arrays.asList(RECORD_TYPE.getFieldNames()[1]));
    private static final List<Integer> INDEX_FIELD_INDICATORS = Arrays.asList(Index.RECORD_INDICATOR);
    private static final List<IAType> INDEX_FIELD_TYPES = Arrays.asList(BuiltinType.AINT64);
    private static final StorageComponentProvider storageManager = new StorageComponentProvider();
    private static final int NUM_PARTITIONS = 2;
    private static TestNodeController nc;
    private static NCAppRuntimeContext ncAppCtx;
    private static IDatasetLifecycleManager dsLifecycleMgr;
    private static Dataset dataset;
    private static Index secondaryIndex;
    private static ITransactionContext txnCtx;
    private static TestLsmBtree[] primaryLsmBtrees;
    private static TestLsmBtree[] secondaryLsmBtrees;
    private static IHyracksTaskContext[] taskCtxs;
    private static IIndexDataflowHelper[] primaryIndexDataflowHelpers;
    private static IIndexDataflowHelper[] secondaryIndexDataflowHelpers;
    private static LSMPrimaryInsertOperatorNodePushable[] insertOps;
    private static Actor[] actors;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("SetUp: ");
        TestHelper.deleteExistingInstanceFiles();
        String configPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "test"
                + File.separator + "resources" + File.separator + "cc-multipart.conf";
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
        List<List<String>> partitioningKeys = new ArrayList<>();
        partitioningKeys.add(Collections.singletonList("key"));
        dataset = new TestDataset(DATAVERSE_NAME, DATASET_NAME, DATAVERSE_NAME, DATA_TYPE_NAME,
                NODE_GROUP_NAME, NoMergePolicyFactory.NAME, null, new InternalDatasetDetails(null,
                        PartitioningStrategy.HASH, partitioningKeys, null, null, null, false, null),
                null, DatasetType.INTERNAL, DATASET_ID, 0);
        secondaryIndex = new Index(DATAVERSE_NAME, DATASET_NAME, INDEX_NAME, INDEX_TYPE, INDEX_FIELD_NAMES,
                INDEX_FIELD_INDICATORS, INDEX_FIELD_TYPES, false, false, false, 0);
        taskCtxs = new IHyracksTaskContext[NUM_PARTITIONS];
        primaryIndexDataflowHelpers = new IIndexDataflowHelper[NUM_PARTITIONS];
        secondaryIndexDataflowHelpers = new IIndexDataflowHelper[NUM_PARTITIONS];
        primaryLsmBtrees = new TestLsmBtree[NUM_PARTITIONS];
        secondaryLsmBtrees = new TestLsmBtree[NUM_PARTITIONS];
        insertOps = new LSMPrimaryInsertOperatorNodePushable[NUM_PARTITIONS];
        JobId jobId = nc.newJobId();
        txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(jobId),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
        actors = new Actor[NUM_PARTITIONS];
        for (int i = 0; i < taskCtxs.length; i++) {
            taskCtxs[i] = nc.createTestContext(jobId, i, false);
            PrimaryIndexInfo primaryIndexInfo = nc.createPrimaryIndex(dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null,
                    storageManager, KEY_INDEXES, KEY_INDICATORS_LIST, i);
            SecondaryIndexInfo secondaryIndexInfo =
                    nc.createSecondaryIndex(primaryIndexInfo, secondaryIndex, storageManager, i);
            IndexDataflowHelperFactory iHelperFactory =
                    new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
            primaryIndexDataflowHelpers[i] =
                    iHelperFactory.create(taskCtxs[i].getJobletContext().getServiceContext(), i);
            primaryIndexDataflowHelpers[i].open();
            primaryLsmBtrees[i] = (TestLsmBtree) primaryIndexDataflowHelpers[i].getIndexInstance();
            iHelperFactory =
                    new IndexDataflowHelperFactory(nc.getStorageManager(), secondaryIndexInfo.getFileSplitProvider());
            secondaryIndexDataflowHelpers[i] =
                    iHelperFactory.create(taskCtxs[i].getJobletContext().getServiceContext(), i);
            secondaryIndexDataflowHelpers[i].open();
            secondaryLsmBtrees[i] = (TestLsmBtree) secondaryIndexDataflowHelpers[i].getIndexInstance();
            secondaryIndexDataflowHelpers[i].close();
            primaryIndexDataflowHelpers[i].close();
            insertOps[i] = nc.getInsertPipeline(taskCtxs[i], dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null,
                    KEY_INDEXES, KEY_INDICATORS_LIST, storageManager, secondaryIndex, null).getLeft();
            actors[i] = new Actor("player-" + i, i);
        }
        // allow all operations
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            StorageTestUtils.allowAllOps(primaryLsmBtrees[i]);
            StorageTestUtils.allowAllOps(secondaryLsmBtrees[i]);
            actors[i].add(new Request(Request.Action.INSERT_OPEN));
        }
    }

    @After
    public void destroyIndex() throws Exception {
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            Request close = new Request(Request.Action.INSERT_CLOSE);
            actors[i].add(close);
            close.await();
        }
        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
        for (IIndexDataflowHelper indexDataflowHelper : secondaryIndexDataflowHelpers) {
            indexDataflowHelper.destroy();
        }
        for (IIndexDataflowHelper indexDataflowHelper : primaryIndexDataflowHelpers) {
            indexDataflowHelper.destroy();
        }
        for (Actor actor : actors) {
            actor.stop();
        }
    }

    @Test
    public void testFlushOneFullOneEmpty() {
        try {
            int totalNumOfComponents = TOTAL_NUM_OF_RECORDS / RECORDS_PER_COMPONENT;
            for (int j = 0; j < totalNumOfComponents; j++) {
                actors[0].add(new Request(Request.Action.INSERT_PATCH));
                actors[0].add(new Request(Request.Action.FLUSH_DATASET));
            }
            ensureDone(actors[0]);
            // search now and ensure partition 0 has all the records
            StorageTestUtils.searchAndAssertCount(nc, 0, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            // and that partition 1 has no records
            StorageTestUtils.searchAndAssertCount(nc, 1, dataset, storageManager, 0);
            // and that partition 0 has numFlushes disk components
            Assert.assertEquals(totalNumOfComponents, primaryLsmBtrees[0].getDiskComponents().size());
            // and that partition 1 has no disk components
            Assert.assertEquals(0, primaryLsmBtrees[1].getDiskComponents().size());
            // and that in partition 0, all secondary components has a corresponding primary
            List<ILSMDiskComponent> secondaryDiskComponents = secondaryLsmBtrees[0].getDiskComponents();
            List<ILSMDiskComponent> primaryDiskComponents = primaryLsmBtrees[0].getDiskComponents();
            for (int i = 0; i < secondaryDiskComponents.size(); i++) {
                Assert.assertEquals(secondaryDiskComponents.get(i).getId(), primaryDiskComponents.get(i).getId());
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void ensureDone(Actor actor) throws InterruptedException {
        Request req = new Request(Request.Action.DUMMY);
        actor.add(req);
        req.await();
    }

    /**
     * This test update partition 0, schedule flush and modify partition 1
     * Then ensure that in partition 1, primary and secondary have the same component ids
     */
    @Test
    public void testAllocateWhileFlushIsScheduled() {
        try {
            // when the vbc becomes full, we want to know
            AtomicBoolean isFull = new AtomicBoolean(false);
            MutableBoolean proceedToScheduleFlush = new MutableBoolean(false);
            primaryLsmBtrees[0].addVirtuablBufferCacheCallback(new IVirtualBufferCacheCallback() {
                @Override
                public void isFullChanged(boolean newValue) {
                    synchronized (isFull) {
                        isFull.set(newValue);
                        isFull.notifyAll();
                    }
                    synchronized (proceedToScheduleFlush) {
                        while (!proceedToScheduleFlush.booleanValue()) {
                            try {
                                proceedToScheduleFlush.wait();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                }
            });
            Request insertReq = new Request(Request.Action.INSERT_PATCH);
            actors[0].add(insertReq);
            while (true) {
                Thread.sleep(100);
                if (insertReq.done) {
                    // if done, then flush was not triggered, then we need to insert a new patch
                    insertReq = new Request(Request.Action.INSERT_PATCH);
                    actors[0].add(insertReq);
                } else if (isFull.get()) {
                    break;
                }
            }

            // now, we need to do two things
            // allocate primary in partition 1 but not proceed
            MutableBoolean proceedToAllocateSecondaryIndex = new MutableBoolean(false);
            MutableBoolean allocated = new MutableBoolean(false);
            primaryLsmBtrees[1].addAllocateCallback(new ITestOpCallback<Void>() {
                @Override
                public void before(Void t) {
                    // Nothing
                }

                @Override
                public void after(Void t) {
                    synchronized (allocated) {
                        allocated.setValue(true);
                        allocated.notifyAll();
                    }
                    synchronized (proceedToAllocateSecondaryIndex) {
                        while (!proceedToAllocateSecondaryIndex.booleanValue()) {
                            try {
                                proceedToAllocateSecondaryIndex.wait();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                }
            });
            insertReq = new Request(Request.Action.INSERT_PATCH);
            actors[1].add(insertReq);
            // Wait for the allocation to happen
            synchronized (allocated) {
                while (!allocated.booleanValue()) {
                    allocated.wait();
                }
            }
            // The memory component has been allocated. now we allow the first actor to proceed to schedule flush
            MutableBoolean flushStarted = new MutableBoolean(false);
            primaryLsmBtrees[0].addFlushCallback(new ITestOpCallback<Semaphore>() {
                @Override
                public void before(Semaphore t) {
                    synchronized (flushStarted) {
                        flushStarted.setValue(true);
                        flushStarted.notifyAll();
                    }
                }

                @Override
                public void after(Semaphore t) {
                }
            });
            synchronized (proceedToScheduleFlush) {
                proceedToScheduleFlush.setValue(true);
                proceedToScheduleFlush.notifyAll();
            }
            // Now we need to know that the flush has been scheduled
            synchronized (flushStarted) {
                while (!flushStarted.booleanValue()) {
                    flushStarted.wait();
                }
            }

            // we now allow the allocation to proceed
            synchronized (proceedToAllocateSecondaryIndex) {
                proceedToAllocateSecondaryIndex.setValue(true);
                proceedToAllocateSecondaryIndex.notifyAll();
            }
            // ensure the insert patch has completed
            insertReq.await();
            primaryLsmBtrees[1].clearAllocateCallbacks();
            // check the Ids of the memory components of partition 1
            // This shows the bug
            Assert.assertEquals(primaryLsmBtrees[1].getCurrentMemoryComponent().getId(),
                    secondaryLsmBtrees[1].getCurrentMemoryComponent().getId());
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRecycleWhileFlushIsScheduled() {
        try {
            Request insertReq = new Request(Request.Action.INSERT_PATCH);
            actors[0].add(insertReq);
            Request flushReq = new Request(Request.Action.FLUSH_DATASET);
            actors[0].add(flushReq);
            flushReq.await();
            // ensure that index switched to second component
            Assert.assertEquals(1, primaryLsmBtrees[0].getCurrentMemoryComponentIndex());
            insertReq = new Request(Request.Action.INSERT_PATCH);
            actors[0].add(insertReq);
            flushReq = new Request(Request.Action.FLUSH_DATASET);
            actors[0].add(flushReq);
            flushReq.await();
            // ensure we switched back to first component
            Assert.assertEquals(0, primaryLsmBtrees[0].getCurrentMemoryComponentIndex());
            // flush first component of partition 1
            insertReq = new Request(Request.Action.INSERT_PATCH);
            actors[1].add(insertReq);
            flushReq = new Request(Request.Action.FLUSH_DATASET);
            actors[1].add(flushReq);
            flushReq.await();
            // ensure partition 1 is now on second component
            Assert.assertEquals(1, primaryLsmBtrees[1].getCurrentMemoryComponentIndex());
            // now we want to control when schedule flush is executed
            AtomicBoolean arrivedAtSchduleFlush = new AtomicBoolean(false);
            AtomicBoolean finishedSchduleFlush = new AtomicBoolean(false);
            MutableBoolean proceedToScheduleFlush = new MutableBoolean(false);
            // keep track of the flush of partition 1 since partitions 0 and 1 are flushed seperately
            addOpTrackerCallback(primaryLsmBtrees[1], new ITestOpCallback<Void>() {
                @Override
                public void before(Void t) {
                    synchronized (arrivedAtSchduleFlush) {
                        arrivedAtSchduleFlush.set(true);
                        arrivedAtSchduleFlush.notifyAll();
                    }
                    synchronized (proceedToScheduleFlush) {
                        while (!proceedToScheduleFlush.booleanValue()) {
                            try {
                                proceedToScheduleFlush.wait();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                }

                @Override
                public void after(Void t) {
                    synchronized (finishedSchduleFlush) {
                        finishedSchduleFlush.set(true);
                        finishedSchduleFlush.notifyAll();
                    }
                }
            });
            AtomicBoolean isFull = new AtomicBoolean(false);
            MutableBoolean proceedAfterIsFullChanged = new MutableBoolean(false);
            primaryLsmBtrees[1].addVirtuablBufferCacheCallback(new IVirtualBufferCacheCallback() {
                @Override
                public void isFullChanged(boolean newValue) {
                    synchronized (isFull) {
                        isFull.set(newValue);
                        isFull.notifyAll();
                    }
                    synchronized (proceedAfterIsFullChanged) {
                        while (!proceedAfterIsFullChanged.booleanValue()) {
                            try {
                                proceedAfterIsFullChanged.wait();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                }
            });

            // now we start adding records to partition 1 until flush is triggerred
            insertReq = new Request(Request.Action.INSERT_PATCH);
            actors[1].add(insertReq);
            while (true) {
                Thread.sleep(100);
                if (insertReq.done) {
                    // if done, then flush was not triggered, then we need to insert a new patch
                    insertReq = new Request(Request.Action.INSERT_PATCH);
                    actors[1].add(insertReq);
                } else if (isFull.get()) {
                    break;
                }
            }
            // Now we know that vbc is full and flush will be scheduled, we allow this to proceed
            synchronized (proceedAfterIsFullChanged) {
                proceedAfterIsFullChanged.setValue(true);
                proceedAfterIsFullChanged.notifyAll();
            }

            // now we want to control the recycling of components in partition 0
            MutableBoolean recycledPrimary = new MutableBoolean(false);
            MutableBoolean proceedAfterRecyclePrimary = new MutableBoolean(false);
            ITestOpCallback<ILSMMemoryComponent> primaryRecycleCallback = new ITestOpCallback<ILSMMemoryComponent>() {
                @Override
                public void before(ILSMMemoryComponent t) {
                }

                @Override
                public void after(ILSMMemoryComponent t) {
                    synchronized (recycledPrimary) {
                        recycledPrimary.setValue(true);
                        recycledPrimary.notifyAll();
                    }
                    synchronized (proceedAfterRecyclePrimary) {
                        while (!proceedAfterRecyclePrimary.booleanValue()) {
                            try {
                                proceedAfterRecyclePrimary.wait();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                }
            };
            primaryLsmBtrees[0].addIoRecycleCallback(primaryRecycleCallback);

            MutableBoolean arrivedToRecycleSecondary = new MutableBoolean(false);
            MutableBoolean proceedToRecycleSecondary = new MutableBoolean(false);
            ITestOpCallback<ILSMMemoryComponent> secondaryRecycleCallback = new ITestOpCallback<ILSMMemoryComponent>() {
                @Override
                public void before(ILSMMemoryComponent t) {
                    synchronized (arrivedToRecycleSecondary) {
                        arrivedToRecycleSecondary.setValue(true);
                        arrivedToRecycleSecondary.notifyAll();
                    }
                    synchronized (proceedToRecycleSecondary) {
                        while (!proceedToRecycleSecondary.booleanValue()) {
                            try {
                                proceedToRecycleSecondary.wait();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                }

                @Override
                public void after(ILSMMemoryComponent t) {
                }
            };
            secondaryLsmBtrees[0].addIoRecycleCallback(secondaryRecycleCallback);
            // we first ensure that schedule flush arrived
            synchronized (arrivedAtSchduleFlush) {
                while (!arrivedAtSchduleFlush.get()) {
                    arrivedAtSchduleFlush.wait();
                }
            }

            // we insert a single frame into partition 0
            insertReq = new Request(Request.Action.INSERT_PATCH);
            actors[0].add(insertReq);
            // wait until component has been recycled
            synchronized (recycledPrimary) {
                while (!recycledPrimary.booleanValue()) {
                    recycledPrimary.wait();
                }
            }
            synchronized (proceedAfterRecyclePrimary) {
                proceedAfterRecyclePrimary.setValue(true);
                proceedAfterRecyclePrimary.notifyAll();
            }
            // now, we know that the primary has been recycled. we allow it to proceed
            // we allow the scheduleFlush to proceed
            synchronized (proceedToScheduleFlush) {
                proceedToScheduleFlush.setValue(true);
                proceedToScheduleFlush.notifyAll();
            }
            // wait until scheduleFlushCompletes
            synchronized (finishedSchduleFlush) {
                while (!finishedSchduleFlush.get()) {
                    finishedSchduleFlush.wait();
                }
            }
            // allow recycling of secondary
            synchronized (proceedToRecycleSecondary) {
                proceedToRecycleSecondary.setValue(true);
                proceedToRecycleSecondary.notifyAll();
            }
            // ensure that the insert completes
            insertReq.await();
            dsLifecycleMgr.getDatasetInfo(DATASET_ID).waitForIO();
            // check first partition
            List<ILSMDiskComponent> secondaryDiskComponents = secondaryLsmBtrees[0].getDiskComponents();
            List<ILSMDiskComponent> primaryDiskComponents = primaryLsmBtrees[0].getDiskComponents();
            for (int i = 0; i < secondaryDiskComponents.size(); i++) {
                Assert.assertEquals(secondaryDiskComponents.get(i).getId(), primaryDiskComponents.get(i).getId());
            }
            // check second partition
            secondaryDiskComponents = secondaryLsmBtrees[1].getDiskComponents();
            primaryDiskComponents = primaryLsmBtrees[1].getDiskComponents();
            for (int i = 0; i < secondaryDiskComponents.size(); i++) {
                Assert.assertEquals(secondaryDiskComponents.get(i).getId(), primaryDiskComponents.get(i).getId());
            }
            // ensure the two memory components at partition 0 have the same component ids
            Assert.assertEquals(primaryLsmBtrees[0].getCurrentMemoryComponent().getId(),
                    secondaryLsmBtrees[0].getCurrentMemoryComponent().getId());
            // ensure the two memory components at partition 0 have the same component ids
            Assert.assertEquals(primaryLsmBtrees[1].getCurrentMemoryComponent().getId(),
                    secondaryLsmBtrees[1].getCurrentMemoryComponent().getId());
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private static class Request {
        enum Action {
            DUMMY,
            INSERT_OPEN,
            INSERT_PATCH,
            FLUSH_DATASET,
            INSERT_CLOSE
        }

        private final Action action;
        private volatile boolean done;

        public Request(Action action) {
            this.action = action;
            done = false;
        }

        synchronized void complete() {
            done = true;
            notifyAll();
        }

        synchronized void await() throws InterruptedException {
            while (!done) {
                wait();
            }
        }

    }

    public class Actor extends SingleThreadEventProcessor<Request> {
        private final int partition;
        private final RecordTupleGenerator tupleGenerator;
        private final VSizeFrame frame;
        private final FrameTupleAppender tupleAppender;

        public Actor(String name, int partition) throws HyracksDataException {
            super(name);
            this.partition = partition;
            tupleGenerator = new RecordTupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS,
                    RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            frame = new VSizeFrame(taskCtxs[partition]);
            tupleAppender = new FrameTupleAppender(frame);
        }

        @Override
        protected void handle(Request req) throws Exception {
            try {
                switch (req.action) {
                    case FLUSH_DATASET:
                        if (tupleAppender.getTupleCount() > 0) {
                            tupleAppender.write(insertOps[partition], true);
                        }
                        dsLifecycleMgr.flushDataset(dataset.getDatasetId(), false);
                        break;
                    case INSERT_CLOSE:
                        insertOps[partition].close();
                        break;
                    case INSERT_OPEN:
                        insertOps[partition].open();
                        break;
                    case INSERT_PATCH:
                        for (int j = 0; j < RECORDS_PER_COMPONENT; j++) {
                            ITupleReference tuple = tupleGenerator.next();
                            DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOps[partition]);
                        }
                        if (tupleAppender.getTupleCount() > 0) {
                            tupleAppender.write(insertOps[partition], true);
                        }
                        StorageTestUtils.waitForOperations(primaryLsmBtrees[partition]);
                        break;
                    default:
                        break;
                }
            } finally {
                req.complete();
            }
        }
    }

    private static void addOpTrackerCallback(TestLsmBtree lsmBtree, ITestOpCallback<Void> callback) {
        if (!lsmBtree.isPrimaryIndex()) {
            throw new IllegalArgumentException("Can only add callbacks to primary opTracker");
        }
        TestPrimaryIndexOperationTracker opTracker = (TestPrimaryIndexOperationTracker) lsmBtree.getOperationTracker();
        opTracker.addCallback(callback);
    }
}
