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
import java.util.Collections;
import java.util.List;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.bootstrap.TestNodeController.SecondaryIndexInfo;
import org.apache.asterix.app.data.gen.RecordTupleGenerator;
import org.apache.asterix.app.data.gen.RecordTupleGenerator.GenerationFunction;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.IndexCheckpoint;
import org.apache.asterix.common.storage.ResourceReference;
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
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorNodePushable;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
import org.apache.asterix.test.base.TestMethodTracer;
import org.apache.asterix.test.common.TestHelper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.util.SingleThreadEventProcessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.datagen.IFieldValueGenerator;
import org.apache.hyracks.storage.am.common.datagen.TupleGenerator;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.Parameterized;

public class CheckpointInSecondaryIndexTest {
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
    private static final List<Integer> KEY_INDICATORS_LIST = Collections.singletonList(Index.RECORD_INDICATOR);
    private static final int RECORDS_PER_COMPONENT = 500;
    private static final int DATASET_ID = 101;
    private static final String DATAVERSE_NAME = "TestDV";
    private static final String DATASET_NAME = "TestDS";
    private static final String INDEX_NAME = "TestIdx";
    private static final String DATA_TYPE_NAME = "DUMMY";
    private static final String NODE_GROUP_NAME = "DEFAULT";
    private static final IndexType INDEX_TYPE = IndexType.BTREE;
    private static final IFieldValueGenerator[] SECONDARY_INDEX_VALUE_GENERATOR =
            { new AInt64ValueGenerator(), new AInt32ValueGenerator() };
    private static final List<List<String>> INDEX_FIELD_NAMES =
            Collections.singletonList(Collections.singletonList(RECORD_TYPE.getFieldNames()[1]));
    private static final List<Integer> INDEX_FIELD_INDICATORS = Collections.singletonList(Index.RECORD_INDICATOR);
    private static final List<IAType> INDEX_FIELD_TYPES = Collections.singletonList(BuiltinType.AINT64);
    private static final StorageComponentProvider storageManager = new StorageComponentProvider();
    private static TestNodeController nc;
    private static NCAppRuntimeContext ncAppCtx;
    private static IDatasetLifecycleManager dsLifecycleMgr;
    private static Dataset dataset;
    private static Index secondaryIndex;
    private static ITransactionContext txnCtx;
    private static TestLsmBtree primaryLsmBtree;
    private static TestLsmBtree secondaryLsmBtree;
    private static PrimaryIndexInfo primaryIndexInfo;
    private static IHyracksTaskContext taskCtx;
    private static IIndexDataflowHelper primaryIndexDataflowHelper;
    private static IIndexDataflowHelper secondaryIndexDataflowHelper;
    private static LSMPrimaryInsertOperatorNodePushable insertOp;
    private static LSMIndexBulkLoadOperatorNodePushable indexLoadOp;
    private static IHyracksTaskContext loadTaskCtx;
    private static SecondaryIndexInfo secondaryIndexInfo;
    private static Actor actor;

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

    @Rule
    public TestRule tracer = new TestMethodTracer();

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
        taskCtx = null;
        primaryIndexDataflowHelper = null;
        secondaryIndexDataflowHelper = null;
        primaryLsmBtree = null;
        insertOp = null;
        JobId jobId = nc.newJobId();
        txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(jobId),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
        actor = null;
        taskCtx = nc.createTestContext(jobId, 0, false);
        primaryIndexInfo = nc.createPrimaryIndex(dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null, storageManager,
                KEY_INDEXES, KEY_INDICATORS_LIST, 0);
        IndexDataflowHelperFactory iHelperFactory =
                new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
        primaryIndexDataflowHelper = iHelperFactory.create(taskCtx.getJobletContext().getServiceContext(), 0);
        primaryIndexDataflowHelper.open();
        primaryLsmBtree = (TestLsmBtree) primaryIndexDataflowHelper.getIndexInstance();
        primaryIndexDataflowHelper.close();
        // This pipeline skips the secondary index
        insertOp = nc.getInsertPipeline(taskCtx, dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null, KEY_INDEXES,
                KEY_INDICATORS_LIST, storageManager, null, null).getLeft();
        actor = new Actor("player");
        // allow all operations
        StorageTestUtils.allowAllOps(primaryLsmBtree);
        actor.add(new Request(Request.Action.INSERT_OPEN));
    }

    @After
    public void destroyIndex() throws Exception {
        Request close = new Request(Request.Action.INSERT_CLOSE);
        actor.add(close);
        close.await();
        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
        if (secondaryIndexDataflowHelper != null) {
            secondaryIndexDataflowHelper.destroy();
        }
        primaryIndexDataflowHelper.destroy();
        actor.stop();
    }

    @Test
    public void testCheckpointUpdatedWhenSecondaryIsEmpty() throws Exception {
        // create secondary
        createSecondaryIndex();
        actor.add(new Request(Request.Action.INSERT_PATCH));
        ensureDone(actor);
        // search now and ensure partition 0 has all the records
        StorageTestUtils.searchAndAssertCount(nc, 0, dataset, storageManager, RECORDS_PER_COMPONENT);
        // and that secondary index is empty
        Assert.assertTrue(secondaryLsmBtree.isCurrentMutableComponentEmpty());
        // flush
        actor.add(new Request(Request.Action.FLUSH_DATASET));
        ensureDone(actor);
        // ensure primary has a component
        Assert.assertEquals(1, primaryLsmBtree.getDiskComponents().size());

        // ensure secondary doesn't have a component
        Assert.assertEquals(0, secondaryLsmBtree.getDiskComponents().size());
        // ensure that current memory component index match
        Assert.assertEquals(secondaryLsmBtree.getCurrentMemoryComponentIndex(),
                primaryLsmBtree.getCurrentMemoryComponentIndex());
        // ensure both checkpoint files has the same component id as the last flushed component id
        ILSMDiskComponent primaryDiskComponent = primaryLsmBtree.getDiskComponents().get(0);
        LSMComponentId id = (LSMComponentId) primaryDiskComponent.getId();
        long min = id.getMinId();
        // primary ref
        Field fileManagerField = AbstractLSMIndex.class.getDeclaredField("fileManager"); //get private declared object from class
        fileManagerField.setAccessible(true); //Make it accessible so you can access it
        ILSMIndexFileManager primaryFileManager = (ILSMIndexFileManager) fileManagerField.get(primaryLsmBtree);
        final ResourceReference primaryRef = ResourceReference
                .of(primaryFileManager.getRelFlushFileReference().getInsertIndexFileReference().getAbsolutePath());
        IIndexCheckpointManager primaryCheckpointManager = getIndexCheckpointManagerProvider().get(primaryRef);
        // secondary ref
        ILSMIndexFileManager secondaryFileManager = (ILSMIndexFileManager) fileManagerField.get(secondaryLsmBtree);
        final ResourceReference secondaryRef = ResourceReference
                .of(secondaryFileManager.getRelFlushFileReference().getInsertIndexFileReference().getAbsolutePath());
        IIndexCheckpointManager secondaryCheckpointManager = getIndexCheckpointManagerProvider().get(secondaryRef);
        IndexCheckpoint latestPrimaryCheckpoint = primaryCheckpointManager.getLatest();
        IndexCheckpoint latestSecondaryCheckpoint = secondaryCheckpointManager.getLatest();
        Assert.assertEquals(latestPrimaryCheckpoint.getLastComponentId(), min);
        Assert.assertEquals(latestSecondaryCheckpoint.getLastComponentId(), min);
        Assert.assertTrue(latestPrimaryCheckpoint.hasNullMissingValuesFix());
        Assert.assertTrue(latestSecondaryCheckpoint.hasNullMissingValuesFix());
    }

    private void createSecondaryIndex()
            throws HyracksDataException, RemoteException, ACIDException, AlgebricksException {
        SecondaryIndexInfo secondaryIndexInfo =
                nc.createSecondaryIndex(primaryIndexInfo, secondaryIndex, storageManager, 0);
        IndexDataflowHelperFactory iHelperFactory =
                new IndexDataflowHelperFactory(nc.getStorageManager(), secondaryIndexInfo.getFileSplitProvider());
        secondaryIndexDataflowHelper = iHelperFactory.create(taskCtx.getJobletContext().getServiceContext(), 0);
        secondaryIndexDataflowHelper.open();
        secondaryLsmBtree = (TestLsmBtree) secondaryIndexDataflowHelper.getIndexInstance();
        secondaryIndexDataflowHelper.close();
    }

    @Test
    public void testCheckpointWhenBulkloadingSecondaryAndPrimaryIsSingleComponent() throws Exception {
        // create secondary
        actor.add(new Request(Request.Action.INSERT_PATCH));
        ensureDone(actor);
        // search now and ensure partition 0 has all the records
        StorageTestUtils.searchAndAssertCount(nc, 0, dataset, storageManager, RECORDS_PER_COMPONENT);
        // flush
        actor.add(new Request(Request.Action.FLUSH_DATASET));
        ensureDone(actor);
        // ensure primary has a component
        Assert.assertEquals(1, primaryLsmBtree.getDiskComponents().size());
        // ensure both checkpoint files has the same component id as the last flushed component id
        ILSMDiskComponent primaryDiskComponent = primaryLsmBtree.getDiskComponents().get(0);
        LSMComponentId id = (LSMComponentId) primaryDiskComponent.getId();
        long min = id.getMinId();
        // primary ref
        Field fileManagerField = AbstractLSMIndex.class.getDeclaredField("fileManager"); //get private declared object from class
        fileManagerField.setAccessible(true); //Make it accessible so you can access it
        ILSMIndexFileManager primaryFileManager = (ILSMIndexFileManager) fileManagerField.get(primaryLsmBtree);
        final ResourceReference primaryRef = ResourceReference
                .of(primaryFileManager.getRelFlushFileReference().getInsertIndexFileReference().getAbsolutePath());
        IIndexCheckpointManager primaryCheckpointManager = getIndexCheckpointManagerProvider().get(primaryRef);
        IndexCheckpoint latestPrimaryCheckpoint = primaryCheckpointManager.getLatest();
        Assert.assertEquals(latestPrimaryCheckpoint.getLastComponentId(), min);
        createSecondaryIndex();
        JobId jobId = nc.newJobId();
        loadTaskCtx = nc.createTestContext(jobId, 0, false);
        Pair<SecondaryIndexInfo, LSMIndexBulkLoadOperatorNodePushable> infoAndOp =
                nc.getBulkLoadSecondaryOperator(loadTaskCtx, dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null,
                        KEY_INDEXES, KEY_INDICATORS_LIST, storageManager, secondaryIndex, RECORDS_PER_COMPONENT);
        indexLoadOp = infoAndOp.getRight();
        secondaryIndexInfo = infoAndOp.getLeft();
        actor.add(new Request(Request.Action.LOAD_OPEN));
        actor.add(new Request(Request.Action.INDEX_LOAD_PATCH));
        actor.add(new Request(Request.Action.LOAD_CLOSE));
        ensureDone(actor);
        latestPrimaryCheckpoint = primaryCheckpointManager.getLatest();
        Assert.assertEquals(latestPrimaryCheckpoint.getLastComponentId(), min);
        ILSMIndexFileManager secondaryFileManager = (ILSMIndexFileManager) fileManagerField.get(secondaryLsmBtree);
        final ResourceReference secondaryRef = ResourceReference
                .of(secondaryFileManager.getRelFlushFileReference().getInsertIndexFileReference().getAbsolutePath());
        IIndexCheckpointManager secondaryCheckpointManager = getIndexCheckpointManagerProvider().get(secondaryRef);
        IndexCheckpoint latestSecondaryCheckpoint = secondaryCheckpointManager.getLatest();
        Assert.assertEquals(latestSecondaryCheckpoint.getLastComponentId(), min);
        Assert.assertTrue(latestPrimaryCheckpoint.hasNullMissingValuesFix());
        Assert.assertTrue(latestSecondaryCheckpoint.hasNullMissingValuesFix());
    }

    @Test
    public void testCheckpointWhenBulkloadingSecondaryAndPrimaryIsTwoComponents() throws Exception {
        // create secondary
        actor.add(new Request(Request.Action.INSERT_PATCH));
        ensureDone(actor);
        // search now and ensure partition 0 has all the records
        StorageTestUtils.searchAndAssertCount(nc, 0, dataset, storageManager, RECORDS_PER_COMPONENT);
        // flush
        actor.add(new Request(Request.Action.FLUSH_DATASET));
        ensureDone(actor);
        // ensure primary has a component
        Assert.assertEquals(1, primaryLsmBtree.getDiskComponents().size());
        // ensure both checkpoint files has the same component id as the last flushed component id
        ILSMDiskComponent primaryDiskComponent = primaryLsmBtree.getDiskComponents().get(0);
        LSMComponentId id = (LSMComponentId) primaryDiskComponent.getId();
        long min = id.getMinId();
        // primary ref
        Field fileManagerField = AbstractLSMIndex.class.getDeclaredField("fileManager"); //get private declared object from class
        fileManagerField.setAccessible(true); //Make it accessible so you can access it
        ILSMIndexFileManager primaryFileManager = (ILSMIndexFileManager) fileManagerField.get(primaryLsmBtree);
        final ResourceReference primaryRef = ResourceReference
                .of(primaryFileManager.getRelFlushFileReference().getInsertIndexFileReference().getAbsolutePath());
        IIndexCheckpointManager primaryCheckpointManager = getIndexCheckpointManagerProvider().get(primaryRef);
        IndexCheckpoint latestPrimaryCheckpoint = primaryCheckpointManager.getLatest();
        Assert.assertEquals(latestPrimaryCheckpoint.getLastComponentId(), min);
        actor.add(new Request(Request.Action.INSERT_PATCH));
        ensureDone(actor);
        actor.add(new Request(Request.Action.FLUSH_DATASET));
        ensureDone(actor);
        Assert.assertEquals(2, primaryLsmBtree.getDiskComponents().size());
        // ensure both checkpoint files has the same component id as the last flushed component id
        primaryDiskComponent = primaryLsmBtree.getDiskComponents().get(0);
        id = (LSMComponentId) primaryDiskComponent.getId();
        min = id.getMaxId();
        createSecondaryIndex();
        JobId jobId = nc.newJobId();
        loadTaskCtx = nc.createTestContext(jobId, 0, false);
        Pair<SecondaryIndexInfo, LSMIndexBulkLoadOperatorNodePushable> infoAndOp =
                nc.getBulkLoadSecondaryOperator(loadTaskCtx, dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null,
                        KEY_INDEXES, KEY_INDICATORS_LIST, storageManager, secondaryIndex, RECORDS_PER_COMPONENT);
        indexLoadOp = infoAndOp.getRight();
        secondaryIndexInfo = infoAndOp.getLeft();
        actor.add(new Request(Request.Action.LOAD_OPEN));
        actor.add(new Request(Request.Action.INDEX_LOAD_PATCH));
        actor.add(new Request(Request.Action.LOAD_CLOSE));
        ensureDone(actor);
        latestPrimaryCheckpoint = primaryCheckpointManager.getLatest();
        Assert.assertEquals(latestPrimaryCheckpoint.getLastComponentId(), min);
        ILSMIndexFileManager secondaryFileManager = (ILSMIndexFileManager) fileManagerField.get(secondaryLsmBtree);
        final ResourceReference secondaryRef = ResourceReference
                .of(secondaryFileManager.getRelFlushFileReference().getInsertIndexFileReference().getAbsolutePath());
        IIndexCheckpointManager secondaryCheckpointManager = getIndexCheckpointManagerProvider().get(secondaryRef);
        IndexCheckpoint latestSecondaryCheckpoint = secondaryCheckpointManager.getLatest();
        Assert.assertEquals(latestSecondaryCheckpoint.getLastComponentId(), min);
        Assert.assertTrue(latestPrimaryCheckpoint.hasNullMissingValuesFix());
        Assert.assertTrue(latestSecondaryCheckpoint.hasNullMissingValuesFix());
    }

    @Test
    public void testCheckpointWhenBulkloadedSecondaryIsEmptyAndPrimaryIsEmpty() throws Exception {
        // ensure primary has no component
        Assert.assertEquals(0, primaryLsmBtree.getDiskComponents().size());
        // primary ref
        Field fileManagerField = AbstractLSMIndex.class.getDeclaredField("fileManager"); //get private declared object from class
        fileManagerField.setAccessible(true); //Make it accessible so you can access it
        ILSMIndexFileManager primaryFileManager = (ILSMIndexFileManager) fileManagerField.get(primaryLsmBtree);
        final ResourceReference primaryRef = ResourceReference
                .of(primaryFileManager.getRelFlushFileReference().getInsertIndexFileReference().getAbsolutePath());
        IIndexCheckpointManager primaryCheckpointManager = getIndexCheckpointManagerProvider().get(primaryRef);
        IndexCheckpoint latestPrimaryCheckpoint = primaryCheckpointManager.getLatest();
        createSecondaryIndex();
        JobId jobId = nc.newJobId();
        loadTaskCtx = nc.createTestContext(jobId, 0, false);
        Pair<SecondaryIndexInfo, LSMIndexBulkLoadOperatorNodePushable> infoAndOp =
                nc.getBulkLoadSecondaryOperator(loadTaskCtx, dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null,
                        KEY_INDEXES, KEY_INDICATORS_LIST, storageManager, secondaryIndex, RECORDS_PER_COMPONENT);
        indexLoadOp = infoAndOp.getRight();
        secondaryIndexInfo = infoAndOp.getLeft();
        actor.add(new Request(Request.Action.LOAD_OPEN));
        actor.add(new Request(Request.Action.LOAD_CLOSE));
        ensureDone(actor);
        latestPrimaryCheckpoint = primaryCheckpointManager.getLatest();
        ILSMIndexFileManager secondaryFileManager = (ILSMIndexFileManager) fileManagerField.get(secondaryLsmBtree);
        final ResourceReference secondaryRef = ResourceReference
                .of(secondaryFileManager.getRelFlushFileReference().getInsertIndexFileReference().getAbsolutePath());
        IIndexCheckpointManager secondaryCheckpointManager = getIndexCheckpointManagerProvider().get(secondaryRef);
        IndexCheckpoint latestSecondaryCheckpoint = secondaryCheckpointManager.getLatest();
        Assert.assertEquals(latestSecondaryCheckpoint.getLastComponentId(),
                latestPrimaryCheckpoint.getLastComponentId());
        Assert.assertTrue(latestPrimaryCheckpoint.hasNullMissingValuesFix());
        Assert.assertTrue(latestSecondaryCheckpoint.hasNullMissingValuesFix());
    }

    @Test
    public void testCheckpointWhenBulkloadedSecondaryIsEmptyAndPrimaryIsNotEmpty() throws Exception {
        // create secondary
        actor.add(new Request(Request.Action.INSERT_PATCH));
        ensureDone(actor);
        // search now and ensure partition 0 has all the records
        StorageTestUtils.searchAndAssertCount(nc, 0, dataset, storageManager, RECORDS_PER_COMPONENT);
        // flush
        actor.add(new Request(Request.Action.FLUSH_DATASET));
        ensureDone(actor);
        // ensure primary has a component
        Assert.assertEquals(1, primaryLsmBtree.getDiskComponents().size());
        // ensure both checkpoint files has the same component id as the last flushed component id
        ILSMDiskComponent primaryDiskComponent = primaryLsmBtree.getDiskComponents().get(0);
        LSMComponentId id = (LSMComponentId) primaryDiskComponent.getId();
        long min = id.getMinId();
        // primary ref
        Field fileManagerField = AbstractLSMIndex.class.getDeclaredField("fileManager"); //get private declared object from class
        fileManagerField.setAccessible(true); //Make it accessible so you can access it
        ILSMIndexFileManager primaryFileManager = (ILSMIndexFileManager) fileManagerField.get(primaryLsmBtree);
        final ResourceReference primaryRef = ResourceReference
                .of(primaryFileManager.getRelFlushFileReference().getInsertIndexFileReference().getAbsolutePath());
        IIndexCheckpointManager primaryCheckpointManager = getIndexCheckpointManagerProvider().get(primaryRef);
        IndexCheckpoint latestPrimaryCheckpoint = primaryCheckpointManager.getLatest();
        Assert.assertEquals(latestPrimaryCheckpoint.getLastComponentId(), min);
        createSecondaryIndex();
        JobId jobId = nc.newJobId();
        loadTaskCtx = nc.createTestContext(jobId, 0, false);
        Pair<SecondaryIndexInfo, LSMIndexBulkLoadOperatorNodePushable> infoAndOp =
                nc.getBulkLoadSecondaryOperator(loadTaskCtx, dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null,
                        KEY_INDEXES, KEY_INDICATORS_LIST, storageManager, secondaryIndex, RECORDS_PER_COMPONENT);
        indexLoadOp = infoAndOp.getRight();
        secondaryIndexInfo = infoAndOp.getLeft();
        actor.add(new Request(Request.Action.LOAD_OPEN));
        actor.add(new Request(Request.Action.LOAD_CLOSE));
        ensureDone(actor);
        latestPrimaryCheckpoint = primaryCheckpointManager.getLatest();
        Assert.assertEquals(latestPrimaryCheckpoint.getLastComponentId(), min);
        ILSMIndexFileManager secondaryFileManager = (ILSMIndexFileManager) fileManagerField.get(secondaryLsmBtree);
        final ResourceReference secondaryRef = ResourceReference
                .of(secondaryFileManager.getRelFlushFileReference().getInsertIndexFileReference().getAbsolutePath());
        IIndexCheckpointManager secondaryCheckpointManager = getIndexCheckpointManagerProvider().get(secondaryRef);
        IndexCheckpoint latestSecondaryCheckpoint = secondaryCheckpointManager.getLatest();
        Assert.assertEquals(latestSecondaryCheckpoint.getLastComponentId(), min);
        Assert.assertTrue(latestPrimaryCheckpoint.hasNullMissingValuesFix());
        Assert.assertTrue(latestSecondaryCheckpoint.hasNullMissingValuesFix());
    }

    protected IIndexCheckpointManagerProvider getIndexCheckpointManagerProvider() {
        return ncAppCtx.getIndexCheckpointManagerProvider();
    }

    private void ensureDone(Actor actor) throws InterruptedException {
        Request req = new Request(Request.Action.DUMMY);
        actor.add(req);
        req.await();
    }

    private static class Request {
        enum Action {
            DUMMY,
            INSERT_OPEN,
            LOAD_OPEN,
            INSERT_PATCH,
            INDEX_LOAD_PATCH,
            FLUSH_DATASET,
            INSERT_CLOSE,
            LOAD_CLOSE,
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
        private final RecordTupleGenerator primaryInsertTupleGenerator;
        private final FrameTupleAppender tupleAppender;

        public Actor(String name) throws HyracksDataException {
            super(name);
            primaryInsertTupleGenerator = new RecordTupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS,
                    RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            tupleAppender = new FrameTupleAppender(new VSizeFrame(taskCtx));
        }

        @Override
        protected void handle(Request req) throws Exception {
            try {
                switch (req.action) {
                    case FLUSH_DATASET:
                        if (tupleAppender.getTupleCount() > 0) {
                            tupleAppender.write(insertOp, true);
                        }
                        dsLifecycleMgr.flushDataset(dataset.getDatasetId(), false);
                        break;
                    case INSERT_CLOSE:
                        insertOp.close();
                        break;
                    case INSERT_OPEN:
                        insertOp.open();
                        break;
                    case LOAD_OPEN:
                        indexLoadOp.open();
                        break;
                    case LOAD_CLOSE:
                        indexLoadOp.close();
                        break;
                    case INSERT_PATCH:
                        for (int j = 0; j < RECORDS_PER_COMPONENT; j++) {
                            ITupleReference tuple = primaryInsertTupleGenerator.next();
                            DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
                        }
                        if (tupleAppender.getTupleCount() > 0) {
                            tupleAppender.write(insertOp, true);
                        }
                        StorageTestUtils.waitForOperations(primaryLsmBtree);
                        break;
                    case INDEX_LOAD_PATCH:
                        TupleGenerator secondaryLoadTupleGenerator =
                                new TupleGenerator(SECONDARY_INDEX_VALUE_GENERATOR, secondaryIndexInfo.getSerdes(), 0);
                        FrameTupleAppender secondaryTupleAppender = new FrameTupleAppender(new VSizeFrame(loadTaskCtx));
                        for (int j = 0; j < RECORDS_PER_COMPONENT; j++) {
                            ITupleReference tuple = secondaryLoadTupleGenerator.next();
                            DataflowUtils.addTupleToFrame(secondaryTupleAppender, tuple, indexLoadOp);
                        }
                        if (secondaryTupleAppender.getTupleCount() > 0) {
                            secondaryTupleAppender.write(indexLoadOp, true);
                        }
                        break;
                    default:
                        break;
                }
            } finally {
                req.complete();
            }
        }
    }
}
