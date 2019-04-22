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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.data.gen.RecordTupleGenerator;
import org.apache.asterix.app.data.gen.RecordTupleGenerator.GenerationFunction;
import org.apache.asterix.app.data.gen.TestTupleCounterFrameWriter;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
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
import org.apache.asterix.test.dataflow.StorageTestUtils.Searcher;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SearchCursorComponentSwitchTest {
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
    private static final int TOTAL_NUM_OF_RECORDS = 2000;
    private static final int RECORDS_PER_COMPONENT = 1000;
    private static final int DATASET_ID = 101;
    private static final String DATAVERSE_NAME = "TestDV";
    private static final String DATASET_NAME = "TestDS";
    private static final String DATA_TYPE_NAME = "DUMMY";
    private static final String NODE_GROUP_NAME = "DEFAULT";
    private static final StorageComponentProvider storageManager = new StorageComponentProvider();
    private static TestNodeController nc;
    private static TestLsmBtree lsmBtree;
    private static NCAppRuntimeContext ncAppCtx;
    private static IDatasetLifecycleManager dsLifecycleMgr;
    private static Dataset dataset;
    private static IHyracksTaskContext ctx;
    private static IIndexDataflowHelper indexDataflowHelper;
    private static ITransactionContext txnCtx;
    private static LSMPrimaryInsertOperatorNodePushable insertOp;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("SetUp: ");
        TestHelper.deleteExistingInstanceFiles();
        nc = new TestNodeController(null, false);
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
        PrimaryIndexInfo primaryIndexInfo = nc.createPrimaryIndex(dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null,
                storageManager, KEY_INDEXES, KEY_INDICATORS_LIST, 0);
        IndexDataflowHelperFactory iHelperFactory =
                new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
        JobId jobId = nc.newJobId();
        ctx = nc.createTestContext(jobId, 0, false);
        indexDataflowHelper = iHelperFactory.create(ctx.getJobletContext().getServiceContext(), 0);
        indexDataflowHelper.open();
        lsmBtree = (TestLsmBtree) indexDataflowHelper.getIndexInstance();
        indexDataflowHelper.close();
        txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(ctx),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
        insertOp = nc.getInsertPipeline(ctx, dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null, KEY_INDEXES,
                KEY_INDICATORS_LIST, storageManager, null, null).getLeft();
    }

    @After
    public void destroyIndex() throws Exception {
        indexDataflowHelper.destroy();
    }

    void unblockSearch(TestLsmBtree lsmBtree) {
        lsmBtree.addSearchCallback(AllowTestOpCallback.INSTANCE);
        lsmBtree.allowSearch(1);
    }

    @Test
    public void testCursorSwitchSucceed() {
        try {
            // allow all operations
            StorageTestUtils.allowAllOps(lsmBtree);
            // except search
            lsmBtree.clearSearchCallbacks();
            insertOp.open();
            RecordTupleGenerator tupleGenerator = new RecordTupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES,
                    KEY_INDICATORS, RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            Searcher firstSearcher = null;
            for (int j = 0; j < TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % RECORDS_PER_COMPONENT == (RECORDS_PER_COMPONENT - 1) && j + 1 != TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    StorageTestUtils.flush(dsLifecycleMgr, lsmBtree, dataset, false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            // start the search
            firstSearcher = new Searcher(nc, 0, dataset, storageManager, lsmBtree, TOTAL_NUM_OF_RECORDS);
            // wait till firstSearcher enter the components
            firstSearcher.waitUntilEntered();
            StorageTestUtils.flush(dsLifecycleMgr, lsmBtree, dataset, false);
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            // unblock the search
            unblockSearch(lsmBtree);
            // ensure the search got the correct number
            Assert.assertTrue(firstSearcher.result());
            // search now and ensure
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCursorSwitchFails() {
        try {
            // allow all operations
            StorageTestUtils.allowAllOps(lsmBtree);
            // except search
            lsmBtree.clearSearchCallbacks();
            insertOp.open();
            RecordTupleGenerator tupleGenerator = new RecordTupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES,
                    KEY_INDICATORS, RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            Searcher firstSearcher = null;
            for (int j = 0; j < TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % RECORDS_PER_COMPONENT == (RECORDS_PER_COMPONENT - 1) && j + 1 != TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOp, true);
                    }
                    StorageTestUtils.flush(dsLifecycleMgr, lsmBtree, dataset, false);
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOp, true);
            }
            insertOp.close();
            // start the search
            firstSearcher = new Searcher(nc, 0, dataset, storageManager, lsmBtree, TOTAL_NUM_OF_RECORDS);
            // wait till firstSearcher enter the components
            firstSearcher.waitUntilEntered();
            StorageTestUtils.flush(dsLifecycleMgr, lsmBtree, dataset, false);
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            // merge all components
            ILSMIndexAccessor mergeAccessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            List<ILSMDiskComponent> mergedComponents = new ArrayList<>(lsmBtree.getDiskComponents());
            ILSMIOOperation merge = mergeAccessor.scheduleMerge(mergedComponents);
            merge.sync();
            if (merge.getStatus() == LSMIOOperationStatus.FAILURE) {
                throw HyracksDataException.create(merge.getFailure());
            }
            // unblock the search
            unblockSearch(lsmBtree);
            // ensure the search got the correct number
            Assert.assertTrue(firstSearcher.result());
            // search now and ensure
            searchAndAssertCount(nc, ctx, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void searchAndAssertCount(TestNodeController nc, IHyracksTaskContext ctx, Dataset dataset,
            StorageComponentProvider storageManager, int numOfRecords)
            throws HyracksDataException, AlgebricksException {
        nc.newJobId();
        TestTupleCounterFrameWriter countOp =
                StorageTestUtils.create(nc.getSearchOutputDesc(KEY_TYPES, RECORD_TYPE, META_TYPE),
                        Collections.emptyList(), Collections.emptyList(), false);
        IPushRuntime emptyTupleOp = nc.getFullScanPipeline(countOp, ctx, dataset, KEY_TYPES, RECORD_TYPE, META_TYPE,
                new NoMergePolicyFactory(), null, null, KEY_INDEXES, KEY_INDICATORS_LIST, storageManager);
        emptyTupleOp.open();
        emptyTupleOp.close();
        Assert.assertEquals(numOfRecords, countOp.getCount());
    }
}
