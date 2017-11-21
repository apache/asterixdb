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
import org.apache.asterix.app.data.gen.TupleGenerator;
import org.apache.asterix.app.data.gen.TupleGenerator.GenerationFunction;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.dataflow.LSMInsertDeleteOperatorNodePushable;
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
import org.apache.asterix.test.common.TestHelper;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiPartitionLSMIndexTest {
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
    private static final int TOTAL_NUM_OF_RECORDS = 10000;
    private static final int RECORDS_PER_COMPONENT = 1000;
    private static final int DATASET_ID = 101;
    private static final String DATAVERSE_NAME = "TestDV";
    private static final String DATASET_NAME = "TestDS";
    private static final String DATA_TYPE_NAME = "DUMMY";
    private static final String NODE_GROUP_NAME = "DEFAULT";
    private static final StorageComponentProvider storageManager = new StorageComponentProvider();
    private static final int NUM_PARTITIONS = 2;
    private static TestNodeController nc;
    private static NCAppRuntimeContext ncAppCtx;
    private static IDatasetLifecycleManager dsLifecycleMgr;
    private static Dataset dataset;
    private static ITransactionContext txnCtx;
    private static TestLsmBtree[] primarylsmBtrees;
    private static IHyracksTaskContext[] taskCtxs;
    private static IIndexDataflowHelper[] indexDataflowHelpers;
    private static LSMInsertDeleteOperatorNodePushable[] insertOps;

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
        dataset = new TestDataset(DATAVERSE_NAME, DATASET_NAME, DATAVERSE_NAME, DATA_TYPE_NAME, NODE_GROUP_NAME,
                NoMergePolicyFactory.NAME, null, new InternalDatasetDetails(null, PartitioningStrategy.HASH,
                        partitioningKeys, null, null, null, false, null),
                null, DatasetType.INTERNAL, DATASET_ID, 0);
        taskCtxs = new IHyracksTaskContext[NUM_PARTITIONS];
        indexDataflowHelpers = new IIndexDataflowHelper[NUM_PARTITIONS];
        primarylsmBtrees = new TestLsmBtree[NUM_PARTITIONS];
        insertOps = new LSMInsertDeleteOperatorNodePushable[NUM_PARTITIONS];
        JobId jobId = nc.newJobId();
        txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(jobId),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
        for (int i = 0; i < taskCtxs.length; i++) {
            taskCtxs[i] = nc.createTestContext(jobId, i, false);
            PrimaryIndexInfo primaryIndexInfo = nc.createPrimaryIndex(dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null,
                    storageManager, KEY_INDEXES, KEY_INDICATORS_LIST, i);
            IndexDataflowHelperFactory iHelperFactory =
                    new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
            indexDataflowHelpers[i] = iHelperFactory.create(taskCtxs[i].getJobletContext().getServiceContext(), i);
            indexDataflowHelpers[i].open();
            primarylsmBtrees[i] = (TestLsmBtree) indexDataflowHelpers[i].getIndexInstance();
            indexDataflowHelpers[i].close();
            insertOps[i] = nc.getInsertPipeline(taskCtxs[i], dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null,
                    KEY_INDEXES, KEY_INDICATORS_LIST, storageManager).getLeft();
        }
    }

    @After
    public void destroyIndex() throws Exception {
        for (IIndexDataflowHelper indexDataflowHelper : indexDataflowHelpers) {
            indexDataflowHelper.destroy();
        }
    }

    @Test
    public void testFlushOneFullOneEmpty() {
        try {
            // allow all operations
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                ComponentRollbackTest.allowAllOps(primarylsmBtrees[i]);
            }

            insertOps[0].open();
            TupleGenerator tupleGenerator = new TupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS,
                    RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
            VSizeFrame frame = new VSizeFrame(taskCtxs[0]);
            FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
            int numFlushes = 0;
            for (int j = 0; j < TOTAL_NUM_OF_RECORDS; j++) {
                // flush every RECORDS_PER_COMPONENT records
                if (j % RECORDS_PER_COMPONENT == (RECORDS_PER_COMPONENT - 1) && j + 1 != TOTAL_NUM_OF_RECORDS) {
                    if (tupleAppender.getTupleCount() > 0) {
                        tupleAppender.write(insertOps[0], true);
                    }
                    dsLifecycleMgr.flushDataset(dataset.getDatasetId(), false);
                    numFlushes++;
                }
                ITupleReference tuple = tupleGenerator.next();
                DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOps[0]);
            }
            if (tupleAppender.getTupleCount() > 0) {
                tupleAppender.write(insertOps[0], true);
            }
            insertOps[0].close();
            dsLifecycleMgr.flushDataset(dataset.getDatasetId(), false);
            numFlushes++;
            nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            // search now and ensure partition 0 has all the records
            ComponentRollbackTest.searchAndAssertCount(nc, 0, dataset, storageManager, TOTAL_NUM_OF_RECORDS);
            // and that partition 1 has no records
            ComponentRollbackTest.searchAndAssertCount(nc, 1, dataset, storageManager, 0);
            // and that partition 0 has numFlushes disk components
            Assert.assertEquals(numFlushes, primarylsmBtrees[0].getDiskComponents().size());
            // and that partition 1 has no disk components
            Assert.assertEquals(0, primarylsmBtrees[1].getDiskComponents().size());
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

}
