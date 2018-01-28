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
package org.apache.asterix.test.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.utils.SplitsAndConstraintsUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.common.TestHelper;
import org.apache.asterix.test.runtime.ExecutionTestUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor.DropOption;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexDropOperatorNodePushableTest {

    private static final IAType[] KEY_TYPES = { BuiltinType.AINT32 };
    private static final ARecordType RECORD_TYPE = new ARecordType("TestRecordType", new String[] { "key", "value" },
            new IAType[] { BuiltinType.AINT32, BuiltinType.AINT64 }, false);
    private static final ARecordType META_TYPE = null;
    private static final int[] KEY_INDEXES = { 0 };
    private static final List<Integer> KEY_INDICATORS_LIST = Arrays.asList(new Integer[] { Index.RECORD_INDICATOR });
    private static final int DATASET_ID = 101;
    private static final String DATAVERSE_NAME = "TestDV";
    private static final String DATASET_NAME = "TestDS";
    private static final String DATA_TYPE_NAME = "DUMMY";
    private static final String NODE_GROUP_NAME = "DEFAULT";
    private final AtomicBoolean dropFailed = new AtomicBoolean(false);
    private final TestExecutor testExecutor = new TestExecutor();

    @Before
    public void setUp() throws Exception {
        System.out.println("SetUp: ");
        TestHelper.deleteExistingInstanceFiles();
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("TearDown");
        TestHelper.deleteExistingInstanceFiles();
    }

    /**
     * Tests dropping a dataset using different
     * drop options
     *
     * @throws Exception
     */
    @Test
    public void dropOptionsTest() throws Exception {
        TestNodeController nc = new TestNodeController(null, false);
        try {
            nc.init();
            StorageComponentProvider storageManager = new StorageComponentProvider();
            List<List<String>> partitioningKeys = new ArrayList<>();
            partitioningKeys.add(Collections.singletonList("key"));
            Dataset dataset = new Dataset(DATAVERSE_NAME, DATASET_NAME, DATAVERSE_NAME, DATA_TYPE_NAME, NODE_GROUP_NAME,
                    NoMergePolicyFactory.NAME,
                    null, new InternalDatasetDetails(null, InternalDatasetDetails.PartitioningStrategy.HASH,
                            partitioningKeys, null, null, null, false, null),
                    null, DatasetConfig.DatasetType.INTERNAL, DATASET_ID, 0);
            // create dataset
            TestNodeController.PrimaryIndexInfo indexInfo = nc.createPrimaryIndex(dataset, KEY_TYPES, RECORD_TYPE,
                    META_TYPE, null, storageManager, KEY_INDEXES, KEY_INDICATORS_LIST, 0);
            IndexDataflowHelperFactory helperFactory =
                    new IndexDataflowHelperFactory(nc.getStorageManager(), indexInfo.getFileSplitProvider());
            JobId jobId = nc.newJobId();
            IHyracksTaskContext ctx = nc.createTestContext(jobId, 0, true);
            IIndexDataflowHelper dataflowHelper = helperFactory.create(ctx.getJobletContext().getServiceContext(), 0);
            dropInUse(ctx, helperFactory, dataflowHelper);
            dropInUseWithWait(ctx, helperFactory, dataflowHelper);
            dropNonExisting(ctx, helperFactory);
            dropNonExistingWithIfExists(ctx, helperFactory);
        } finally {
            nc.deInit();
        }
    }

    /**
     * Tests dropping an index whose dataset has no active
     * operations
     *
     * @throws Exception
     */
    @Test
    public void dropIndexInUseTest() throws Exception {
        TestNodeController nc = new TestNodeController(null, false);
        try {
            nc.init();
            String datasetName = "ds";
            String indexName = "fooIdx";
            // create dataset and index
            final TestCaseContext.OutputFormat format = TestCaseContext.OutputFormat.CLEAN_JSON;
            testExecutor.executeSqlppUpdateOrDdl("CREATE TYPE KeyType AS { id: int, foo: int };", format);
            testExecutor.executeSqlppUpdateOrDdl("CREATE DATASET " + datasetName + "(KeyType) PRIMARY KEY id;", format);
            testExecutor.executeSqlppUpdateOrDdl("CREATE INDEX " + indexName + " on " + datasetName + "(foo)", format);
            final MetadataTransactionContext mdTxn = MetadataManager.INSTANCE.beginTransaction();
            ICcApplicationContext appCtx = (ICcApplicationContext) ExecutionTestUtil.integrationUtil
                    .getClusterControllerService().getApplicationContext();
            MetadataProvider metadataProver = new MetadataProvider(appCtx, null);
            metadataProver.setMetadataTxnContext(mdTxn);
            final String defaultDv = MetadataBuiltinEntities.DEFAULT_DATAVERSE.getDataverseName();
            final Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxn, defaultDv, datasetName);
            MetadataManager.INSTANCE.commitTransaction(mdTxn);
            FileSplit[] splits = SplitsAndConstraintsUtil.getIndexSplits(appCtx.getClusterStateManager(), dataset,
                    indexName, Arrays.asList("asterix_nc1"));
            final ConstantFileSplitProvider constantFileSplitProvider =
                    new ConstantFileSplitProvider(Arrays.copyOfRange(splits, 0, 1));
            IndexDataflowHelperFactory helperFactory =
                    new IndexDataflowHelperFactory(nc.getStorageManager(), constantFileSplitProvider);
            JobId jobId = nc.newJobId();
            IHyracksTaskContext ctx = nc.createTestContext(jobId, 0, true);
            IIndexDataflowHelper dataflowHelper = helperFactory.create(ctx.getJobletContext().getServiceContext(), 0);
            dropInUse(ctx, helperFactory, dataflowHelper);
        } finally {
            nc.deInit();
        }
    }

    private void dropInUse(IHyracksTaskContext ctx, IndexDataflowHelperFactory helperFactory,
            IIndexDataflowHelper dataflowHelper) throws Exception {
        dropFailed.set(false);
        // open the index to make it in-use
        dataflowHelper.open();
        // try to drop in-use index (should fail)
        IndexDropOperatorNodePushable dropInUseOp =
                new IndexDropOperatorNodePushable(helperFactory, EnumSet.noneOf(DropOption.class), ctx, 0);
        try {
            dropInUseOp.initialize();
        } catch (HyracksDataException e) {
            e.printStackTrace();
            Assert.assertEquals(ErrorCode.CANNOT_DROP_IN_USE_INDEX, e.getErrorCode());
            dropFailed.set(true);
        }
        Assert.assertTrue(dropFailed.get());
    }

    private void dropInUseWithWait(IHyracksTaskContext ctx, IndexDataflowHelperFactory helperFactory,
            IIndexDataflowHelper dataflowHelper) throws Exception {
        dropFailed.set(false);
        // drop with option wait for in-use should be successful once the index is closed
        final IndexDropOperatorNodePushable dropWithWaitOp = new IndexDropOperatorNodePushable(helperFactory,
                EnumSet.of(DropOption.IF_EXISTS, DropOption.WAIT_ON_IN_USE), ctx, 0);
        Thread dropThread = new Thread(() -> {
            try {
                dropWithWaitOp.initialize();
            } catch (HyracksDataException e) {
                dropFailed.set(true);
                e.printStackTrace();
            }
        });
        dropThread.start();
        // wait for the drop thread to start
        while (dropThread.getState() == Thread.State.NEW) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        // close the index to allow the drop to complete
        dataflowHelper.close();
        dropThread.join();
        Assert.assertFalse(dropFailed.get());
    }

    private void dropNonExisting(IHyracksTaskContext ctx, IndexDataflowHelperFactory helperFactory) throws Exception {
        dropFailed.set(false);
        // Dropping non-existing index
        IndexDropOperatorNodePushable dropNonExistingOp =
                new IndexDropOperatorNodePushable(helperFactory, EnumSet.noneOf(DropOption.class), ctx, 0);
        try {
            dropNonExistingOp.initialize();
        } catch (HyracksDataException e) {
            e.printStackTrace();
            Assert.assertEquals(ErrorCode.INDEX_DOES_NOT_EXIST, e.getErrorCode());
            dropFailed.set(true);
        }
        Assert.assertTrue(dropFailed.get());
    }

    private void dropNonExistingWithIfExists(IHyracksTaskContext ctx, IndexDataflowHelperFactory helperFactory)
            throws Exception {
        // Dropping non-existing index with if exists option should be successful
        dropFailed.set(false);
        IndexDropOperatorNodePushable dropNonExistingWithIfExistsOp =
                new IndexDropOperatorNodePushable(helperFactory, EnumSet.of(DropOption.IF_EXISTS), ctx, 0);
        try {
            dropNonExistingWithIfExistsOp.initialize();
        } catch (HyracksDataException e) {
            e.printStackTrace();
            dropFailed.set(true);
        }
        Assert.assertFalse(dropFailed.get());
    }
}
