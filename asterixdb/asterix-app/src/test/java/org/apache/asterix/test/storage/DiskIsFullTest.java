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

import static org.apache.hyracks.util.StorageUtil.StorageUnit.MEGABYTE;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.data.gen.TupleGenerator;
import org.apache.asterix.app.data.gen.TupleGenerator.GenerationFunction;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.dataflow.LSMInsertDeleteOperatorNodePushable;
import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.common.transactions.ITransactionContext;
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
import org.apache.commons.lang3.SystemUtils;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.apache.hyracks.util.DiskUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DiskIsFullTest {

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
    private static final int[] KEY_INDICATOR = { Index.RECORD_INDICATOR };
    private static final List<Integer> KEY_INDICATOR_LIST = Arrays.asList(new Integer[] { Index.RECORD_INDICATOR });
    private static final int DATASET_ID = 101;
    private static final String DATAVERSE_NAME = "TestDV";
    private static final String DATASET_NAME = "TestDS";
    private static final String DATA_TYPE_NAME = "DUMMY";
    private static final String NODE_GROUP_NAME = "DEFAULT";
    private static final String TEST_DISK_NAME = "asterixdb_ram_disk";
    private boolean shouldRun = true;

    @Before
    public void setUp() throws Exception {
        if (!SystemUtils.IS_OS_MAC) {
            System.out.println("Skipping test " + DiskIsFullTest.class.getName() + " due to unsupported OS");
            shouldRun = false;
            return;
        }
        System.out.println("SetUp: ");
        TestHelper.deleteExistingInstanceFiles();
        // create RAM disk
        final Path ramDiskRoot = DiskUtil.mountRamDisk(TEST_DISK_NAME, 4, MEGABYTE);
        // Use RAM disk for storage
        AsterixHyracksIntegrationUtil.setStoragePath(ramDiskRoot.toAbsolutePath().toString());
    }

    @After
    public void tearDown() throws Exception {
        if (!shouldRun) {
            return;
        }
        System.out.println("TearDown");
        TestHelper.deleteExistingInstanceFiles();
        DiskUtil.unmountRamDisk(TEST_DISK_NAME);
        AsterixHyracksIntegrationUtil.restoreDefaultStoragePath();
    }

    @Test
    public void testDiskIsFull() {
        if (!shouldRun) {
            return;
        }
        HyracksDataException expectedException =
                HyracksDataException.create(ErrorCode.CANNOT_MODIFY_INDEX_DISK_IS_FULL);
        try {
            TestNodeController nc = new TestNodeController(null, false);
            nc.init();
            StorageComponentProvider storageManager = new StorageComponentProvider();
            List<List<String>> partitioningKeys = new ArrayList<>();
            partitioningKeys.add(Collections.singletonList("key"));
            Dataset dataset =
                    new Dataset(DATAVERSE_NAME, DATASET_NAME, DATAVERSE_NAME, DATA_TYPE_NAME, NODE_GROUP_NAME, null,
                            null,
                            new InternalDatasetDetails(null, PartitioningStrategy.HASH, partitioningKeys, null, null,
                                    null, false, null, false), null, DatasetType.INTERNAL, DATASET_ID, 0);
            try {
                nc.createPrimaryIndex(dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, new NoMergePolicyFactory(), null,
                        null, storageManager, KEY_INDEXES, KEY_INDICATOR_LIST);
                IHyracksTaskContext ctx = nc.createTestContext(false);
                nc.newJobId();
                ITransactionContext txnCtx = nc.getTransactionManager().getTransactionContext(nc.getTxnJobId(), true);
                // Prepare insert operation
                LSMInsertDeleteOperatorNodePushable insertOp =
                        nc.getInsertPipeline(ctx, dataset, KEY_TYPES, RECORD_TYPE, META_TYPE,
                                new NoMergePolicyFactory(), null, null, KEY_INDEXES, KEY_INDICATOR_LIST, storageManager)
                                .getLeft();
                insertOp.open();
                TupleGenerator tupleGenerator =
                        new TupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATOR, RECORD_GEN_FUNCTION,
                                UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
                VSizeFrame frame = new VSizeFrame(ctx);
                FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
                // Insert records until disk becomes full
                int tupleCount = 100000;
                while (tupleCount > 0) {
                    ITupleReference tuple = tupleGenerator.next();
                    try {
                        DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
                    } catch (Throwable t) {
                        final Throwable rootCause = ExceptionUtils.getRootCause(t);
                        rootCause.printStackTrace();
                        if (rootCause instanceof HyracksDataException) {
                            HyracksDataException cause = (HyracksDataException) rootCause;
                            Assert.assertEquals(cause.getErrorCode(), expectedException.getErrorCode());
                            Assert.assertEquals(cause.getMessage(), expectedException.getMessage());
                            return;
                        } else {
                            break;
                        }
                    }
                    tupleCount--;
                }
                Assert.fail("Expected exception (" + expectedException + ") was not thrown");
            } finally {
                nc.deInit();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail("Expected exception (" + expectedException + ") was not thrown");
        }
    }
}