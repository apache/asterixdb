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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.data.gen.RecordTupleGenerator;
import org.apache.asterix.app.data.gen.RecordTupleGenerator.GenerationFunction;
import org.apache.asterix.app.data.gen.TestTupleCounterFrameWriter;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
import org.apache.asterix.test.common.TestHelper;
import org.apache.asterix.transaction.management.service.logging.LogReader;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.test.CountAnswer;
import org.apache.hyracks.api.test.FrameWriterTestUtils;
import org.apache.hyracks.api.test.FrameWriterTestUtils.FrameWriterOperation;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.io.MessagingFrameTupleAppender;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.util.ComponentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LogMarkerTest {

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
    private static final int NUM_OF_RECORDS = 100000;
    private static final int SNAPSHOT_SIZE = 1000;
    private static final int DATASET_ID = 101;
    private static final String DATAVERSE_NAME = "TestDV";
    private static final String DATASET_NAME = "TestDS";
    private static final String DATA_TYPE_NAME = "DUMMY";
    private static final String NODE_GROUP_NAME = "DEFAULT";

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

    @Test
    public void testInsertWithSnapshot() {
        try {
            TestNodeController nc = new TestNodeController(null, false);
            nc.init();
            StorageComponentProvider storageManager = new StorageComponentProvider();
            List<List<String>> partitioningKeys = new ArrayList<>();
            partitioningKeys.add(Collections.singletonList("key"));
            try {
                PrimaryIndexInfo indexInfo = nc.createPrimaryIndex(StorageTestUtils.DATASET, KEY_TYPES, RECORD_TYPE,
                        META_TYPE, null, storageManager, KEY_INDEXES, KEY_INDICATORS_LIST, 0);
                JobId jobId = nc.newJobId();
                IHyracksTaskContext ctx = nc.createTestContext(jobId, 0, true);
                ITransactionContext txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(ctx),
                        new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
                LSMPrimaryInsertOperatorNodePushable insertOp =
                        nc.getInsertPipeline(ctx, StorageTestUtils.DATASET, KEY_TYPES, RECORD_TYPE, META_TYPE, null,
                                KEY_INDEXES, KEY_INDICATORS_LIST, storageManager, null, null).getLeft();
                insertOp.open();
                RecordTupleGenerator tupleGenerator =
                        new RecordTupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATORS,
                                RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
                VSizeFrame frame = new VSizeFrame(ctx);
                VSizeFrame marker = new VSizeFrame(ctx);
                FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
                long markerId = 0L;
                for (int j = 0; j < NUM_OF_RECORDS; j++) {
                    if (j % SNAPSHOT_SIZE == 0) {
                        marker.reset();
                        marker.getBuffer().put(MessagingFrameTupleAppender.MARKER_MESSAGE);
                        marker.getBuffer().putLong(markerId);
                        marker.getBuffer().flip();
                        markerId++;
                        TaskUtil.put(HyracksConstants.KEY_MESSAGE, marker, ctx);
                        tupleAppender.flush(insertOp);
                    }
                    ITupleReference tuple = tupleGenerator.next();
                    DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
                }
                if (tupleAppender.getTupleCount() > 0) {
                    tupleAppender.write(insertOp, true);
                }
                insertOp.close();
                nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
                IndexDataflowHelperFactory iHelperFactory =
                        new IndexDataflowHelperFactory(nc.getStorageManager(), indexInfo.getFileSplitProvider());
                IIndexDataflowHelper dataflowHelper =
                        iHelperFactory.create(ctx.getJobletContext().getServiceContext(), 0);
                dataflowHelper.open();
                LSMBTree btree = (LSMBTree) dataflowHelper.getIndexInstance();
                ArrayBackedValueStorage buffer = new ArrayBackedValueStorage();

                ComponentUtils.get(btree, ComponentUtils.MARKER_LSN_KEY, buffer);
                long lsn = LongPointable.getLong(buffer.getByteArray(), buffer.getStartOffset());
                int numOfMarkers = 0;
                LogReader logReader = (LogReader) nc.getTransactionSubsystem().getLogManager().getLogReader(false);
                long expectedMarkerId = markerId - 1;
                while (lsn >= 0) {
                    numOfMarkers++;
                    ILogRecord logRecord = logReader.read(lsn);
                    lsn = logRecord.getPreviousMarkerLSN();
                    long logMarkerId = logRecord.getMarker().getLong();
                    Assert.assertEquals(expectedMarkerId, logMarkerId);
                    expectedMarkerId--;
                }
                logReader.close();
                dataflowHelper.close();
                Assert.assertEquals(markerId, numOfMarkers);
                nc.newJobId();
                TestTupleCounterFrameWriter countOp = create(nc.getSearchOutputDesc(KEY_TYPES, RECORD_TYPE, META_TYPE),
                        Collections.emptyList(), Collections.emptyList(), false);
                IPushRuntime emptyTupleOp = nc.getFullScanPipeline(countOp, ctx, StorageTestUtils.DATASET, KEY_TYPES,
                        RECORD_TYPE, META_TYPE, new NoMergePolicyFactory(), null, null, KEY_INDEXES,
                        KEY_INDICATORS_LIST, storageManager);
                emptyTupleOp.open();
                emptyTupleOp.close();
                Assert.assertEquals(NUM_OF_RECORDS, countOp.getCount());
            } finally {
                nc.deInit();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

    }

    public static TestTupleCounterFrameWriter create(RecordDescriptor recordDescriptor,
            Collection<FrameWriterOperation> exceptionThrowingOperations,
            Collection<FrameWriterOperation> errorThrowingOperations, boolean deepCopyInputFrames) {
        CountAnswer openAnswer = FrameWriterTestUtils.createAnswer(FrameWriterOperation.Open,
                exceptionThrowingOperations, errorThrowingOperations);
        CountAnswer nextAnswer = FrameWriterTestUtils.createAnswer(FrameWriterOperation.NextFrame,
                exceptionThrowingOperations, errorThrowingOperations);
        CountAnswer flushAnswer = FrameWriterTestUtils.createAnswer(FrameWriterOperation.Flush,
                exceptionThrowingOperations, errorThrowingOperations);
        CountAnswer failAnswer = FrameWriterTestUtils.createAnswer(FrameWriterOperation.Fail,
                exceptionThrowingOperations, errorThrowingOperations);
        CountAnswer closeAnswer = FrameWriterTestUtils.createAnswer(FrameWriterOperation.Close,
                exceptionThrowingOperations, errorThrowingOperations);
        return new TestTupleCounterFrameWriter(recordDescriptor, openAnswer, nextAnswer, flushAnswer, failAnswer,
                closeAnswer, deepCopyInputFrames);
    }
}
