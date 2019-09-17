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

package org.apache.hyracks.storage.am.lsm.btree;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestContext;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestDriver;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestUtils;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.common.IIndexTestContext;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestContext;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

@SuppressWarnings("rawtypes")
public class LSMBTreeUpdateInPlaceScanDiskComponentsTest extends OrderedIndexTestDriver {

    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();

    protected final TreeSet<UpdatedCheckTuple> checkTuples = new TreeSet<>();
    private boolean hasOnlyKeys;

    class UpdatedCheckTuple<T extends Comparable<T>> extends CheckTuple<T> {

        private boolean isUpdated = false;
        private boolean antimatter = false;

        public UpdatedCheckTuple(int numFields, int numKeys) {
            super(numFields, numKeys);
        }

        public void setUpdated(boolean updated) {
            isUpdated = updated;
        }

        public void setAntimatter(boolean antimatter) {
            this.antimatter = antimatter;
        }

        public boolean isUpdated() {
            return isUpdated;
        }

        public boolean isAntimatter() {
            return antimatter;
        }
    }

    public LSMBTreeUpdateInPlaceScanDiskComponentsTest() {
        super(LSMBTreeTestHarness.LEAF_FRAMES_TO_TEST);
    }

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Override
    protected Random getRandom() {
        return harness.getRandom();
    }

    @Override
    protected String getTestOpName() {
        return "Update in-place disk Components Scan";
    }

    @Override
    protected OrderedIndexTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            BTreeLeafFrameType leafType, boolean filtered) throws HyracksDataException {
        return LSMBTreeTestContext.create(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), fieldSerdes, numKeys,
                harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory(), false, !hasOnlyKeys,
                hasOnlyKeys);
    }

    interface IndexModification {
        void modify(IIndexTestContext ctx, ITupleReference tuple, UpdatedCheckTuple checkTuple)
                throws HyracksDataException;
    }

    IndexModification insertModification =
            (IIndexTestContext ctx, ITupleReference tuple, UpdatedCheckTuple checkTuple) -> {
                try {
                    ctx.getIndexAccessor().insert(tuple);
                    checkTuples.add(checkTuple);
                } catch (HyracksDataException e) {
                    // We set expected values only after insertion succeeds because
                    // we ignore duplicate keys.
                    if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                        throw e;
                    }
                }
            };

    IndexModification upsertModification =
            (IIndexTestContext ctx, ITupleReference tuple, UpdatedCheckTuple checkTuple) -> {
                try {
                    ctx.getIndexAccessor().upsert(tuple);
                    SortedSet<UpdatedCheckTuple> existingSet = checkTuples.subSet(checkTuple, true, checkTuple, true);
                    if (existingSet.isEmpty()) {
                        checkTuples.add(checkTuple);
                    } else {
                        UpdatedCheckTuple existingTuple = existingSet.first();
                        checkTuples.remove(existingTuple);
                        checkTuple.setUpdated(!existingTuple.isUpdated());
                        checkTuples.add(checkTuple);
                    }
                } catch (HyracksDataException e) {
                    // We set expected values only after insertion succeeds because
                    // we ignore duplicate keys.
                    if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                        throw e;
                    }
                }
            };

    IndexModification deleteModification =
            (IIndexTestContext ctx, ITupleReference tuple, UpdatedCheckTuple checkTuple) -> {
                ctx.getIndexAccessor().delete(tuple);
                // Remove check tuple from expected results.
                if (!checkTuples.contains(checkTuple)) {
                    fail("Trying to delete tuple " + checkTuple + " that does not exist");
                }
                checkTuple.setUpdated(!checkTuple.isUpdated());
                checkTuple.setAntimatter(true);
            };

    abstract class TupleOperation {
        protected final IndexModification op;

        public TupleOperation(IndexModification op) {
            this.op = op;
        }

        public abstract void performOperation(IIndexTestContext ctx, int numTuples) throws HyracksDataException;
    }

    class GenerateTupleOperation extends TupleOperation {
        public GenerateTupleOperation(IndexModification op) {
            super(op);
        }

        @Override
        public void performOperation(IIndexTestContext ctx, int numTuples) throws HyracksDataException {
            int fieldCount = ctx.getFieldCount();
            int numKeyFields = ctx.getKeyFieldCount();
            // Scale range of values according to number of keys.
            // For example, for 2 keys we want the square root of numTuples, for 3
            // keys the cube root of numTuples, etc.
            int maxValue = (int) Math.ceil(Math.pow(numTuples, 1.0 / numKeyFields));
            for (int i = 0; i < numTuples; i++) {
                if (LOGGER.isInfoEnabled()) {
                    if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                        LOGGER.info("Generating Tuple " + (i + 1) + "/" + numTuples);
                    }
                }
                UpdatedCheckTuple newCheckTuple;
                if (ctx.getFieldSerdes()[0] instanceof IntegerSerializerDeserializer) {
                    newCheckTuple =
                            createIntTuple(fieldCount, numKeyFields, maxValue, ctx.getTupleBuilder(), ctx.getTuple());
                } else {
                    newCheckTuple = createStringTuple(ctx.getFieldSerdes(), fieldCount, numKeyFields,
                            ctx.getTupleBuilder(), ctx.getTuple());
                }
                op.modify(ctx, ctx.getTuple(), newCheckTuple);
            }
        }
    }

    private UpdatedCheckTuple createStringTuple(ISerializerDeserializer[] fieldSerdes, int fieldCount, int numKeyFields,
            ArrayTupleBuilder tupleBuilder, ArrayTupleReference tuple) throws HyracksDataException {
        String[] fieldValues = new String[fieldCount];
        // Set keys.
        for (int j = 0; j < numKeyFields; j++) {
            int length = (Math.abs(getRandom().nextInt()) % 10) + 1;
            fieldValues[j] = OrderedIndexTestUtils.getRandomString(length, getRandom());
        }
        // Set values.
        for (int j = numKeyFields; j < fieldCount; j++) {
            fieldValues[j] = OrderedIndexTestUtils.getRandomString(5, getRandom());
        }
        TupleUtils.createTuple(tupleBuilder, tuple, fieldSerdes, false, (Object[]) fieldValues);
        UpdatedCheckTuple<String> checkTuple = new UpdatedCheckTuple<>(fieldValues.length, numKeyFields);
        for (String s : fieldValues) {
            checkTuple.appendField(s);
        }
        return checkTuple;
    }

    private UpdatedCheckTuple createIntTuple(int fieldCount, int numKeyFields, int maxValue,
            ArrayTupleBuilder tupleBuilder, ArrayTupleReference tuple) throws HyracksDataException {
        int[] fieldValues = new int[fieldCount];
        // Set keys.
        for (int j = 0; j < numKeyFields; j++) {
            fieldValues[j] = getRandom().nextInt() % maxValue;
        }
        // Set values.
        for (int j = numKeyFields; j < fieldCount; j++) {
            fieldValues[j] = j;
        }
        TupleUtils.createIntegerTuple(tupleBuilder, tuple, fieldValues);
        UpdatedCheckTuple<Integer> checkTuple = new UpdatedCheckTuple<>(fieldValues.length, numKeyFields);
        for (int v : fieldValues) {
            checkTuple.appendField(v);
        }
        return checkTuple;
    }

    class DeleteTupleOperation extends TupleOperation {
        public DeleteTupleOperation(IndexModification op) {
            super(op);
        }

        @Override
        public void performOperation(IIndexTestContext ctx, int numTuples) throws HyracksDataException {
            ArrayTupleBuilder deleteTupleBuilder = new ArrayTupleBuilder(ctx.getKeyFieldCount());
            ArrayTupleReference deleteTuple = new ArrayTupleReference();
            int numCheckTuples = checkTuples.size();
            // Copy CheckTuple references into array, so we can randomly pick from there.
            UpdatedCheckTuple[] checkTuplesArray = new UpdatedCheckTuple[numCheckTuples];
            int idx = 0;
            for (UpdatedCheckTuple t : checkTuples) {
                checkTuplesArray[idx++] = t;
            }

            for (int i = 0; i < numTuples && numCheckTuples > 0; i++) {
                if (LOGGER.isInfoEnabled()) {
                    if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                        LOGGER.info("Deleting Tuple " + (i + 1) + "/" + numTuples);
                    }
                }
                int checkTupleIdx = Math.abs(getRandom().nextInt() % numCheckTuples);
                UpdatedCheckTuple checkTuple = checkTuplesArray[checkTupleIdx];
                OrderedIndexTestUtils.createTupleFromCheckTuple(checkTuple, deleteTupleBuilder, deleteTuple,
                        ctx.getFieldSerdes());

                op.modify(ctx, deleteTuple, checkTuple);

                // Swap with last "valid" CheckTuple.
                UpdatedCheckTuple tmp = checkTuplesArray[numCheckTuples - 1];
                checkTuplesArray[numCheckTuples - 1] = checkTuple;
                checkTuplesArray[checkTupleIdx] = tmp;
                numCheckTuples--;
            }
        }
    }

    private void insertDeleteTest(OrderedIndexTestContext ctx) throws HyracksDataException {
        test(ctx, new GenerateTupleOperation(insertModification), new DeleteTupleOperation(deleteModification));
    }

    private void upsertDeleteTest(OrderedIndexTestContext ctx) throws HyracksDataException {
        test(ctx, new GenerateTupleOperation(upsertModification), new DeleteTupleOperation(deleteModification));
    }

    private void insertUpsertTest(OrderedIndexTestContext ctx) throws HyracksDataException {
        test(ctx, new GenerateTupleOperation(insertModification), new GenerateTupleOperation(upsertModification));
    }

    private void upsertUpsertTest(OrderedIndexTestContext ctx) throws HyracksDataException {
        test(ctx, new GenerateTupleOperation(upsertModification), new GenerateTupleOperation(upsertModification));
    }

    @Override
    protected void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, BTreeLeafFrameType leafType,
            ITupleReference lowKey, ITupleReference highKey, ITupleReference prefixLowKey,
            ITupleReference prefixHighKey) throws Exception {
        //tests with tuples consisting only of keys create secondary index, others - a primary index
        hasOnlyKeys = fieldSerdes.length == numKeys;

        OrderedIndexTestContext ctx = createTestContext(fieldSerdes, numKeys, leafType, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        insertDeleteTest(ctx);
        ctx.getIndex().clear();
        checkTuples.clear();

        upsertDeleteTest(ctx);
        ctx.getIndex().clear();
        checkTuples.clear();

        insertUpsertTest(ctx);
        ctx.getIndex().clear();
        checkTuples.clear();

        upsertUpsertTest(ctx);
        ctx.getIndex().clear();
        checkTuples.clear();

        ctx.getIndex().validate();
        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    protected void test(OrderedIndexTestContext ctx, TupleOperation op1, TupleOperation op2)
            throws HyracksDataException {

        ILSMIndexAccessor accessor = (ILSMIndexAccessor) ctx.getIndexAccessor();
        op1.performOperation(ctx, AccessMethodTestsConfig.BTREE_NUM_TUPLES_TO_INSERT);
        op2.performOperation(ctx,
                AccessMethodTestsConfig.BTREE_NUM_TUPLES_TO_INSERT / AccessMethodTestsConfig.BTREE_NUM_INSERT_ROUNDS);
        accessor.scheduleFlush();
        LSMBTree btree = (LSMBTree) ctx.getIndex();
        Assert.assertEquals("Check disk components", 1, btree.getDiskComponents().size());

        ILSMDiskComponent btreeComponent = btree.getDiskComponents().get(0);
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        BTree.BTreeAccessor btreeAccessor = ((BTree) btreeComponent.getIndex()).createAccessor(actx);

        ITreeIndexCursor cursor = btreeAccessor.createDiskOrderScanCursor();
        try {
            btreeAccessor.diskOrderScan(cursor);
            try {
                for (UpdatedCheckTuple t : checkTuples) {
                    if (!t.isUpdated() || !hasOnlyKeys) {
                        checkReturnedTuple((LSMBTreeTupleReference) getNext(cursor), ctx.getFieldSerdes(), t,
                                ctx.getKeyFieldCount());
                    }
                }
                Assert.assertFalse(cursor.hasNext());
            } finally {
                cursor.close();
            }
        } finally {
            cursor.destroy();
        }
    }

    protected void checkReturnedTuple(LSMBTreeTupleReference tuple, ISerializerDeserializer[] fieldSerdes,
            UpdatedCheckTuple checkTuple, int numKeys) throws HyracksDataException {
        Assert.assertEquals("Check tuple anti-matter flag", checkTuple.isAntimatter(), tuple.isAntimatter());
        //check keys
        for (int i = 0; i < numKeys; i++) {
            ByteArrayInputStream inStream =
                    new ByteArrayInputStream(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            DataInput dataIn = new DataInputStream(inStream);
            Object actualObj = fieldSerdes[i].deserialize(dataIn);
            Assert.assertEquals("Actual and expected keys do not match on key " + i, checkTuple.getField(i), actualObj);
        }
        //check the rest of fields only if the tuple is not anti-matter
        if (!tuple.isAntimatter()) {
            for (int i = numKeys; i < fieldSerdes.length; i++) {
                ByteArrayInputStream inStream = new ByteArrayInputStream(tuple.getFieldData(i), tuple.getFieldStart(i),
                        tuple.getFieldLength(i));
                DataInput dataIn = new DataInputStream(inStream);
                Object actualObj = fieldSerdes[i].deserialize(dataIn);
                Assert.assertEquals("Actual and expected fields do not match on field " + i, checkTuple.getField(i),
                        actualObj);
            }
        }
    }

    protected ITupleReference getNext(IIndexCursor cursor) throws HyracksDataException {
        Assert.assertTrue(cursor.hasNext());
        cursor.next();
        return cursor.getTuple();
    }
}
