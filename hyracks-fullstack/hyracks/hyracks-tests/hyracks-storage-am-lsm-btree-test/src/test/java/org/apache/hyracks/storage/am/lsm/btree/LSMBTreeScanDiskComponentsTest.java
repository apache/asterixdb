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

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestContext;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestDriver;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestUtils;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestContext;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

@SuppressWarnings("rawtypes")
public class LSMBTreeScanDiskComponentsTest extends OrderedIndexTestDriver {

    private final OrderedIndexTestUtils orderedIndexTestUtils;

    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();

    public LSMBTreeScanDiskComponentsTest() {
        super(LSMBTreeTestHarness.LEAF_FRAMES_TO_TEST);
        this.orderedIndexTestUtils = new OrderedIndexTestUtils();

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
    protected OrderedIndexTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            BTreeLeafFrameType leafType, boolean filtered) throws Exception {
        return LSMBTreeTestContext.create(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), fieldSerdes, numKeys,
                harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory(), false, true, false);
    }

    @Override
    protected Random getRandom() {
        return harness.getRandom();
    }

    @Override
    protected void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, BTreeLeafFrameType leafType,
            ITupleReference lowKey, ITupleReference highKey, ITupleReference prefixLowKey,
            ITupleReference prefixHighKey) throws Exception {
        OrderedIndexTestContext ctx = createTestContext(fieldSerdes, numKeys, leafType, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();
        // We assume all fieldSerdes are of the same type. Check the first one
        // to determine which field types to generate.
        if (fieldSerdes[0] instanceof IntegerSerializerDeserializer) {
            test(ctx, fieldSerdes);
        } else if (fieldSerdes[0] instanceof UTF8StringSerializerDeserializer) {
            test(ctx, fieldSerdes);
        }

        ctx.getIndex().validate();
        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    protected void test(OrderedIndexTestContext ctx, ISerializerDeserializer[] fieldSerdes)
            throws HyracksDataException {
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) ctx.getIndexAccessor();
        //component 2 contains 1 and 2
        upsertTuple(ctx, fieldSerdes, getValue(1, fieldSerdes));
        upsertTuple(ctx, fieldSerdes, getValue(2, fieldSerdes));
        accessor.scheduleFlush();

        //component 1 contains 1 and -2
        upsertTuple(ctx, fieldSerdes, getValue(1, fieldSerdes));
        deleteTuple(ctx, fieldSerdes, getValue(2, fieldSerdes));
        accessor.scheduleFlush();

        //component 0 contains 2 and 3
        upsertTuple(ctx, fieldSerdes, getValue(3, fieldSerdes));
        upsertTuple(ctx, fieldSerdes, getValue(2, fieldSerdes));
        accessor.scheduleFlush();

        LSMBTree btree = (LSMBTree) ctx.getIndex();
        Assert.assertEquals("Check disk components", 3, btree.getDiskComponents().size());

        IIndexCursor cursor = accessor.createSearchCursor(false);
        accessor.scanDiskComponents(cursor);

        ITupleReference tuple = getNext(cursor);
        checkReturnedTuple(ctx, tuple, fieldSerdes, 2, false, getValue(1, fieldSerdes));

        tuple = getNext(cursor);
        checkReturnedTuple(ctx, tuple, fieldSerdes, 1, false, getValue(1, fieldSerdes));

        tuple = getNext(cursor);
        checkReturnedTuple(ctx, tuple, fieldSerdes, 2, false, getValue(2, fieldSerdes));

        tuple = getNext(cursor);
        checkReturnedTuple(ctx, tuple, fieldSerdes, 1, true, getValue(2, fieldSerdes));

        tuple = getNext(cursor);
        checkReturnedTuple(ctx, tuple, fieldSerdes, 0, false, getValue(2, fieldSerdes));

        tuple = getNext(cursor);
        checkReturnedTuple(ctx, tuple, fieldSerdes, 0, false, getValue(3, fieldSerdes));

        Assert.assertFalse(cursor.hasNext());
    }

    protected void checkReturnedTuple(OrderedIndexTestContext ctx, ITupleReference tuple,
            ISerializerDeserializer[] fieldSerdes, int componentPos, boolean antimatter, Object value)
            throws HyracksDataException {
        int actualComponentPos = IntegerPointable.getInteger(tuple.getFieldData(0), tuple.getFieldStart(0));
        Assert.assertEquals("Check returned component position", componentPos, actualComponentPos);

        boolean actualAntiMatter = BooleanPointable.getBoolean(tuple.getFieldData(1), tuple.getFieldStart(1));
        Assert.assertEquals("Check returned anti-matter flag", antimatter, actualAntiMatter);

        int[] permutation = new int[ctx.getFieldCount()];
        for (int i = 0; i < permutation.length; i++) {
            permutation[i] = i + 2;
        }

        PermutingTupleReference originalTuple = new PermutingTupleReference(permutation);
        originalTuple.reset(tuple);

        for (int i = 0; i < fieldSerdes.length; i++) {
            ByteArrayInputStream inStream = new ByteArrayInputStream(originalTuple.getFieldData(i),
                    originalTuple.getFieldStart(i), originalTuple.getFieldLength(i));
            DataInput dataIn = new DataInputStream(inStream);
            Object actualObj = fieldSerdes[i].deserialize(dataIn);
            if (!actualObj.equals(value)) {
                fail("Actual and expected fields do not match on field " + i + ".\nExpected: " + value + "\nActual  : "
                        + actualObj);
            }
        }

    }

    protected ITupleReference getNext(IIndexCursor cursor) throws HyracksDataException {
        Assert.assertTrue(cursor.hasNext());
        cursor.next();
        return cursor.getTuple();
    }

    @Override
    protected String getTestOpName() {
        return "Disk Components Scan";
    }

    protected void upsertTuple(OrderedIndexTestContext ctx, ISerializerDeserializer[] fieldSerdes, Object value)
            throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(ctx.getFieldCount());
        for (int i = 0; i < ctx.getFieldCount(); i++) {
            tupleBuilder.addField(fieldSerdes[i], value);
        }
        ArrayTupleReference tuple = new ArrayTupleReference();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        try {
            ctx.getIndexAccessor().upsert(tuple);
        } catch (HyracksDataException e) {
            if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                throw e;
            }
        }
    }

    protected void deleteTuple(OrderedIndexTestContext ctx, ISerializerDeserializer[] fieldSerdes, Object value)
            throws HyracksDataException {
        ArrayTupleBuilder deletedBuilder = new ArrayTupleBuilder(ctx.getKeyFieldCount());
        for (int i = 0; i < ctx.getKeyFieldCount(); i++) {
            deletedBuilder.addField(fieldSerdes[i], value);
        }
        ArrayTupleReference deleteTuple = new ArrayTupleReference();
        deleteTuple.reset(deletedBuilder.getFieldEndOffsets(), deletedBuilder.getByteArray());
        try {
            ctx.getIndexAccessor().delete(deleteTuple);
        } catch (HyracksDataException e) {
            if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                throw e;
            }
        }
    }

    protected Object getValue(Object value, ISerializerDeserializer[] fieldSerdes) {
        if (fieldSerdes[0] instanceof IntegerSerializerDeserializer) {
            return value;
        } else if (fieldSerdes[0] instanceof UTF8StringSerializerDeserializer) {
            return String.valueOf(value);
        } else {
            return null;
        }
    }
}
