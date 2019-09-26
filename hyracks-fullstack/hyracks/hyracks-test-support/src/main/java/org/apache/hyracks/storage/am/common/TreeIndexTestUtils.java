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

package org.apache.hyracks.storage.am.common;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.MergeOperation;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;

@SuppressWarnings("rawtypes")
public abstract class TreeIndexTestUtils {
    private static final Logger LOGGER = LogManager.getLogger();

    protected abstract CheckTuple createCheckTuple(int numFields, int numKeyFields);

    protected abstract ISearchPredicate createNullSearchPredicate();

    public abstract void checkExpectedResults(IIndexCursor cursor, Collection checkTuples,
            ISerializerDeserializer[] fieldSerdes, int keyFieldCount, Iterator<CheckTuple> checkIter) throws Exception;

    protected abstract CheckTuple createIntCheckTuple(int[] fieldValues, int numKeyFields);

    protected abstract void setIntKeyFields(int[] fieldValues, int numKeyFields, int maxValue, Random rnd);

    protected abstract void setIntPayloadFields(int[] fieldValues, int numKeyFields, int numFields);

    protected abstract Collection createCheckTuplesCollection();

    protected abstract ArrayTupleBuilder createDeleteTupleBuilder(IIndexTestContext ctx);

    // See if tuple with corresponding checkTuple exists in ctx.checkTuples.
    protected abstract boolean checkDiskOrderScanResult(ITupleReference tuple, CheckTuple checkTuple,
            IIndexTestContext ctx) throws HyracksDataException;

    public static int compareFilterTuples(ITupleReference lhs, ITupleReference rhs, IBinaryComparator comp)
            throws HyracksDataException {
        return comp.compare(lhs.getFieldData(0), lhs.getFieldStart(0), lhs.getFieldLength(0), rhs.getFieldData(0),
                rhs.getFieldStart(0), rhs.getFieldLength(0));
    }

    public static void createTupleFromCheckTuple(CheckTuple checkTuple, ArrayTupleBuilder tupleBuilder,
            ArrayTupleReference tuple, ISerializerDeserializer[] fieldSerdes) throws HyracksDataException {
        createTupleFromCheckTuple(checkTuple, tupleBuilder, tuple, fieldSerdes, false);
    }

    @SuppressWarnings("unchecked")
    public static void createTupleFromCheckTuple(CheckTuple checkTuple, ArrayTupleBuilder tupleBuilder,
            ArrayTupleReference tuple, ISerializerDeserializer[] fieldSerdes, boolean filtered)
            throws HyracksDataException {
        int fieldCount = tupleBuilder.getFieldEndOffsets().length;
        DataOutput dos = tupleBuilder.getDataOutput();
        tupleBuilder.reset();
        for (int i = 0; i < (filtered ? fieldCount - 1 : fieldCount); i++) {
            fieldSerdes[i].serialize(checkTuple.getField(i), dos);
            tupleBuilder.addFieldEndOffset();
        }
        if (filtered) {
            fieldSerdes[0].serialize(checkTuple.getField(0), dos);
            tupleBuilder.addFieldEndOffset();
        }
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }

    @SuppressWarnings("unchecked")
    public CheckTuple createCheckTupleFromTuple(ITupleReference tuple, ISerializerDeserializer[] fieldSerdes,
            int numKeys) throws HyracksDataException {
        CheckTuple checkTuple = createCheckTuple(fieldSerdes.length, numKeys);
        int fieldCount = Math.min(fieldSerdes.length, tuple.getFieldCount());
        for (int i = 0; i < fieldCount; i++) {
            ByteArrayInputStream inStream =
                    new ByteArrayInputStream(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            DataInput dataIn = new DataInputStream(inStream);
            Comparable fieldObj = (Comparable) fieldSerdes[i].deserialize(dataIn);
            checkTuple.appendField(fieldObj);
        }
        return checkTuple;
    }

    @SuppressWarnings("unchecked")
    public void checkScan(IIndexTestContext ctx) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Scan.");
        }
        IIndexCursor scanCursor = ctx.getIndexAccessor().createSearchCursor(false);
        try {
            ISearchPredicate nullPred = createNullSearchPredicate();
            ctx.getIndexAccessor().search(scanCursor, nullPred);
            try {
                Iterator<CheckTuple> checkIter = ctx.getCheckTuples().iterator();
                checkExpectedResults(scanCursor, ctx.getCheckTuples(), ctx.getFieldSerdes(), ctx.getKeyFieldCount(),
                        checkIter);
            } finally {
                scanCursor.close();
            }
        } finally {
            scanCursor.destroy();
        }
    }

    public void checkDiskOrderScan(IIndexTestContext ctx) throws Exception {
        try {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Testing Disk-Order Scan.");
            }
            ITreeIndexAccessor treeIndexAccessor = (ITreeIndexAccessor) ctx.getIndexAccessor();
            try {
                ITreeIndexCursor diskOrderCursor = treeIndexAccessor.createDiskOrderScanCursor();
                try {
                    int actualCount = 0;
                    treeIndexAccessor.diskOrderScan(diskOrderCursor);
                    try {
                        actualCount = scan(ctx, diskOrderCursor);
                    } finally {
                        diskOrderCursor.close();
                    }
                    if (actualCount < ctx.getCheckTuples().size()) {
                        fail("Disk-order scan returned fewer answers than expected.\nExpected: "
                                + ctx.getCheckTuples().size() + "\nActual  : " + actualCount);
                    }
                    if (actualCount > ctx.getCheckTuples().size()) {
                        fail("Disk-order scan returned more answers than expected.\nExpected: "
                                + ctx.getCheckTuples().size() + "\nActual  : " + actualCount);
                    }
                } finally {
                    diskOrderCursor.destroy();
                }
            } finally {
                treeIndexAccessor.destroy();
            }
        } catch (UnsupportedOperationException e) {
            // Ignore exception because some indexes, e.g. the LSMTrees, don't
            // support disk-order scan.
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Ignoring disk-order scan since it's not supported.");
            }
        } catch (ClassCastException e) {
            // Ignore exception because IIndexAccessor sometimes isn't
            // an ITreeIndexAccessor, e.g., for the LSMBTree.
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Ignoring disk-order scan since it's not supported.");
            }
        }
    }

    private int scan(IIndexTestContext ctx, ITreeIndexCursor diskOrderCursor) throws HyracksDataException {
        int actualCount = 0;
        while (diskOrderCursor.hasNext()) {
            diskOrderCursor.next();
            ITupleReference tuple = diskOrderCursor.getTuple();
            CheckTuple checkTuple = createCheckTupleFromTuple(tuple, ctx.getFieldSerdes(), ctx.getKeyFieldCount());
            if (!checkDiskOrderScanResult(tuple, checkTuple, ctx)) {
                fail("Disk-order scan returned unexpected answer: " + checkTuple.toString());
            }
            actualCount++;
        }
        return actualCount;
    }

    public Pair<ITupleReference, ITupleReference> insertIntTuples(IIndexTestContext ctx, int numTuples, Random rnd)
            throws Exception {
        return insertIntTuples(ctx, numTuples, false, rnd);
    }

    public Pair<ITupleReference, ITupleReference> insertIntTuples(IIndexTestContext ctx, int numTuples,
            boolean filtered, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        int[] fieldValues = new int[ctx.getFieldCount()];
        // Scale range of values according to number of keys.
        // For example, for 2 keys we want the square root of numTuples, for 3
        // keys the cube root of numTuples, etc.
        int maxValue = (int) Math.ceil(Math.pow(numTuples, 1.0 / numKeyFields));
        MutablePair<ITupleReference, ITupleReference> minMax = null;
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            setIntKeyFields(fieldValues, numKeyFields, maxValue, rnd);
            // Set values.
            setIntPayloadFields(fieldValues, numKeyFields, fieldCount);
            TupleUtils.createIntegerTuple(ctx.getTupleBuilder(), ctx.getTuple(), filtered, fieldValues);
            if (LOGGER.isInfoEnabled()) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Inserting Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            try {
                ctx.getIndexAccessor().insert(ctx.getTuple());
                ctx.insertCheckTuple(createIntCheckTuple(fieldValues, ctx.getKeyFieldCount()), ctx.getCheckTuples());
                if (filtered) {
                    addFilterField(ctx, minMax);
                }
            } catch (HyracksDataException e) {
                // We set expected values only after insertion succeeds because
                // we ignore duplicate keys.
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    throw e;
                }
            }
        }
        return minMax;
    }

    protected void addFilterField(IIndexTestContext ctx, MutablePair<ITupleReference, ITupleReference> minMax)
            throws HyracksDataException {
        //Duplicate the PK field as a filter field at the end of the tuple to be inserted.
        int filterField = ctx.getFieldCount();
        ITupleReference currTuple = ctx.getTuple();
        ArrayTupleBuilder filterBuilder = new ArrayTupleBuilder(1);
        filterBuilder.addField(currTuple.getFieldData(filterField), currTuple.getFieldStart(filterField),
                currTuple.getFieldLength(filterField));
        IBinaryComparator comparator = ctx.getComparatorFactories()[0].createBinaryComparator();
        ArrayTupleReference filterOnlyTuple = new ArrayTupleReference();
        filterOnlyTuple.reset(filterBuilder.getFieldEndOffsets(), filterBuilder.getByteArray());
        if (minMax == null) {
            minMax = MutablePair.of(filterOnlyTuple, filterOnlyTuple);
        } else if (compareFilterTuples(minMax.getLeft(), filterOnlyTuple, comparator) > 0) {
            minMax.setLeft(filterOnlyTuple);
        } else if (compareFilterTuples(minMax.getRight(), filterOnlyTuple, comparator) < 0) {
            minMax.setRight(filterOnlyTuple);
        }
    }

    @SuppressWarnings("unchecked")
    public void upsertIntTuples(IIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        int[] fieldValues = new int[ctx.getFieldCount()];
        // Scale range of values according to number of keys.
        // For example, for 2 keys we want the square root of numTuples, for 3
        // keys the cube root of numTuples, etc.
        int maxValue = (int) Math.ceil(Math.pow(numTuples, 1.0 / numKeyFields));
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            setIntKeyFields(fieldValues, numKeyFields, maxValue, rnd);
            // Set values.
            setIntPayloadFields(fieldValues, numKeyFields, fieldCount);
            TupleUtils.createIntegerTuple(ctx.getTupleBuilder(), ctx.getTuple(), fieldValues);
            if (LOGGER.isInfoEnabled()) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Inserting Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            try {
                ctx.getIndexAccessor().upsert(ctx.getTuple());
                ctx.insertCheckTuple(createIntCheckTuple(fieldValues, ctx.getKeyFieldCount()), ctx.getCheckTuples());
            } catch (HyracksDataException e) {
                // We set expected values only after insertion succeeds because
                // we ignore duplicate keys.
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    throw e;
                }
            }
        }
    }

    public void bulkLoadIntTuples(IIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        bulkLoadIntTuples(ctx, numTuples, false, rnd);
    }

    @SuppressWarnings("unchecked")
    public void bulkLoadIntTuples(IIndexTestContext ctx, int numTuples, boolean filter, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        int[] fieldValues = new int[ctx.getFieldCount()];
        int maxValue = (int) Math.ceil(Math.pow(numTuples, 1.0 / numKeyFields));
        Collection<CheckTuple> tmpCheckTuples = createCheckTuplesCollection();
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            setIntKeyFields(fieldValues, numKeyFields, maxValue, rnd);
            // Set values.
            setIntPayloadFields(fieldValues, numKeyFields, fieldCount);

            // Set expected values. (We also use these as the pre-sorted stream
            // for ordered indexes bulk loading).
            ctx.insertCheckTuple(createIntCheckTuple(fieldValues, ctx.getKeyFieldCount()), tmpCheckTuples);
        }
        bulkLoadCheckTuples(ctx, tmpCheckTuples, filter);

        // Add tmpCheckTuples to ctx check tuples for comparing searches.
        for (CheckTuple checkTuple : tmpCheckTuples) {
            ctx.insertCheckTuple(checkTuple, ctx.getCheckTuples());
        }
    }

    public static void bulkLoadCheckTuples(IIndexTestContext ctx, Collection<CheckTuple> checkTuples)
            throws HyracksDataException {
        bulkLoadCheckTuples(ctx, checkTuples, false);
    }

    public static void bulkLoadCheckTuples(IIndexTestContext ctx, Collection<CheckTuple> checkTuples, boolean filtered)
            throws HyracksDataException {
        int fieldCount = ctx.getFieldCount();
        int numTuples = checkTuples.size();
        ArrayTupleBuilder tupleBuilder =
                filtered ? new ArrayTupleBuilder(fieldCount + 1) : new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        // Perform bulk load.
        IIndexBulkLoader bulkLoader =
                ctx.getIndex().createBulkLoader(0.7f, false, numTuples, false, NoOpPageWriteCallback.INSTANCE);
        int c = 1;
        for (CheckTuple checkTuple : checkTuples) {
            if (LOGGER.isInfoEnabled()) {
                //if (c % (numTuples / 10) == 0) {
                LOGGER.info("Bulk Loading Tuple " + c + "/" + numTuples);
                //}
            }
            createTupleFromCheckTuple(checkTuple, tupleBuilder, tuple, ctx.getFieldSerdes(), filtered);
            bulkLoader.add(tuple);
            c++;
        }
        bulkLoader.end();
    }

    @SuppressWarnings("unchecked")
    public void deleteTuples(IIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        ArrayTupleBuilder deleteTupleBuilder = createDeleteTupleBuilder(ctx);
        ArrayTupleReference deleteTuple = new ArrayTupleReference();
        int numCheckTuples = ctx.getCheckTuples().size();
        // Copy CheckTuple references into array, so we can randomly pick from
        // there.
        CheckTuple[] checkTuples = new CheckTuple[numCheckTuples];
        int idx = 0;
        Iterator<CheckTuple> iter = ctx.getCheckTuples().iterator();
        while (iter.hasNext()) {
            CheckTuple checkTuple = iter.next();
            checkTuples[idx++] = checkTuple;
        }

        for (int i = 0; i < numTuples && numCheckTuples > 0; i++) {
            if (LOGGER.isInfoEnabled()) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Deleting Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            int checkTupleIdx = Math.abs(rnd.nextInt() % numCheckTuples);
            CheckTuple checkTuple = checkTuples[checkTupleIdx];
            createTupleFromCheckTuple(checkTuple, deleteTupleBuilder, deleteTuple, ctx.getFieldSerdes());
            ctx.getIndexAccessor().delete(deleteTuple);

            // Remove check tuple from expected results.
            ctx.deleteCheckTuple(checkTuple, ctx.getCheckTuples());

            // Swap with last "valid" CheckTuple.
            CheckTuple tmp = checkTuples[numCheckTuples - 1];
            checkTuples[numCheckTuples - 1] = checkTuple;
            checkTuples[checkTupleIdx] = tmp;
            numCheckTuples--;
        }
    }

    public static void checkCursorStats(ILSMIOOperation op) {
        if (op.getIOOpertionType() == LSMIOOperationType.MERGE) {
            MergeOperation mergeOp = (MergeOperation) op;
            IIndexCursorStats stats = mergeOp.getCursorStats();
            Assert.assertTrue(stats.getPageCounter().get() > 0);
            // Index cursor stats are only an (conservative) approximation of the number of pages of
            // merging components. Thus, there could be some left over pages.
            Assert.assertTrue(stats.getPageCounter().get() >= 0);
        }
    }

}
