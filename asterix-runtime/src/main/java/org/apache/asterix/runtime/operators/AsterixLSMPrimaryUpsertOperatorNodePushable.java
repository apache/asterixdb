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
package org.apache.asterix.runtime.operators;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.dataflow.AsterixLSMIndexUtil;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback.Operation;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMIndexInsertUpdateDeleteOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;

public class AsterixLSMPrimaryUpsertOperatorNodePushable extends LSMIndexInsertUpdateDeleteOperatorNodePushable {

    private PermutingFrameTupleReference key;
    private MultiComparator keySearchCmp;
    private ArrayTupleBuilder nullTupleBuilder;
    private INullWriter nullWriter;
    private ArrayTupleBuilder tb;
    private DataOutput dos;
    private LSMBTree lsmIndex;
    private RangePredicate searchPred;
    private IIndexCursor cursor;
    private ITupleReference prevTuple;
    private int numOfPrimaryKeys;
    boolean isFiltered = false;
    private ArrayTupleReference prevTupleWithFilter = new ArrayTupleReference();
    private ArrayTupleBuilder prevRecWithPKWithFilterValue;
    private ARecordType recordType;
    private int presetFieldIndex = -1;
    private ARecordPointable recPointable;
    private DataOutput prevDos;

    public AsterixLSMPrimaryUpsertOperatorNodePushable(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, int[] fieldPermutation, IRecordDescriptorProvider recordDescProvider, int numOfPrimaryKeys,
            ARecordType recordType, int filterFieldIndex) throws HyracksDataException {
        super(opDesc, ctx, partition, fieldPermutation, recordDescProvider, IndexOperation.UPSERT);
        // initialize nullWriter
        this.nullWriter = opDesc.getNullWriterFactory().createNullWriter();
        // The search key should only have the primary index and not use the permutations.
        this.key = new PermutingFrameTupleReference();
        int[] searchKeyPermutations = new int[numOfPrimaryKeys];
        for (int i = 0; i < searchKeyPermutations.length; i++) {
            searchKeyPermutations[i] = fieldPermutation[i];
        }
        key.setFieldPermutation(searchKeyPermutations);
        this.numOfPrimaryKeys = numOfPrimaryKeys;
        if (fieldPermutation.length > numOfPrimaryKeys + 1) {
            isFiltered = true;
            this.recordType = recordType;
            this.presetFieldIndex = filterFieldIndex;
            this.recPointable = (ARecordPointable) ARecordPointable.FACTORY.createPointable();
            this.prevRecWithPKWithFilterValue = new ArrayTupleBuilder(fieldPermutation.length);
            this.prevDos = prevRecWithPKWithFilterValue.getDataOutput();
        }
    }

    // we have the permutation which has [pk locations, record location, optional:filter-location]
    // the index -> we don't need anymore data?
    // we need to use the primary index opTracker and secondary indexes callbacks for insert/delete since the lock would
    // have been obtained through searchForUpsert operation

    @Override
    public void open() throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        accessor = new FrameTupleAccessor(inputRecDesc);
        writeBuffer = new VSizeFrame(ctx);
        writer.open();
        indexHelper.open();
        lsmIndex = (LSMBTree) indexHelper.getIndexInstance();

        try {
            nullTupleBuilder = new ArrayTupleBuilder(1);
            DataOutput out = nullTupleBuilder.getDataOutput();
            try {
                nullWriter.writeNull(out);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            nullTupleBuilder.addFieldEndOffset();
            searchPred = createSearchPredicate();
            tb = new ArrayTupleBuilder(recordDesc.getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender(new VSizeFrame(ctx), true);
            modCallback = opDesc.getModificationOpCallbackFactory().createModificationOperationCallback(
                    indexHelper.getResourcePath(), indexHelper.getResourceID(), lsmIndex, ctx);

            indexAccessor = lsmIndex.createAccessor(modCallback, opDesc.getSearchOpCallbackFactory()
                    .createSearchOperationCallback(indexHelper.getResourceID(), ctx));
            cursor = createCursor();
            frameTuple = new FrameTupleReference();
            IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                    .getApplicationContext().getApplicationObject();
            AsterixLSMIndexUtil.checkAndSetFirstLSN(lsmIndex, runtimeCtx.getTransactionSubsystem().getLogManager());
        } catch (Exception e) {
            indexHelper.close();
            throw new HyracksDataException(e);
        }
    }

    private void resetSearchPredicate(int tupleIndex) {
        key.reset(accessor, tupleIndex);
    }

    protected void writeOutput(int tupleIndex) throws Exception {
        tb.reset();
        frameTuple.reset(accessor, tupleIndex);
        for (int i = 0; i < frameTuple.getFieldCount(); i++) {
            dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
            tb.addFieldEndOffset();
        }
        if (prevTuple != null) {
            dos.write(prevTuple.getFieldData(numOfPrimaryKeys), prevTuple.getFieldStart(numOfPrimaryKeys),
                    prevTuple.getFieldLength(numOfPrimaryKeys));
            tb.addFieldEndOffset();
            // if with filters, append the filter
            if (isFiltered) {
                dos.write(prevTuple.getFieldData(numOfPrimaryKeys + 1), prevTuple.getFieldStart(numOfPrimaryKeys + 1),
                        prevTuple.getFieldLength(numOfPrimaryKeys + 1));
                tb.addFieldEndOffset();
            }
        } else {
            addNullField();
            // if with filters, append null
            if (isFiltered) {
                addNullField();
            }
        }
        FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
    }

    private void addNullField() throws IOException {
        dos.write(nullTupleBuilder.getByteArray());
        tb.addFieldEndOffset();
    }

    //TODO: use tryDelete/tryInsert in order to prevent deadlocks
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        LSMTreeIndexAccessor lsmAccessor = (LSMTreeIndexAccessor) indexAccessor;
        int tupleCount = accessor.getTupleCount();

        try {
            for (int i = 0; i < tupleCount; i++) {
                tuple.reset(accessor, i);
                resetSearchPredicate(i);
                cursor.reset();
                lsmAccessor.search(cursor, searchPred);
                if (cursor.hasNext()) {
                    cursor.next();
                    prevTuple = cursor.getTuple();
                    cursor.reset();
                    modCallback.setOp(Operation.DELETE);
                    if (isFiltered) {
                        prevTuple = getPrevTupleWithFilter(prevTuple);
                    }
                    if (i == 0) {
                        lsmAccessor.delete(prevTuple);
                    } else {
                        lsmAccessor.forceDelete(prevTuple);
                    }
                } else {
                    prevTuple = null;
                }
                modCallback.setOp(Operation.INSERT);
                if (prevTuple == null && i == 0) {
                    lsmAccessor.insert(tuple);
                } else {
                    lsmAccessor.forceInsert(tuple);
                }
                writeOutput(i);
            }
            if (tupleCount > 0) {
                // All tuples has to move forward to maintain the correctness of the transaction pipeline
                appender.write(writer, true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    private ITupleReference getPrevTupleWithFilter(ITupleReference prevTuple) throws IOException, AsterixException {
        prevRecWithPKWithFilterValue.reset();
        for (int i = 0; i < prevTuple.getFieldCount(); i++) {
            prevDos.write(prevTuple.getFieldData(i), prevTuple.getFieldStart(i), prevTuple.getFieldLength(i));
            prevRecWithPKWithFilterValue.addFieldEndOffset();
        }
        recPointable.set(prevTuple.getFieldData(numOfPrimaryKeys), prevTuple.getFieldStart(numOfPrimaryKeys),
                prevTuple.getFieldLength(numOfPrimaryKeys));
        // copy the field data from prevTuple
        prevDos.write(recPointable.getClosedFieldType(recordType, presetFieldIndex).getTypeTag().serialize());
        prevDos.write(recPointable.getByteArray(), recPointable.getClosedFieldOffset(recordType, presetFieldIndex),
                recPointable.getClosedFieldSize(recordType, presetFieldIndex));
        prevRecWithPKWithFilterValue.addFieldEndOffset();
        // prepare the tuple
        prevTupleWithFilter.reset(prevRecWithPKWithFilterValue.getFieldEndOffsets(),
                prevRecWithPKWithFilterValue.getByteArray());
        return prevTupleWithFilter;
    }

    private RangePredicate createSearchPredicate() {
        keySearchCmp = BTreeUtils.getSearchMultiComparator(lsmIndex.getComparatorFactories(), key);
        return new RangePredicate(key, key, true, true, keySearchCmp, keySearchCmp, null, null);
    }

    protected IIndexCursor createCursor() {
        return indexAccessor.createSearchCursor(false);
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            cursor.close();
            writer.close();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            indexHelper.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }
}
