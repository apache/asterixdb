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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.dataflow.LSMIndexUtil;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ILogMarkerCallback;
import org.apache.asterix.common.transactions.PrimaryIndexLogMarkerCallback;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback.Operation;
import org.apache.asterix.transaction.management.opcallbacks.LockThenSearchOperationCallback;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameTupleProcessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMIndexInsertUpdateDeleteOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.hyracks.util.trace.ITracer.Scope;
import org.apache.hyracks.util.trace.TraceUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LSMPrimaryUpsertOperatorNodePushable extends LSMIndexInsertUpdateDeleteOperatorNodePushable {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final ThreadLocal<DateFormat> DATE_FORMAT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"));
    private final PermutingFrameTupleReference key;
    private MultiComparator keySearchCmp;
    private ArrayTupleBuilder missingTupleBuilder;
    private final IMissingWriter missingWriter;
    private ArrayTupleBuilder tb;
    private DataOutput dos;
    private RangePredicate searchPred;
    private IIndexCursor cursor;
    private ITupleReference prevTuple;
    private final int numOfPrimaryKeys;
    boolean isFiltered = false;
    private final ArrayTupleReference prevTupleWithFilter = new ArrayTupleReference();
    private ArrayTupleBuilder prevRecWithPKWithFilterValue;
    private ARecordType recordType;
    private int presetFieldIndex = -1;
    private ARecordPointable recPointable;
    private DataOutput prevDos;
    private final boolean hasMeta;
    private final int filterFieldIndex;
    private final int metaFieldIndex;
    private LockThenSearchOperationCallback searchCallback;
    private IFrameOperationCallback frameOpCallback;
    private final IFrameOperationCallbackFactory frameOpCallbackFactory;
    private AbstractIndexModificationOperationCallback abstractModCallback;
    private final ISearchOperationCallbackFactory searchCallbackFactory;
    private final IFrameTupleProcessor processor;
    private LSMTreeIndexAccessor lsmAccessor;
    private final ITracer tracer;
    private final long traceCategory;
    private long lastRecordInTimeStamp = 0L;

    public LSMPrimaryUpsertOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            IIndexDataflowHelperFactory indexHelperFactory, int[] fieldPermutation, RecordDescriptor inputRecDesc,
            IModificationOperationCallbackFactory modCallbackFactory,
            ISearchOperationCallbackFactory searchCallbackFactory, int numOfPrimaryKeys, ARecordType recordType,
            int filterFieldIndex, IFrameOperationCallbackFactory frameOpCallbackFactory,
            IMissingWriterFactory missingWriterFactory, final boolean hasSecondaries) throws HyracksDataException {
        super(ctx, partition, indexHelperFactory, fieldPermutation, inputRecDesc, IndexOperation.UPSERT,
                modCallbackFactory, null);
        this.key = new PermutingFrameTupleReference();
        this.searchCallbackFactory = searchCallbackFactory;
        this.numOfPrimaryKeys = numOfPrimaryKeys;
        this.frameOpCallbackFactory = frameOpCallbackFactory;
        missingWriter = missingWriterFactory.createMissingWriter();
        int[] searchKeyPermutations = new int[numOfPrimaryKeys];
        for (int i = 0; i < searchKeyPermutations.length; i++) {
            searchKeyPermutations[i] = fieldPermutation[i];
        }
        key.setFieldPermutation(searchKeyPermutations);
        hasMeta = (fieldPermutation.length > numOfPrimaryKeys + 1) && (filterFieldIndex < 0
                || (filterFieldIndex >= 0 && (fieldPermutation.length > numOfPrimaryKeys + 2)));
        this.metaFieldIndex = numOfPrimaryKeys + 1;
        this.filterFieldIndex = numOfPrimaryKeys + (hasMeta ? 2 : 1);
        if (filterFieldIndex >= 0) {
            isFiltered = true;
            this.recordType = recordType;
            this.presetFieldIndex = filterFieldIndex;
            this.recPointable = ARecordPointable.FACTORY.createPointable();
            this.prevRecWithPKWithFilterValue = new ArrayTupleBuilder(fieldPermutation.length + (hasMeta ? 1 : 0));
            this.prevDos = prevRecWithPKWithFilterValue.getDataOutput();
        }
        processor = new IFrameTupleProcessor() {
            @Override
            public void process(ITupleReference tuple, int index) throws HyracksDataException {
                try {
                    tb.reset();
                    boolean recordWasInserted = false;
                    boolean recordWasDeleted = false;
                    boolean isDelete = isDeleteOperation(tuple, numOfPrimaryKeys);
                    resetSearchPredicate(index);
                    if (isFiltered || isDelete || hasSecondaries) {
                        lsmAccessor.search(cursor, searchPred);
                        try {
                            if (cursor.hasNext()) {
                                cursor.next();
                                prevTuple = cursor.getTuple();
                                appendUpsertIndicator(!isDelete);
                                appendFilterToPrevTuple();
                                appendPrevRecord();
                                appendPreviousMeta();
                                appendFilterToOutput();
                            } else {
                                appendUpsertIndicator(!isDelete);
                                appendPreviousTupleAsMissing();
                            }
                        } finally {
                            cursor.close(); // end the search
                        }
                    } else {
                        searchCallback.before(key); // lock
                        appendUpsertIndicator(!isDelete);
                        appendPreviousTupleAsMissing();
                    }
                    if (isDelete && prevTuple != null) {
                        // Only delete if it is a delete and not upsert
                        // And previous tuple with the same key was found
                        abstractModCallback.setOp(Operation.DELETE);
                        lsmAccessor.forceDelete(tuple);
                        recordWasDeleted = true;
                    } else if (!isDelete) {
                        abstractModCallback.setOp(Operation.UPSERT);
                        lsmAccessor.forceUpsert(tuple);
                        recordWasInserted = true;
                    }
                    if (isFiltered && prevTuple != null) {
                        // need to update the filter of the new component with the previous value
                        lsmAccessor.updateFilter(prevTuple);
                    }
                    writeOutput(index, recordWasInserted, recordWasDeleted);
                } catch (Exception e) {
                    throw HyracksDataException.create(e);
                }
            }

            @Override
            public void start() throws HyracksDataException {
                lsmAccessor.getCtx().setOperation(IndexOperation.UPSERT);
            }

            @Override
            public void finish() throws HyracksDataException {
                lsmAccessor.getCtx().setOperation(IndexOperation.UPSERT);
            }

            @Override
            public void fail(Throwable th) {
                // We must fail before we exit the components
                frameOpCallback.fail(th);
            }
        };
        tracer = ctx.getJobletContext().getServiceContext().getTracer();
        traceCategory = tracer.getRegistry().get(TraceUtils.LATENCY);
    }

    // we have the permutation which has [pk locations, record location, optional:filter-location]
    // the index -> we don't need anymore data?
    // we need to use the primary index opTracker and secondary indexes callbacks for insert/delete since the lock would
    // have been obtained through searchForUpsert operation

    @Override
    public void open() throws HyracksDataException {
        accessor = new FrameTupleAccessor(inputRecDesc);
        writeBuffer = new VSizeFrame(ctx);
        writer.open();
        indexHelper.open();
        index = indexHelper.getIndexInstance();
        try {
            if (ctx.getSharedObject() != null) {
                PrimaryIndexLogMarkerCallback callback = new PrimaryIndexLogMarkerCallback((AbstractLSMIndex) index);
                TaskUtil.put(ILogMarkerCallback.KEY_MARKER_CALLBACK, callback, ctx);
            }
            missingTupleBuilder = new ArrayTupleBuilder(1);
            DataOutput out = missingTupleBuilder.getDataOutput();
            try {
                missingWriter.writeMissing(out);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            missingTupleBuilder.addFieldEndOffset();
            searchPred = createSearchPredicate();
            tb = new ArrayTupleBuilder(recordDesc.getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender(new VSizeFrame(ctx), true);
            modCallback =
                    modOpCallbackFactory.createModificationOperationCallback(indexHelper.getResource(), ctx, this);
            abstractModCallback = (AbstractIndexModificationOperationCallback) modCallback;
            searchCallback = (LockThenSearchOperationCallback) searchCallbackFactory
                    .createSearchOperationCallback(indexHelper.getResource().getId(), ctx, this);
            IIndexAccessParameters iap = new IndexAccessParameters(abstractModCallback, searchCallback);
            indexAccessor = index.createAccessor(iap);
            lsmAccessor = (LSMTreeIndexAccessor) indexAccessor;
            cursor = indexAccessor.createSearchCursor(false);
            frameTuple = new FrameTupleReference();
            INcApplicationContext appCtx =
                    (INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();
            LSMIndexUtil.checkAndSetFirstLSN((AbstractLSMIndex) index,
                    appCtx.getTransactionSubsystem().getLogManager());
            frameOpCallback = new IFrameOperationCallback() {
                IFrameOperationCallback callback =
                        frameOpCallbackFactory.createFrameOperationCallback(ctx, (ILSMIndexAccessor) indexAccessor);

                @Override
                public void frameCompleted() throws HyracksDataException {
                    appender.write(writer, true);
                    callback.frameCompleted();
                }

                @Override
                public void close() throws IOException {
                    callback.close();
                }

                @Override
                public void fail(Throwable th) {
                    callback.fail(th);
                }
            };
        } catch (Throwable e) { // NOSONAR: Re-thrown
            throw HyracksDataException.create(e);
        }
    }

    private void resetSearchPredicate(int tupleIndex) {
        key.reset(accessor, tupleIndex);
        searchPred.reset(key, key, true, true, keySearchCmp, keySearchCmp);
    }

    private void writeOutput(int tupleIndex, boolean recordWasInserted, boolean recordWasDeleted) throws IOException {
        if (recordWasInserted || recordWasDeleted) {
            frameTuple.reset(accessor, tupleIndex);
            for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                tb.addFieldEndOffset();
            }
            FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        } else {
            try {
                searchCallback.release();
            } catch (ACIDException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    private static boolean isDeleteOperation(ITupleReference t1, int field) {
        return TypeTagUtil.isType(t1, field, ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
    }

    private void writeMissingField() throws IOException {
        dos.write(missingTupleBuilder.getByteArray());
        tb.addFieldEndOffset();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int itemCount = accessor.getTupleCount();
        lsmAccessor.batchOperate(accessor, tuple, processor, frameOpCallback);
        if (itemCount > 0) {
            lastRecordInTimeStamp = System.currentTimeMillis();
        }
    }

    private void appendFilterToOutput() throws IOException {
        // if with filters, append the filter
        if (isFiltered) {
            dos.write(prevTuple.getFieldData(filterFieldIndex), prevTuple.getFieldStart(filterFieldIndex),
                    prevTuple.getFieldLength(filterFieldIndex));
            tb.addFieldEndOffset();
        }
    }

    private void appendUpsertIndicator(boolean isUpsert) throws IOException {
        recordDesc.getFields()[0].serialize(isUpsert ? ABoolean.TRUE : ABoolean.FALSE, dos);
        tb.addFieldEndOffset();
    }

    private void appendPrevRecord() throws IOException {
        dos.write(prevTuple.getFieldData(numOfPrimaryKeys), prevTuple.getFieldStart(numOfPrimaryKeys),
                prevTuple.getFieldLength(numOfPrimaryKeys));
        tb.addFieldEndOffset();
    }

    private void appendPreviousMeta() throws IOException {
        // if has meta, then append meta
        if (hasMeta) {
            dos.write(prevTuple.getFieldData(metaFieldIndex), prevTuple.getFieldStart(metaFieldIndex),
                    prevTuple.getFieldLength(metaFieldIndex));
            tb.addFieldEndOffset();
        }
    }

    private void appendPreviousTupleAsMissing() throws IOException {
        prevTuple = null;
        writeMissingField();
        if (hasMeta) {
            writeMissingField();
        }
        // if with filters, append null
        if (isFiltered) {
            writeMissingField();
        }
    }

    /**
     * Flushes tuples (which have already been written to tuple appender's buffer in writeOutput() method)
     * to the next operator/consumer.
     */
    @Override
    public void flushPartialFrame() throws HyracksDataException {
        appender.write(writer, true);
    }

    private void appendFilterToPrevTuple() throws IOException {
        if (isFiltered) {
            prevRecWithPKWithFilterValue.reset();
            for (int i = 0; i < prevTuple.getFieldCount(); i++) {
                prevDos.write(prevTuple.getFieldData(i), prevTuple.getFieldStart(i), prevTuple.getFieldLength(i));
                prevRecWithPKWithFilterValue.addFieldEndOffset();
            }
            recPointable.set(prevTuple.getFieldData(numOfPrimaryKeys), prevTuple.getFieldStart(numOfPrimaryKeys),
                    prevTuple.getFieldLength(numOfPrimaryKeys));
            // copy the field data from prevTuple
            byte tag = recPointable.getClosedFieldType(recordType, presetFieldIndex).getTypeTag().serialize();
            prevDos.write(tag);
            prevDos.write(recPointable.getByteArray(), recPointable.getClosedFieldOffset(recordType, presetFieldIndex),
                    recPointable.getClosedFieldSize(recordType, presetFieldIndex));
            prevRecWithPKWithFilterValue.addFieldEndOffset();
            // prepare the tuple
            prevTupleWithFilter.reset(prevRecWithPKWithFilterValue.getFieldEndOffsets(),
                    prevRecWithPKWithFilterValue.getByteArray());
            prevTuple = prevTupleWithFilter;
        }
    }

    private RangePredicate createSearchPredicate() {
        keySearchCmp = BTreeUtils.getSearchMultiComparator(((ITreeIndex) index).getComparatorFactories(), key);
        return new RangePredicate(key, key, true, true, keySearchCmp, keySearchCmp, null, null);
    }

    @Override
    public void close() throws HyracksDataException {
        traceLastRecordIn();
        Throwable failure = CleanupUtils.close(frameOpCallback, null);
        failure = CleanupUtils.destroy(failure, cursor);
        failure = CleanupUtils.close(writer, failure);
        failure = CleanupUtils.close(indexHelper, failure);
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }

    @SuppressWarnings({ "squid:S1181", "squid:S1166" })
    private void traceLastRecordIn() {
        try {
            if (tracer.isEnabled(traceCategory) && lastRecordInTimeStamp > 0 && indexHelper != null
                    && indexHelper.getIndexInstance() != null) {
                tracer.instant("UpsertClose", traceCategory, Scope.t,
                        "{\"last-record-in\":\"" + DATE_FORMAT.get().format(new Date(lastRecordInTimeStamp))
                                + "\", \"index\":" + indexHelper.getIndexInstance().toString() + "}");
            }
        } catch (Throwable traceFailure) {
            try {
                LOGGER.warn("Tracing last record in failed", traceFailure);
            } catch (Throwable ignore) {
                // Ignore logging failure
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void flush() throws HyracksDataException {
        // No op since nextFrame flushes by default
    }
}
