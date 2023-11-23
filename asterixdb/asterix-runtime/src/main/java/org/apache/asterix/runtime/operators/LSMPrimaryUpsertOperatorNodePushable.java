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
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.dataflow.LSMIndexUtil;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.messaging.AtomicJobPreparedMessage;
import org.apache.asterix.common.transactions.ILogMarkerCallback;
import org.apache.asterix.common.transactions.PrimaryIndexLogMarkerCallback;
import org.apache.asterix.om.base.AInt8;
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
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.nc.NodeControllerService;
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
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameTupleProcessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMIndexInsertUpdateDeleteOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.FlushOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.projection.ITupleProjector;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.hyracks.util.trace.ITracer.Scope;
import org.apache.hyracks.util.trace.TraceUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

public class LSMPrimaryUpsertOperatorNodePushable extends LSMIndexInsertUpdateDeleteOperatorNodePushable {

    public static final AInt8 UPSERT_NEW = new AInt8((byte) 0);
    public static final AInt8 UPSERT_EXISTING = new AInt8((byte) 1);
    public static final AInt8 DELETE_EXISTING = new AInt8((byte) 2);
    private static final Logger LOGGER = LogManager.getLogger();
    private static final ThreadLocal<DateFormat> DATE_FORMAT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"));
    protected final PermutingFrameTupleReference key;
    private MultiComparator keySearchCmp;
    private ArrayTupleBuilder missingTupleBuilder;
    private final IMissingWriter missingWriter;
    protected ArrayTupleBuilder tb;
    private DataOutput dos;
    protected RangePredicate searchPred;
    protected final IIndexCursor[] cursors;
    protected ITupleReference prevTuple;
    protected final int numOfPrimaryKeys;
    protected boolean isFiltered = false;
    private final ArrayTupleReference prevTupleWithFilter = new ArrayTupleReference();
    private ArrayTupleBuilder prevRecWithPKWithFilterValue;
    private Integer filterSourceIndicator = null;
    private ARecordType filterItemType;
    private int presetFieldIndex = -1;
    private ARecordPointable recPointable;
    private DataOutput prevDos;
    private final boolean hasMeta;
    private final int filterFieldIndex;
    private final int metaFieldIndex;
    protected final ISearchOperationCallback[] searchCallbacks;
    protected final IFrameOperationCallback[] frameOpCallbacks;
    private final IFrameOperationCallbackFactory frameOpCallbackFactory;
    private final ISearchOperationCallbackFactory searchCallbackFactory;
    private final IFrameTupleProcessor[] processors;
    private final ITracer tracer;
    private final long traceCategory;
    private final ITupleProjector tupleProjector;
    private long lastRecordInTimeStamp = 0L;
    private final Int2ObjectMap<IntSet> partition2TuplesMap = new Int2ObjectOpenHashMap<>();
    private final boolean hasSecondaries;

    public LSMPrimaryUpsertOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            IIndexDataflowHelperFactory indexHelperFactory, int[] fieldPermutation, RecordDescriptor inputRecDesc,
            IModificationOperationCallbackFactory modCallbackFactory,
            ISearchOperationCallbackFactory searchCallbackFactory, int numOfPrimaryKeys, Integer filterSourceIndicator,
            ARecordType filterItemType, int filterFieldIndex, IFrameOperationCallbackFactory frameOpCallbackFactory,
            IMissingWriterFactory missingWriterFactory, boolean hasSecondaries, ITupleProjectorFactory projectorFactory,
            ITuplePartitionerFactory tuplePartitionerFactory, int[][] partitionsMap) throws HyracksDataException {
        super(ctx, partition, indexHelperFactory, fieldPermutation, inputRecDesc, IndexOperation.UPSERT,
                modCallbackFactory, null, tuplePartitionerFactory, partitionsMap);
        this.hasSecondaries = hasSecondaries;
        this.frameOpCallbacks = new IFrameOperationCallback[partitions.length];
        this.searchCallbacks = new ISearchOperationCallback[partitions.length];
        this.cursors = new IIndexCursor[partitions.length];
        this.processors = new IFrameTupleProcessor[partitions.length];
        this.key = new PermutingFrameTupleReference();
        this.searchCallbackFactory = searchCallbackFactory;
        this.numOfPrimaryKeys = numOfPrimaryKeys;
        this.frameOpCallbackFactory = frameOpCallbackFactory;
        missingWriter = missingWriterFactory.createMissingWriter();
        int[] searchKeyPermutations = new int[numOfPrimaryKeys];
        System.arraycopy(fieldPermutation, 0, searchKeyPermutations, 0, searchKeyPermutations.length);
        key.setFieldPermutation(searchKeyPermutations);
        hasMeta = (fieldPermutation.length > numOfPrimaryKeys + 1) && (filterFieldIndex < 0
                || (filterFieldIndex >= 0 && (fieldPermutation.length > numOfPrimaryKeys + 2)));
        this.metaFieldIndex = numOfPrimaryKeys + 1;
        this.filterFieldIndex = numOfPrimaryKeys + (hasMeta ? 2 : 1);
        if (filterFieldIndex >= 0) {
            isFiltered = true;
            this.filterItemType = filterItemType;
            this.presetFieldIndex = filterFieldIndex;
            this.filterSourceIndicator = filterSourceIndicator;
            this.recPointable = ARecordPointable.FACTORY.createPointable();
            this.prevRecWithPKWithFilterValue = new ArrayTupleBuilder(fieldPermutation.length + (hasMeta ? 1 : 0));
            this.prevDos = prevRecWithPKWithFilterValue.getDataOutput();
        }
        tracer = ctx.getJobletContext().getServiceContext().getTracer();
        traceCategory = tracer.getRegistry().get(TraceUtils.LATENCY);
        tupleProjector = projectorFactory.createTupleProjector(ctx);
    }

    protected void beforeModification(ITupleReference tuple) {
        // this is used for extensions to modify tuples before persistence
        // do nothing in the master branch
    }

    protected void createTupleProcessors(final boolean hasSecondaries) {
        for (int i = 0; i < partitions.length; i++) {
            ILSMIndexAccessor lsmAccessor = (ILSMIndexAccessor) indexAccessors[i];
            IIndexCursor cursor = cursors[i];
            ISearchOperationCallback searchCallback = searchCallbacks[i];
            IModificationOperationCallback modCallback = modCallbacks[i];
            IFrameOperationCallback frameOpCallback = frameOpCallbacks[i];
            processors[i] = new IFrameTupleProcessor() {
                @Override
                public void process(FrameTupleAccessor accessor, ITupleReference tuple, int index)
                        throws HyracksDataException {
                    try {
                        tb.reset();
                        IModificationOperationCallback abstractModCallback = modCallback;
                        boolean recordWasInserted = false;
                        boolean recordWasDeleted = false;
                        boolean isDelete = isDeleteOperation(tuple, numOfPrimaryKeys);
                        resetSearchPredicate(index);
                        if (isFiltered || isDelete || hasSecondaries) {
                            lsmAccessor.search(cursor, searchPred);
                            try {
                                if (cursor.hasNext()) {
                                    cursor.next();
                                    prevTuple = tupleProjector.project(cursor.getTuple(), dos, tb);
                                    appendOperationIndicator(!isDelete, true);
                                    appendFilterToPrevTuple();
                                    appendPrevRecord();
                                    appendPreviousMeta();
                                    appendFilterToOutput();
                                } else {
                                    appendOperationIndicator(!isDelete, false);
                                    appendPreviousTupleAsMissing();
                                }
                            } finally {
                                cursor.close(); // end the search
                            }
                        } else {
                            // simple upsert into a non-filtered dataset having no secondary indexes
                            searchCallback.before(key); // lock
                            appendOperationIndicator(true, false);
                            appendPreviousTupleAsMissing();
                        }
                        beforeModification(tuple);
                        if (isDelete && prevTuple != null) {
                            // Only delete if it is a delete and not upsert
                            // And previous tuple with the same key was found
                            if (abstractModCallback instanceof AbstractIndexModificationOperationCallback) {
                                ((AbstractIndexModificationOperationCallback) abstractModCallback)
                                        .setOp(Operation.DELETE);
                            }
                            lsmAccessor.forceDelete(tuple);
                            recordWasDeleted = true;
                        } else if (!isDelete) {
                            if (abstractModCallback instanceof AbstractIndexModificationOperationCallback) {
                                ((AbstractIndexModificationOperationCallback) abstractModCallback)
                                        .setOp(Operation.UPSERT);
                            }
                            lsmAccessor.forceUpsert(tuple);
                            recordWasInserted = true;
                        }
                        if (isFiltered && prevTuple != null) {
                            // need to update the filter of the new component with the previous value
                            lsmAccessor.updateFilter(prevTuple);
                        }
                        writeOutput(index, recordWasInserted, recordWasDeleted, searchCallback);
                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    }
                }

                @Override
                public void start() throws HyracksDataException {
                    ((LSMTreeIndexAccessor) lsmAccessor).getCtx().setOperation(IndexOperation.UPSERT);
                }

                @Override
                public void finish() throws HyracksDataException {
                    ((LSMTreeIndexAccessor) lsmAccessor).getCtx().setOperation(IndexOperation.UPSERT);
                }

                @Override
                public void fail(Throwable th) {
                    // We must fail before we exit the components
                    frameOpCallback.fail(th);
                }
            };
        }
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
        writerOpen = true;
        try {
            missingTupleBuilder = new ArrayTupleBuilder(1);
            DataOutput out = missingTupleBuilder.getDataOutput();
            try {
                missingWriter.writeMissing(out);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            missingTupleBuilder.addFieldEndOffset();
            tb = new ArrayTupleBuilder(recordDesc.getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender(new VSizeFrame(ctx), true);
            INcApplicationContext appCtx =
                    (INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();
            for (int i = 0; i < indexHelpers.length; i++) {
                IIndexDataflowHelper indexHelper = indexHelpers[i];
                indexHelpersOpen[i] = true;
                indexHelper.open();
                indexes[i] = indexHelper.getIndexInstance();
                if (((ILSMIndex) indexes[i]).isAtomic()) {
                    ((PrimaryIndexOperationTracker) ((ILSMIndex) indexes[i]).getOperationTracker()).clear();
                }
                if (ctx.getSharedObject() != null && i == 0) {
                    PrimaryIndexLogMarkerCallback callback =
                            new PrimaryIndexLogMarkerCallback((AbstractLSMIndex) indexes[0]);
                    TaskUtil.put(ILogMarkerCallback.KEY_MARKER_CALLBACK, callback, ctx);
                }
                modCallbacks[i] =
                        modOpCallbackFactory.createModificationOperationCallback(indexHelper.getResource(), ctx, this);
                searchCallbacks[i] = searchCallbackFactory
                        .createSearchOperationCallback(indexHelper.getResource().getId(), ctx, this);
                IIndexAccessParameters iap = new IndexAccessParameters(modCallbacks[i], searchCallbacks[i]);
                iap.getParameters().put(HyracksConstants.TUPLE_PROJECTOR, tupleProjector);
                indexAccessors[i] = indexes[i].createAccessor(iap);
                setAtomicOpContextIfAtomic(indexes[i], indexAccessors[i]);
                cursors[i] = ((LSMTreeIndexAccessor) indexAccessors[i]).createSearchCursor(false);
                LSMIndexUtil.checkAndSetFirstLSN((AbstractLSMIndex) indexes[i],
                        appCtx.getTransactionSubsystem().getLogManager());
            }
            searchPred = createSearchPredicate(indexes[0]);
            frameTuple = new FrameTupleReference();
            createFrameOpCallbacks();
            createTupleProcessors(hasSecondaries);
        } catch (Throwable e) { // NOSONAR: Re-thrown
            throw HyracksDataException.create(e);
        }
    }

    private void createFrameOpCallbacks() throws HyracksDataException {
        for (int i = 0; i < partitions.length; i++) {
            LSMTreeIndexAccessor lsmAccessor = (LSMTreeIndexAccessor) indexAccessors[i];
            frameOpCallbacks[i] = new IFrameOperationCallback() {
                final IFrameOperationCallback callback =
                        frameOpCallbackFactory.createFrameOperationCallback(ctx, lsmAccessor);

                @Override
                public void frameCompleted() throws HyracksDataException {
                    if (appender.getTupleCount() > 0) {
                        //TODO: mixed-frame vs frame-per-storage-partition
                        appender.write(writer, true);
                    }
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

                @Override
                public void open() throws HyracksDataException {
                    callback.open();
                }
            };
            frameOpCallbacks[i].open();
        }
    }

    protected void resetSearchPredicate(int tupleIndex) {
        key.reset(accessor, tupleIndex);
        searchPred.reset(key, key, true, true, keySearchCmp, keySearchCmp);
    }

    protected void writeOutput(int tupleIndex, boolean recordWasInserted, boolean recordWasDeleted,
            ISearchOperationCallback searchCallback) throws IOException {
        if (recordWasInserted || recordWasDeleted) {
            frameTuple.reset(accessor, tupleIndex);
            for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                tb.addFieldEndOffset();
            }
            FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        } else {
            try {
                if (searchCallback instanceof LockThenSearchOperationCallback) {
                    ((LockThenSearchOperationCallback) searchCallback).release();
                }
            } catch (ACIDException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    protected static boolean isDeleteOperation(ITupleReference t1, int field) {
        return TypeTagUtil.isType(t1, field, ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
    }

    private void writeMissingField() throws IOException {
        dos.write(missingTupleBuilder.getByteArray());
        tb.addFieldEndOffset();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        partition2TuplesMap.clear();
        int itemCount = accessor.getTupleCount();
        for (int i = 0; i < itemCount; i++) {
            int storagePartition = tuplePartitioner.partition(accessor, i);
            int pIdx = storagePartitionId2Index.get(storagePartition);
            IntSet tupleIndexes = partition2TuplesMap.computeIfAbsent(pIdx, k -> new IntOpenHashSet());
            tupleIndexes.add(i);
        }
        // to ensure all partitions will be processed at least once, add partitions with missing tuples
        for (int partition : storagePartitionId2Index.values()) {
            partition2TuplesMap.computeIfAbsent(partition, k -> new IntOpenHashSet());
        }
        for (Int2ObjectMap.Entry<IntSet> p2tuplesMapEntry : partition2TuplesMap.int2ObjectEntrySet()) {
            int pIdx = p2tuplesMapEntry.getIntKey();
            LSMTreeIndexAccessor lsmAccessor = (LSMTreeIndexAccessor) indexAccessors[pIdx];
            IFrameOperationCallback frameOpCallback = frameOpCallbacks[pIdx];
            IFrameTupleProcessor processor = processors[pIdx];
            lsmAccessor.batchOperate(accessor, tuple, processor, frameOpCallback, p2tuplesMapEntry.getValue());
        }
        if (itemCount > 0) {
            lastRecordInTimeStamp = System.currentTimeMillis();
        }
    }

    protected void appendFilterToOutput() throws IOException {
        // if with filters, append the filter
        if (isFiltered) {
            dos.write(prevTuple.getFieldData(filterFieldIndex), prevTuple.getFieldStart(filterFieldIndex),
                    prevTuple.getFieldLength(filterFieldIndex));
            tb.addFieldEndOffset();
        }
    }

    @SuppressWarnings("unchecked") // using serializer
    protected void appendOperationIndicator(boolean isUpsert, boolean prevTupleExists) throws IOException {
        if (isUpsert) {
            if (prevTupleExists) {
                recordDesc.getFields()[0].serialize(UPSERT_EXISTING, dos);
            } else {
                recordDesc.getFields()[0].serialize(UPSERT_NEW, dos);
            }
        } else {
            recordDesc.getFields()[0].serialize(DELETE_EXISTING, dos);
        }
        tb.addFieldEndOffset();
    }

    protected void appendPrevRecord() throws IOException {
        dos.write(prevTuple.getFieldData(numOfPrimaryKeys), prevTuple.getFieldStart(numOfPrimaryKeys),
                prevTuple.getFieldLength(numOfPrimaryKeys));
        tb.addFieldEndOffset();
    }

    protected void appendPreviousMeta() throws IOException {
        // if has meta, then append meta
        if (hasMeta) {
            dos.write(prevTuple.getFieldData(metaFieldIndex), prevTuple.getFieldStart(metaFieldIndex),
                    prevTuple.getFieldLength(metaFieldIndex));
            tb.addFieldEndOffset();
        }
    }

    protected void appendPreviousTupleAsMissing() throws IOException {
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
        if (appender.getTupleCount() > 0) {
            appender.write(writer, true);
        }
    }

    protected void appendFilterToPrevTuple() throws IOException {
        if (isFiltered) {
            prevRecWithPKWithFilterValue.reset();
            for (int i = 0; i < prevTuple.getFieldCount(); i++) {
                prevDos.write(prevTuple.getFieldData(i), prevTuple.getFieldStart(i), prevTuple.getFieldLength(i));
                prevRecWithPKWithFilterValue.addFieldEndOffset();
            }

            if (filterSourceIndicator == 0) {
                recPointable.set(prevTuple.getFieldData(numOfPrimaryKeys), prevTuple.getFieldStart(numOfPrimaryKeys),
                        prevTuple.getFieldLength(numOfPrimaryKeys));
            } else {
                recPointable.set(prevTuple.getFieldData(metaFieldIndex), prevTuple.getFieldStart(metaFieldIndex),
                        prevTuple.getFieldLength(metaFieldIndex));
            }
            // copy the field data from prevTuple
            byte tag = recPointable.getClosedFieldType(filterItemType, presetFieldIndex).getTypeTag().serialize();
            prevDos.write(tag);
            prevDos.write(recPointable.getByteArray(),
                    recPointable.getClosedFieldOffset(filterItemType, presetFieldIndex),
                    recPointable.getClosedFieldSize(filterItemType, presetFieldIndex));
            prevRecWithPKWithFilterValue.addFieldEndOffset();
            // prepare the tuple
            prevTupleWithFilter.reset(prevRecWithPKWithFilterValue.getFieldEndOffsets(),
                    prevRecWithPKWithFilterValue.getByteArray());
            prevTuple = prevTupleWithFilter;
        }
    }

    private RangePredicate createSearchPredicate(IIndex index) {
        keySearchCmp = BTreeUtils.getSearchMultiComparator(((ITreeIndex) index).getComparatorFactories(), key);
        return new RangePredicate(key, key, true, true, keySearchCmp, keySearchCmp, null, null);
    }

    @Override
    public void close() throws HyracksDataException {
        traceLastRecordIn();
        Throwable failure = CleanupUtils.close(frameOpCallbacks, null);
        failure = CleanupUtils.destroy(failure, cursors);
        failure = CleanupUtils.close(writer, failure);
        failure = closeIndexHelpers(failure);
        if (failure == null && !failed) {
            commitAtomicUpsert();
        } else {
            abortAtomicUpsert();
        }

        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }

    @SuppressWarnings({ "squid:S1181", "squid:S1166" })
    private void traceLastRecordIn() {
        try {
            if (tracer.isEnabled(traceCategory) && lastRecordInTimeStamp > 0 && indexHelpers[0] != null
                    && indexHelpers[0].getIndexInstance() != null) {
                tracer.instant("UpsertClose", traceCategory, Scope.t,
                        () -> "{\"last-record-in\":\"" + DATE_FORMAT.get().format(new Date(lastRecordInTimeStamp))
                                + "\", \"index\":" + indexHelpers[0].getIndexInstance().toString() + "}");
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
        failed = true;
        writer.fail();
    }

    @Override
    public void flush() throws HyracksDataException {
        // No op since nextFrame flushes by default
    }

    // TODO: Refactor and remove duplicated code
    private void commitAtomicUpsert() throws HyracksDataException {
        final Map<String, ILSMComponentId> componentIdMap = new HashMap<>();
        boolean atomic = false;
        for (IIndex index : indexes) {
            if (index != null && ((ILSMIndex) index).isAtomic()) {
                PrimaryIndexOperationTracker opTracker =
                        ((PrimaryIndexOperationTracker) ((ILSMIndex) index).getOperationTracker());
                opTracker.finishAllFlush();
                for (Map.Entry<String, FlushOperation> entry : opTracker.getLastFlushOperation().entrySet()) {
                    componentIdMap.put(entry.getKey(), entry.getValue().getFlushingComponent().getId());
                }
                atomic = true;
            }
        }

        if (atomic) {
            AtomicJobPreparedMessage message = new AtomicJobPreparedMessage(ctx.getJobletContext().getJobId(),
                    ctx.getJobletContext().getServiceContext().getNodeId(), componentIdMap);
            try {
                ((NodeControllerService) ctx.getJobletContext().getServiceContext().getControllerService())
                        .sendRealTimeApplicationMessageToCC(ctx.getJobletContext().getJobId().getCcId(),
                                JavaSerializationUtils.serialize(message), null);
            } catch (Exception e) {
                throw new ACIDException(e);
            }
        }
    }

    private void abortAtomicUpsert() throws HyracksDataException {
        for (IIndex index : indexes) {
            if (index != null && ((ILSMIndex) index).isAtomic()) {
                PrimaryIndexOperationTracker opTracker =
                        ((PrimaryIndexOperationTracker) ((ILSMIndex) index).getOperationTracker());
                opTracker.abort();
            }
        }
    }
}
