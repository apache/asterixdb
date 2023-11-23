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
package org.apache.hyracks.storage.am.common.dataflow;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IIntrospectingOperator;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitioner;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.NoOpOperatorStats;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ILSMIndexCursor;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.tuples.ReferenceFrameTupleReference;
import org.apache.hyracks.storage.am.common.util.ResourceReleaseUtils;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.projection.ITupleProjector;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;
import org.apache.hyracks.util.IThreadStatsCollector;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public abstract class IndexSearchOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable
        implements IIntrospectingOperator {

    static final Logger LOGGER = LogManager.getLogger();
    protected final IHyracksTaskContext ctx;
    protected FrameTupleAccessor accessor;
    protected FrameTupleAppender appender;
    protected ArrayTupleBuilder tb;
    protected DataOutput dos;

    protected ISearchPredicate searchPred;
    protected final IIndexDataflowHelper[] indexHelpers;
    protected final boolean[] indexHelpersOpen;
    protected IIndex[] indexes;
    protected IIndexAccessor[] indexAccessors;
    protected IIndexCursor[] cursors;

    protected final RecordDescriptor inputRecDesc;
    protected final boolean retainInput;
    protected FrameTupleReference frameTuple;

    protected final boolean retainMissing;
    protected ArrayTupleBuilder nonMatchTupleBuild;
    protected IMissingWriter nonMatchWriter;

    protected final int[] minFilterFieldIndexes;
    protected final int[] maxFilterFieldIndexes;
    protected PermutingFrameTupleReference minFilterKey;
    protected PermutingFrameTupleReference maxFilterKey;
    protected final boolean appendIndexFilter;
    protected ArrayTupleBuilder nonFilterTupleBuild;
    protected IMissingWriter nonFilterWriter;
    protected final ISearchOperationCallbackFactory searchCallbackFactory;
    protected boolean failed = false;
    protected IOperatorStats stats;

    // Used when the result of the search operation callback needs to be passed.
    protected boolean appendSearchCallbackProceedResult;
    protected byte[] searchCallbackProceedResultFalseValue;
    protected byte[] searchCallbackProceedResultTrueValue;

    protected final ITupleFilterFactory tupleFilterFactory;
    protected ReferenceFrameTupleReference referenceFilterTuple;
    // filter out tuples based on the query-provided select condition
    // only results satisfying the filter condition would be returned to downstream operators
    protected ITupleFilter tupleFilter;
    protected final long outputLimit;
    protected long outputCount = 0;
    protected boolean finished;
    protected final ITupleProjector tupleProjector;
    protected final ITuplePartitioner tuplePartitioner;
    protected final int[] partitions;
    private final Int2IntMap storagePartitionId2Index = new Int2IntOpenHashMap();

    public IndexSearchOperatorNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecDesc, int partition,
            int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes, IIndexDataflowHelperFactory indexHelperFactory,
            boolean retainInput, boolean retainMissing, IMissingWriterFactory nonMatchWriterFactory,
            ISearchOperationCallbackFactory searchCallbackFactory, boolean appendIndexFilter,
            IMissingWriterFactory nonFilterWriterFactory, ITupleFilterFactory tupleFilterFactory, long outputLimit,
            boolean appendSearchCallbackProceedResult, byte[] searchCallbackProceedResultFalseValue,
            byte[] searchCallbackProceedResultTrueValue, ITupleProjectorFactory projectorFactory,
            ITuplePartitionerFactory tuplePartitionerFactory, int[][] partitionsMap) throws HyracksDataException {
        this.ctx = ctx;
        this.appender = new FrameTupleAppender(new VSizeFrame(ctx), true);
        //TODO(partitioning) partitionsMap should not be null
        this.partitions = partitionsMap != null ? partitionsMap[partition] : new int[] { partition };
        for (int i = 0; i < partitions.length; i++) {
            storagePartitionId2Index.put(partitions[i], i);
        }
        this.indexHelpers = new IIndexDataflowHelper[partitions.length];
        this.indexHelpersOpen = new boolean[partitions.length];
        this.indexes = new IIndex[partitions.length];
        this.indexAccessors = new IIndexAccessor[partitions.length];
        this.cursors = new IIndexCursor[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            indexHelpers[i] = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partitions[i]);
        }
        this.retainInput = retainInput;
        this.retainMissing = retainMissing;
        this.appendIndexFilter = appendIndexFilter;
        if (this.retainMissing) {
            this.nonMatchWriter = nonMatchWriterFactory.createMissingWriter();
        }
        if (this.appendIndexFilter) {
            this.nonFilterWriter = nonFilterWriterFactory.createMissingWriter();
        }
        this.inputRecDesc = inputRecDesc;
        this.searchCallbackFactory = searchCallbackFactory;
        this.minFilterFieldIndexes = minFilterFieldIndexes;
        this.maxFilterFieldIndexes = maxFilterFieldIndexes;
        if (minFilterFieldIndexes != null && minFilterFieldIndexes.length > 0) {
            minFilterKey = new PermutingFrameTupleReference();
            minFilterKey.setFieldPermutation(minFilterFieldIndexes);
        }
        if (maxFilterFieldIndexes != null && maxFilterFieldIndexes.length > 0) {
            maxFilterKey = new PermutingFrameTupleReference();
            maxFilterKey.setFieldPermutation(maxFilterFieldIndexes);
        }
        this.appendSearchCallbackProceedResult = appendSearchCallbackProceedResult;
        this.searchCallbackProceedResultFalseValue = searchCallbackProceedResultFalseValue;
        this.searchCallbackProceedResultTrueValue = searchCallbackProceedResultTrueValue;
        this.tupleFilterFactory = tupleFilterFactory;
        this.outputLimit = outputLimit;
        this.stats = new NoOpOperatorStats();

        if (this.tupleFilterFactory != null && this.retainMissing) {
            throw new IllegalStateException("RetainMissing with tuple filter is not supported");
        }

        tupleProjector = projectorFactory.createTupleProjector(ctx);
        tuplePartitioner = tuplePartitionerFactory == null ? null : tuplePartitionerFactory.createPartitioner(ctx);
    }

    protected abstract ISearchPredicate createSearchPredicate(IIndex index);

    protected abstract void resetSearchPredicate(int tupleIndex);

    // Assigns any index-type specific related accessor parameters
    protected abstract void addAdditionalIndexAccessorParams(IIndexAccessParameters iap) throws HyracksDataException;

    protected IIndexCursor createCursor(IIndex idx, IIndexAccessor idxAccessor) throws HyracksDataException {
        return idxAccessor.createSearchCursor(false);
    }

    protected abstract int getFieldCount(IIndex index);

    @Override
    public void open() throws HyracksDataException {
        writer.open();
        ISearchOperationCallback[] searchCallbacks = new ISearchOperationCallback[partitions.length];
        IIndexAccessParameters[] iaps = new IndexAccessParameters[partitions.length];

        for (int i = 0; i < partitions.length; i++) {
            indexHelpersOpen[i] = true;
            indexHelpers[i].open();
            indexes[i] = indexHelpers[i].getIndexInstance();
            searchCallbacks[i] = searchCallbackFactory
                    .createSearchOperationCallback(indexHelpers[i].getResource().getId(), ctx, null);
            iaps[i] = new IndexAccessParameters(NoOpOperationCallback.INSTANCE, searchCallbacks[i]);
            addAdditionalIndexAccessorParams(iaps[i]);
            indexAccessors[i] = indexes[i].createAccessor(iaps[i]);
            cursors[i] = createCursor(indexes[i], indexAccessors[i]);
        }

        subscribeForStats(indexes[0]);
        accessor = new FrameTupleAccessor(inputRecDesc);
        if (retainMissing) {
            int fieldCount = getFieldCount(indexes[0]);
            // Field count in case searchCallback.proceed() result is needed.
            int finalFieldCount = appendSearchCallbackProceedResult ? fieldCount + 1 : fieldCount;
            nonMatchTupleBuild = new ArrayTupleBuilder(finalFieldCount);
            buildMissingTuple(fieldCount, nonMatchTupleBuild, nonMatchWriter);
            if (appendSearchCallbackProceedResult) {
                // Writes the success result in the last field in case we need to write down
                // the result of searchOperationCallback.proceed(). This value can't be missing even for this case.
                writeSearchCallbackProceedResult(nonMatchTupleBuild, true);
            }
        } else {
            nonMatchTupleBuild = null;
        }
        if (appendIndexFilter) {
            int numIndexFilterFields = indexes[0].getNumOfFilterFields();
            nonFilterTupleBuild = new ArrayTupleBuilder(numIndexFilterFields);
            buildMissingTuple(numIndexFilterFields, nonFilterTupleBuild, nonFilterWriter);
        }

        if (tupleFilterFactory != null) {
            tupleFilter = tupleFilterFactory.createTupleFilter(ctx);
            referenceFilterTuple = new ReferenceFrameTupleReference();
        }
        finished = false;
        outputCount = 0;

        try {
            searchPred = createSearchPredicate(indexes[0]);
            tb = new ArrayTupleBuilder(recordDesc.getFieldCount());
            dos = tb.getDataOutput();
            if (retainInput) {
                frameTuple = new FrameTupleReference();
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void writeSearchResults(int tupleIndex, IIndexCursor cursor) throws Exception {
        long matchingTupleCount = 0;
        while (cursor.hasNext()) {
            cursor.next();
            matchingTupleCount++;
            ITupleReference tuple = cursor.getTuple();
            tb.reset();

            if (retainInput) {
                frameTuple.reset(accessor, tupleIndex);
                for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                    dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    tb.addFieldEndOffset();
                }
            }

            // tuple must be written first before the filter is applied to
            // assemble columnar tuples
            tuple = writeTupleToOutput(tuple);
            if (tuple == null) {
                continue;
            }
            if (tupleFilter != null) {
                referenceFilterTuple.reset(tuple);
                if (!tupleFilter.accept(referenceFilterTuple)) {
                    continue;
                }
            }

            if (appendSearchCallbackProceedResult) {
                writeSearchCallbackProceedResult(tb,
                        ((ILSMIndexCursor) cursor).getSearchOperationCallbackProceedResult());
            }
            if (appendIndexFilter) {
                writeFilterTupleToOutput(((ILSMIndexCursor) cursor).getFilterMinTuple());
                writeFilterTupleToOutput(((ILSMIndexCursor) cursor).getFilterMaxTuple());
            }
            FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            if (outputLimit >= 0 && ++outputCount >= outputLimit) {
                finished = true;
                break;
            }
        }
        stats.getInputTupleCounter().update(matchingTupleCount);

        if (matchingTupleCount == 0 && retainInput && retainMissing) {
            FrameUtils.appendConcatToWriter(writer, appender, accessor, tupleIndex,
                    nonMatchTupleBuild.getFieldEndOffsets(), nonMatchTupleBuild.getByteArray(), 0,
                    nonMatchTupleBuild.getSize());
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        try {
            if (tuplePartitioner != null) {
                searchPartition(tupleCount);
            } else {
                searchAllPartitions(tupleCount);
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        appender.flush(writer);
    }

    @Override
    public void close() throws HyracksDataException {
        Throwable failure = flushFrame();
        failure = releaseResources(failure);
        failure = CleanupUtils.close(writer, failure);
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }

    private Throwable flushFrame() {
        Throwable failure = null;
        if (!failed) {
            try {
                if (appender.getTupleCount() > 0) {
                    appender.write(writer, true);
                }
                stats.getPageReads().update(ctx.getThreadStats().getPinnedPagesCount());
                stats.coldReadCounter().update(ctx.getThreadStats().getColdReadCount());
            } catch (Throwable th) { // NOSONAR Must ensure writer.fail is called.
                // subsequently, the failure will be thrown
                failure = th;
            }
            if (failure != null) {
                try {
                    writer.fail();
                } catch (Throwable th) {
                    // subsequently, the failure will be thrown
                    failure = ExceptionUtils.suppress(failure, th);
                }
            }
        }
        return failure;
    }

    private Throwable releaseResources(Throwable failure) {
        for (int i = 0; i < indexes.length; i++) {
            try {
                if (indexes[i] != null) {
                    failure = ResourceReleaseUtils.close(cursors[i], failure);
                    failure = CleanupUtils.destroy(failure, cursors[i], indexAccessors[i]);
                    failure = ResourceReleaseUtils.close(indexHelpers[i], failure);
                } else if (indexHelpersOpen[i]) {
                    // can mean the index was open, but getting the index instance failed (index == null)
                    // or opening the index itself failed at some step during the open
                    failure = ResourceReleaseUtils.close(indexHelpers[i], failure);
                    //TODO(ali): IIndexDataflowHelper.close() should be made idempotent and flags should be removed
                }
            } catch (Throwable th) {// NOSONAR ensure closing other indexes
                // subsequently, the failure will be thrown
                failure = ExceptionUtils.suppress(failure, th);
            }
        }
        return failure;
    }

    @Override
    public void fail() throws HyracksDataException {
        failed = true;
        writer.fail();
    }

    private void subscribeForStats(IIndex index) {
        if (index.getBufferCache() instanceof IThreadStatsCollector) {
            ctx.subscribeThreadToStats((IThreadStatsCollector) index.getBufferCache());
        }
    }

    protected ITupleReference writeTupleToOutput(ITupleReference tuple) throws IOException {
        try {
            return tupleProjector.project(tuple, dos, tb);
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Write the result of a SearchCallback.proceed() if it is needed.
     */
    private void writeSearchCallbackProceedResult(ArrayTupleBuilder atb, boolean searchCallbackProceedResult)
            throws HyracksDataException {
        if (!searchCallbackProceedResult) {
            atb.addField(searchCallbackProceedResultFalseValue, 0, searchCallbackProceedResultFalseValue.length);
        } else {
            atb.addField(searchCallbackProceedResultTrueValue, 0, searchCallbackProceedResultTrueValue.length);
        }
    }

    private void writeFilterTupleToOutput(ITupleReference tuple) throws IOException {
        if (tuple != null) {
            writeTupleToOutput(tuple);
        } else {
            int[] offsets = nonFilterTupleBuild.getFieldEndOffsets();
            for (int i = 0; i < offsets.length; i++) {
                int start = i > 0 ? offsets[i - 1] : 0;
                tb.addField(nonFilterTupleBuild.getByteArray(), start, offsets[i]);
            }
        }
    }

    private static void buildMissingTuple(int numFields, ArrayTupleBuilder nullTuple, IMissingWriter nonMatchWriter) {
        DataOutput out = nullTuple.getDataOutput();
        for (int i = 0; i < numFields; i++) {
            try {
                nonMatchWriter.writeMissing(out);
            } catch (Exception e) {
                LOGGER.log(Level.WARN, e.getMessage(), e);
            }
            nullTuple.addFieldEndOffset();
        }
    }

    @Override
    public String getDisplayName() {
        return "Index Search";
    }

    @Override
    public void setOperatorStats(IOperatorStats stats) {
        this.stats = stats;
    }

    private void searchPartition(int tupleCount) throws Exception {
        for (int i = 0; i < tupleCount && !finished; i++) {
            int storagePartition = tuplePartitioner.partition(accessor, i);
            int pIdx = storagePartitionId2Index.get(storagePartition);
            resetSearchPredicate(i);
            cursors[pIdx].close();
            indexAccessors[pIdx].search(cursors[pIdx], searchPred);
            writeSearchResults(i, cursors[pIdx]);
        }
    }

    private void searchAllPartitions(int tupleCount) throws Exception {
        for (int p = 0; p < partitions.length; p++) {
            for (int i = 0; i < tupleCount && !finished; i++) {
                resetSearchPredicate(i);
                cursors[p].close();
                indexAccessors[p].search(cursors[p], searchPred);
                writeSearchResults(i, cursors[p]);
            }
        }
    }
}
