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

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
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
import org.apache.hyracks.storage.am.common.util.ResourceReleaseUtils;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.util.IThreadStatsCollector;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class IndexSearchOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    static final Logger LOGGER = LogManager.getLogger();
    protected final IHyracksTaskContext ctx;
    protected final IIndexDataflowHelper indexHelper;
    protected FrameTupleAccessor accessor;

    protected FrameTupleAppender appender;
    protected ArrayTupleBuilder tb;
    protected DataOutput dos;

    protected IIndex index;
    protected ISearchPredicate searchPred;
    protected IIndexCursor cursor;
    protected IIndexAccessor indexAccessor;

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
    protected final ISearchOperationCallbackFactory searchCallbackFactory;
    protected boolean failed = false;
    private IOperatorStats stats;

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

    // no filter and limit pushdown
    public IndexSearchOperatorNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecDesc, int partition,
            int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes, IIndexDataflowHelperFactory indexHelperFactory,
            boolean retainInput, boolean retainMissing, IMissingWriterFactory missingWriterFactory,
            ISearchOperationCallbackFactory searchCallbackFactory, boolean appendIndexFilter)
            throws HyracksDataException {
        this(ctx, inputRecDesc, partition, minFilterFieldIndexes, maxFilterFieldIndexes, indexHelperFactory,
                retainInput, retainMissing, missingWriterFactory, searchCallbackFactory, appendIndexFilter, null, -1,
                false, null, null);
    }

    public IndexSearchOperatorNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecDesc, int partition,
            int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes, IIndexDataflowHelperFactory indexHelperFactory,
            boolean retainInput, boolean retainMissing, IMissingWriterFactory missingWriterFactory,
            ISearchOperationCallbackFactory searchCallbackFactory, boolean appendIndexFilter,
            ITupleFilterFactory tupleFactoryFactory, long outputLimit, boolean appendSearchCallbackProceedResult,
            byte[] searchCallbackProceedResultFalseValue, byte[] searchCallbackProceedResultTrueValue)
            throws HyracksDataException {
        this.ctx = ctx;
        this.indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
        this.retainInput = retainInput;
        this.retainMissing = retainMissing;
        this.appendIndexFilter = appendIndexFilter;
        if (this.retainMissing || this.appendIndexFilter) {
            this.nonMatchWriter = missingWriterFactory.createMissingWriter();
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
        this.tupleFilterFactory = tupleFactoryFactory;
        this.outputLimit = outputLimit;

        if (ctx != null && ctx.getStatsCollector() != null) {
            stats = ctx.getStatsCollector().getOrAddOperatorStats(getDisplayName());
        }

        if (this.tupleFilterFactory != null && this.retainMissing) {
            throw new IllegalStateException("RetainMissing with tuple filter is not supported");
        }
    }

    protected abstract ISearchPredicate createSearchPredicate();

    protected abstract void resetSearchPredicate(int tupleIndex);

    // Assigns any index-type specific related accessor parameters
    protected abstract void addAdditionalIndexAccessorParams(IIndexAccessParameters iap) throws HyracksDataException;

    protected IIndexCursor createCursor() throws HyracksDataException {
        return indexAccessor.createSearchCursor(false);
    }

    protected abstract int getFieldCount();

    @Override
    public void open() throws HyracksDataException {
        writer.open();
        indexHelper.open();
        index = indexHelper.getIndexInstance();
        subscribeForStats(index);
        accessor = new FrameTupleAccessor(inputRecDesc);
        if (retainMissing) {
            int fieldCount = getFieldCount();
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
            int numIndexFilterFields = index.getNumOfFilterFields();
            nonFilterTupleBuild = new ArrayTupleBuilder(numIndexFilterFields);
            buildMissingTuple(numIndexFilterFields, nonFilterTupleBuild, nonMatchWriter);
        }

        if (tupleFilterFactory != null) {
            tupleFilter = tupleFilterFactory.createTupleFilter(ctx);
            referenceFilterTuple = new ReferenceFrameTupleReference();
        }
        finished = false;
        outputCount = 0;

        try {
            searchPred = createSearchPredicate();
            tb = new ArrayTupleBuilder(recordDesc.getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender(new VSizeFrame(ctx), true);
            ISearchOperationCallback searchCallback =
                    searchCallbackFactory.createSearchOperationCallback(indexHelper.getResource().getId(), ctx, null);
            IIndexAccessParameters iap = new IndexAccessParameters(NoOpOperationCallback.INSTANCE, searchCallback);
            addAdditionalIndexAccessorParams(iap);
            indexAccessor = index.createAccessor(iap);
            cursor = createCursor();
            if (retainInput) {
                frameTuple = new FrameTupleReference();
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void writeSearchResults(int tupleIndex) throws Exception {
        long matchingTupleCount = 0;
        while (cursor.hasNext()) {
            cursor.next();
            matchingTupleCount++;
            ITupleReference tuple = cursor.getTuple();
            if (tupleFilter != null && !tupleFilter.accept(referenceFilterTuple.reset(tuple))) {
                continue;
            }
            tb.reset();

            if (retainInput) {
                frameTuple.reset(accessor, tupleIndex);
                for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                    dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    tb.addFieldEndOffset();
                }
            }
            writeTupleToOutput(tuple);
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
        stats.getTupleCounter().update(matchingTupleCount);

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
            for (int i = 0; i < tupleCount && !finished; i++) {
                resetSearchPredicate(i);
                cursor.close();
                indexAccessor.search(cursor, searchPred);
                writeSearchResults(i);
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
        Throwable failure = releaseResources();
        failure = CleanupUtils.close(writer, failure);
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }

    private Throwable releaseResources() {
        Throwable failure = null;
        if (index != null) {
            // if index == null, then the index open was not successful
            if (!failed) {
                try {
                    if (appender.getTupleCount() > 0) {
                        appender.write(writer, true);
                    }
                    stats.getDiskIoCounter().update(ctx.getThreadStats().getPinnedPagesCount());
                } catch (Throwable th) { // NOSONAR Must ensure writer.fail is called.
                    // subsequently, the failure will be thrown
                    failure = th;
                }
                if (failure != null) {
                    try {
                        writer.fail();
                    } catch (Throwable th) {// NOSONAR Must cursor.close is called.
                        // subsequently, the failure will be thrown
                        failure = ExceptionUtils.suppress(failure, th);
                    }
                }
            }
            failure = ResourceReleaseUtils.close(cursor, failure);
            failure = CleanupUtils.destroy(failure, cursor, indexAccessor);
            failure = ResourceReleaseUtils.close(indexHelper, failure);
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

    private void writeTupleToOutput(ITupleReference tuple) throws IOException {
        try {
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
                tb.addFieldEndOffset();
            }
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

    /**
     * A wrapper class to wrap ITupleReference into IFrameTupleReference, as the latter
     * is used by ITupleFilter
     *
     */
    private static class ReferenceFrameTupleReference implements IFrameTupleReference {
        private ITupleReference tuple;

        public IFrameTupleReference reset(ITupleReference tuple) {
            this.tuple = tuple;
            return this;
        }

        @Override
        public int getFieldCount() {
            return tuple.getFieldCount();
        }

        @Override
        public byte[] getFieldData(int fIdx) {
            return tuple.getFieldData(fIdx);
        }

        @Override
        public int getFieldStart(int fIdx) {
            return tuple.getFieldStart(fIdx);
        }

        @Override
        public int getFieldLength(int fIdx) {
            return tuple.getFieldLength(fIdx);
        }

        @Override
        public IFrameTupleAccessor getFrameTupleAccessor() {
            throw new UnsupportedOperationException(
                    "getFrameTupleAccessor is not supported by ReferenceFrameTupleReference");
        }

        @Override
        public int getTupleIndex() {
            throw new UnsupportedOperationException("getTupleIndex is not supported by ReferenceFrameTupleReference");
        }

    }

    @Override
    public String getDisplayName() {
        return "Index Search";
    }

}
