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
package org.apache.asterix.common.dataflow;

import java.nio.ByteBuffer;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.transactions.ILogMarkerCallback;
import org.apache.asterix.common.transactions.PrimaryIndexLogMarkerCallback;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMIndexInsertUpdateDeleteOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;

public class LSMInsertDeleteOperatorNodePushable extends LSMIndexInsertUpdateDeleteOperatorNodePushable {

    public static final String KEY_INDEX = "Index";
    private final boolean isPrimary;
    private final SourceLocation sourceLoc;
    // This class has both lsmIndex and index (in super class) pointing to the same object
    private AbstractLSMIndex lsmIndex;
    private int i = 0;

    /**
     * The following three variables are used to keep track of the information regarding flushing partial frame such as
     * 1. whether there was a partial frame flush for the current frame,
     * ==> captured in flushedPartialTuples variable
     * 2. the last flushed tuple index in the frame if there was a partial frame flush,
     * ==> captured in lastFlushedTupleIdx variable
     * 3. the current tuple index the frame, where this operator is working on the current tuple.
     * ==> captured in currentTupleIdx variable
     * These variables are reset for each frame, i.e., whenever nextFrame() is called, these variables are reset.
     */
    private boolean flushedPartialTuples;
    private int currentTupleIdx;
    private int lastFlushedTupleIdx;

    public LSMInsertDeleteOperatorNodePushable(IHyracksTaskContext ctx, int partition, int[] fieldPermutation,
            RecordDescriptor inputRecDesc, IndexOperation op, boolean isPrimary,
            IIndexDataflowHelperFactory indexHelperFactory, IModificationOperationCallbackFactory modCallbackFactory,
            ITupleFilterFactory tupleFilterFactory, SourceLocation sourceLoc) throws HyracksDataException {
        super(ctx, partition, indexHelperFactory, fieldPermutation, inputRecDesc, op, modCallbackFactory,
                tupleFilterFactory);
        this.isPrimary = isPrimary;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public void open() throws HyracksDataException {
        i = 0;
        currentTupleIdx = 0;
        lastFlushedTupleIdx = 0;
        flushedPartialTuples = false;
        accessor = new FrameTupleAccessor(inputRecDesc);
        writeBuffer = new VSizeFrame(ctx);
        appender = new FrameTupleAppender(writeBuffer);
        indexHelper.open();
        lsmIndex = (AbstractLSMIndex) indexHelper.getIndexInstance();
        try {
            if (isPrimary && ctx.getSharedObject() != null) {
                PrimaryIndexLogMarkerCallback callback = new PrimaryIndexLogMarkerCallback(lsmIndex);
                TaskUtil.put(ILogMarkerCallback.KEY_MARKER_CALLBACK, callback, ctx);
            }
            writer.open();
            modCallback =
                    modOpCallbackFactory.createModificationOperationCallback(indexHelper.getResource(), ctx, this);
            IIndexAccessParameters iap = new IndexAccessParameters(modCallback, NoOpOperationCallback.INSTANCE);
            indexAccessor = lsmIndex.createAccessor(iap);
            if (tupleFilterFactory != null) {
                tupleFilter = tupleFilterFactory.createTupleFilter(ctx);
                frameTuple = new FrameTupleReference();
            }
            INcApplicationContext runtimeCtx =
                    (INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();
            LSMIndexUtil.checkAndSetFirstLSN(lsmIndex, runtimeCtx.getTransactionSubsystem().getLogManager());
        } catch (Throwable th) {
            throw HyracksDataException.create(th);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        ILSMIndexAccessor lsmAccessor = (ILSMIndexAccessor) indexAccessor;
        int tupleCount = accessor.getTupleCount();
        try {
            for (; i < tupleCount; i++, currentTupleIdx++) {
                if (tupleFilter != null) {
                    frameTuple.reset(accessor, i);
                    if (!tupleFilter.accept(frameTuple)) {
                        continue;
                    }
                }
                tuple.reset(accessor, i);
                switch (op) {
                    case INSERT:
                        if (i == 0 && isPrimary) {
                            lsmAccessor.insert(tuple);
                        } else {
                            lsmAccessor.forceInsert(tuple);
                        }
                        break;
                    case DELETE:
                        if (i == 0 && isPrimary) {
                            lsmAccessor.delete(tuple);
                        } else {
                            lsmAccessor.forceDelete(tuple);
                        }
                        break;
                    default: {
                        throw HyracksDataException.create(ErrorCode.INVALID_OPERATOR_OPERATION, sourceLoc,
                                op.toString(), LSMInsertDeleteOperatorNodePushable.class.getSimpleName());
                    }
                }
            }
        } catch (HyracksDataException e) {
            if (e.getErrorCode() == ErrorCode.INVALID_OPERATOR_OPERATION) {
                throw e;
            } else {
                throw HyracksDataException.create(ErrorCode.ERROR_PROCESSING_TUPLE, e, sourceLoc, i);
            }
        } catch (Exception e) {
            throw HyracksDataException.create(ErrorCode.ERROR_PROCESSING_TUPLE, e, sourceLoc, i);
        }

        writeBuffer.ensureFrameSize(buffer.capacity());
        if (flushedPartialTuples) {
            flushPartialFrame();
        } else {
            FrameUtils.copyAndFlip(buffer, writeBuffer.getBuffer());
            FrameUtils.flushFrame(writeBuffer.getBuffer(), writer);
        }
        i = 0;
        currentTupleIdx = 0;
        lastFlushedTupleIdx = 0;
        flushedPartialTuples = false;
    }

    /**
     * flushes tuples in a frame from lastFlushedTupleIdx(inclusive) to currentTupleIdx(exclusive)
     */
    @Override
    public void flushPartialFrame() throws HyracksDataException {
        if (lastFlushedTupleIdx == currentTupleIdx) {
            //nothing to flush
            return;
        }
        for (int i = lastFlushedTupleIdx; i < currentTupleIdx; i++) {
            FrameUtils.appendToWriter(writer, appender, accessor, i);
        }
        appender.write(writer, true);
        lastFlushedTupleIdx = currentTupleIdx;
        flushedPartialTuples = true;
    }

    @Override
    public void close() throws HyracksDataException {
        if (lsmIndex != null) {
            try {
                indexHelper.close();
            } finally {
                writer.close();
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        if (lsmIndex != null) {
            writer.fail();
        }
    }

    public boolean isPrimary() {
        return isPrimary;
    }
}
