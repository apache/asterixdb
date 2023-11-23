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
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.messaging.AtomicJobPreparedMessage;
import org.apache.asterix.common.transactions.ILogMarkerCallback;
import org.apache.asterix.common.transactions.PrimaryIndexLogMarkerCallback;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMIndexInsertUpdateDeleteOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.FlushOperation;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.LocalResource;

public class LSMInsertDeleteOperatorNodePushable extends LSMIndexInsertUpdateDeleteOperatorNodePushable {

    protected final boolean isPrimary;
    protected final SourceLocation sourceLoc;
    protected int i = 0;

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
    protected boolean flushedPartialTuples;
    protected int currentTupleIdx;
    protected int lastFlushedTupleIdx;

    public LSMInsertDeleteOperatorNodePushable(IHyracksTaskContext ctx, int partition, int[] fieldPermutation,
            RecordDescriptor inputRecDesc, IndexOperation op, boolean isPrimary,
            IIndexDataflowHelperFactory indexHelperFactory, IModificationOperationCallbackFactory modCallbackFactory,
            ITupleFilterFactory tupleFilterFactory, SourceLocation sourceLoc,
            ITuplePartitionerFactory tuplePartitionerFactory, int[][] partitionsMap) throws HyracksDataException {
        super(ctx, partition, indexHelperFactory, fieldPermutation, inputRecDesc, op, modCallbackFactory,
                tupleFilterFactory, tuplePartitionerFactory, partitionsMap);
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
        try {
            INcApplicationContext runtimeCtx =
                    (INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();

            for (int i = 0; i < indexHelpers.length; i++) {
                IIndexDataflowHelper indexHelper = indexHelpers[i];
                indexHelpersOpen[i] = true;
                indexHelper.open();
                indexes[i] = indexHelper.getIndexInstance();
                if (((ILSMIndex) indexes[i]).isAtomic() && isPrimary()) {
                    ((PrimaryIndexOperationTracker) ((ILSMIndex) indexes[i]).getOperationTracker()).clear();
                }
                LocalResource resource = indexHelper.getResource();
                modCallbacks[i] = modOpCallbackFactory.createModificationOperationCallback(resource, ctx, this);
                IIndexAccessParameters iap = new IndexAccessParameters(modCallbacks[i], NoOpOperationCallback.INSTANCE);
                indexAccessors[i] = indexes[i].createAccessor(iap);
                LSMIndexUtil.checkAndSetFirstLSN((AbstractLSMIndex) indexes[i],
                        runtimeCtx.getTransactionSubsystem().getLogManager());
            }

            if (isPrimary && ctx.getSharedObject() != null) {
                PrimaryIndexLogMarkerCallback callback = new PrimaryIndexLogMarkerCallback((ILSMIndex) indexes[0]);
                TaskUtil.put(ILogMarkerCallback.KEY_MARKER_CALLBACK, callback, ctx);
            }
            writer.open();
            writerOpen = true;
            if (tupleFilterFactory != null) {
                tupleFilter = tupleFilterFactory.createTupleFilter(ctx);
                frameTuple = new FrameTupleReference();
            }
        } catch (Throwable th) {
            throw HyracksDataException.create(th);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
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
                int storagePartition = tuplePartitioner.partition(accessor, i);
                int storageIdx = storagePartitionId2Index.get(storagePartition);
                ILSMIndexAccessor lsmAccessor = (ILSMIndexAccessor) indexAccessors[storageIdx];
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
            if (e.matches(ErrorCode.INVALID_OPERATOR_OPERATION)) {
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
        Throwable failure = closeIndexHelpers(null);
        if (writerOpen) {
            try {
                writer.close();
            } catch (Throwable th) {
                failure = ExceptionUtils.suppress(failure, th);
            }
        }
        if (failure == null && !failed) {
            commitAtomicInsertDelete();
        } else {
            abortAtomicInsertDelete();
        }
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        this.failed = true;
        if (writerOpen) {
            writer.fail();
        }
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    private void commitAtomicInsertDelete() throws HyracksDataException {
        if (isPrimary) {
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
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void abortAtomicInsertDelete() throws HyracksDataException {
        if (isPrimary) {
            for (IIndex index : indexes) {
                if (index != null && ((ILSMIndex) index).isAtomic()) {
                    PrimaryIndexOperationTracker opTracker =
                            ((PrimaryIndexOperationTracker) ((ILSMIndex) index).getOperationTracker());
                    opTracker.abort();
                }
            }
        }
    }
}
