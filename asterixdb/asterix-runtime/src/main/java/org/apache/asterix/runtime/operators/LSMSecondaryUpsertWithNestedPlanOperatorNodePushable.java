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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback;
import org.apache.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.meta.PipelineAssembler;
import org.apache.hyracks.algebricks.runtime.operators.std.NestedTupleSourceRuntimeFactory.NestedTupleSourceRuntime;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.tuples.ConcatenatingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;

public class LSMSecondaryUpsertWithNestedPlanOperatorNodePushable extends LSMSecondaryUpsertOperatorNodePushable {
    private final NestedTupleSourceRuntime[] startOfNewKeyPipelines;
    private final NestedTupleSourceRuntime[] startOfPrevKeyPipelines;
    private final int numberOfPrimaryKeyAndFilterFields;

    public LSMSecondaryUpsertWithNestedPlanOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            IIndexDataflowHelperFactory indexHelperFactory, IModificationOperationCallbackFactory modCallbackFactory,
            int[] fieldPermutation, RecordDescriptor inputRecDesc, int operationFieldIndex,
            IBinaryIntegerInspectorFactory operationInspectorFactory, List<AlgebricksPipeline> secondaryKeysPipeline,
            List<AlgebricksPipeline> prevSecondaryKeysPipeline, ITuplePartitionerFactory tuplePartitionerFactory,
            int[][] partitionsMap) throws HyracksDataException {
        super(ctx, partition, indexHelperFactory, modCallbackFactory, null, null, fieldPermutation, inputRecDesc,
                operationFieldIndex, operationInspectorFactory, null, tuplePartitionerFactory, partitionsMap);
        this.numberOfPrimaryKeyAndFilterFields = fieldPermutation.length;
        this.startOfNewKeyPipelines = buildStartOfPipelines(secondaryKeysPipeline, inputRecDesc, false);
        this.startOfPrevKeyPipelines = buildStartOfPipelines(prevSecondaryKeysPipeline, inputRecDesc, true);
    }

    private NestedTupleSourceRuntime[] buildStartOfPipelines(List<AlgebricksPipeline> pipelines,
            RecordDescriptor inputRecordDescriptor, boolean isPrev) throws HyracksDataException {
        NestedTupleSourceRuntime[] resultant = new NestedTupleSourceRuntime[pipelines.size()];
        PipelineAssembler[] pipelineAssemblers = new PipelineAssembler[pipelines.size()];
        for (int p = 0; p < pipelines.size(); p++) {
            AlgebricksPipeline pipeline = pipelines.get(p);
            RecordDescriptor lastRecordDescriptorInPipeline =
                    pipeline.getRecordDescriptors()[pipeline.getRecordDescriptors().length - 1];

            IFrameWriter outputWriter;
            if (p == 0) {
                // Primary pipeline (the first). Here we perform the insert / delete.
                outputWriter = new IndexTupleUnconditionalOperation(lastRecordDescriptorInPipeline, !isPrev);

            } else {
                IPushRuntime outputPushRuntime = PipelineAssembler.linkPipeline(pipeline, pipelineAssemblers, p);
                if (outputPushRuntime == null) {
                    throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "No output runtime factories found.");
                }
                outputPushRuntime.setInputRecordDescriptor(0, lastRecordDescriptorInPipeline);
                outputWriter = outputPushRuntime;
            }

            PipelineAssembler pipelineAssembler =
                    new PipelineAssembler(pipeline, 1, 1, inputRecordDescriptor, lastRecordDescriptorInPipeline);
            resultant[p] = (NestedTupleSourceRuntime) pipelineAssembler.assemblePipeline(outputWriter, ctx);
            pipelineAssemblers[p] = pipelineAssembler;
        }

        return resultant;
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        frameTuple = new FrameTupleReference();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            tuple.reset(accessor, i);

            // Delete all of our old keys.
            writeTupleToPipelineStarts(buffer, i, startOfPrevKeyPipelines);

            // Insert all of our new keys, if the PIDX operation was also an UPSERT (and not just a DELETE).
            frameTuple.reset(accessor, i);
            int operation = operationInspector.getIntegerValue(frameTuple.getFieldData(operationFieldIndex),
                    frameTuple.getFieldStart(operationFieldIndex), frameTuple.getFieldLength(operationFieldIndex));
            if (operation == UPSERT_NEW || operation == UPSERT_EXISTING) {
                writeTupleToPipelineStarts(buffer, i, startOfNewKeyPipelines);
            }
        }

        // No partial flushing was necessary. Forward entire frame.
        writeBuffer.ensureFrameSize(buffer.capacity());
        FrameUtils.copyAndFlip(buffer, writeBuffer.getBuffer());
        FrameUtils.flushFrame(writeBuffer.getBuffer(), writer);
    }

    private void writeTupleToPipelineStarts(ByteBuffer buffer, int tupleIndex,
            NestedTupleSourceRuntime[] pipelineStarts) throws HyracksDataException {
        for (NestedTupleSourceRuntime nts : pipelineStarts) {
            nts.writeTuple(buffer, tupleIndex);
        }

        int n = 0;
        try {
            for (; n < pipelineStarts.length; n++) {
                NestedTupleSourceRuntime nts = pipelineStarts[n];
                try {
                    nts.open();
                } catch (Exception e) {
                    nts.fail();
                    throw e;
                }
            }
        } finally {
            for (int j = n - 1; j >= 0; j--) {
                pipelineStarts[j].close();
            }
        }
    }

    private class IndexTupleUnconditionalOperation implements IFrameWriter {
        private final RecordDescriptor inputRecordDescriptor;
        private final boolean isInsert; // If this is not an insert, then our operation is delete.

        private FrameTupleAccessor endOfPipelineTupleAccessor;
        private FrameTupleReference endOfPipelineTupleReference;
        private ConcatenatingTupleReference endTupleReference;

        private IndexTupleUnconditionalOperation(RecordDescriptor recordDescriptor, boolean isInsert) {
            this.inputRecordDescriptor = recordDescriptor;
            this.isInsert = isInsert;
        }

        @Override
        public void open() throws HyracksDataException {
            endTupleReference = new ConcatenatingTupleReference(2);
            endOfPipelineTupleAccessor = new FrameTupleAccessor(inputRecordDescriptor);
            endOfPipelineTupleReference = new FrameTupleReference();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {

            endOfPipelineTupleAccessor.reset(buffer);
            int nTuple = endOfPipelineTupleAccessor.getTupleCount();
            for (int t = 0; t < nTuple; t++) {
                endOfPipelineTupleReference.reset(endOfPipelineTupleAccessor, t);

                // Do not perform operations w/ null or missing values (same behavior as atomic upserts).
                if (hasNullOrMissing(endOfPipelineTupleReference)) {
                    continue;
                }

                // Add the secondary keys.
                endTupleReference.reset();
                endTupleReference.addTuple(endOfPipelineTupleReference);

                // Add the primary keys and filter fields.
                endTupleReference.addTuple(tuple);

                int storagePartition = tuplePartitioner.partition(tuple.getFrameTupleAccessor(), tuple.getTupleIndex());
                int storageIdx = storagePartitionId2Index.get(storagePartition);
                ILSMIndexAccessor workingLSMAccessor = (ILSMIndexAccessor) indexAccessors[storageIdx];
                IModificationOperationCallback abstractModCallback = modCallbacks[storageIdx];
                // Finally, pass the tuple to our accessor. There are only two operations: insert or delete.
                if (this.isInsert) {
                    if (abstractModCallback instanceof AbstractIndexModificationOperationCallback) {
                        ((AbstractIndexModificationOperationCallback) abstractModCallback)
                                .setOp(AbstractIndexModificationOperationCallback.Operation.INSERT);
                    }
                    try {
                        workingLSMAccessor.forceInsert(endTupleReference);
                    } catch (HyracksDataException e) {
                        if (!e.matches(org.apache.hyracks.api.exceptions.ErrorCode.DUPLICATE_KEY)) {
                            throw e;
                        }
                    }
                } else {
                    if (abstractModCallback instanceof AbstractIndexModificationOperationCallback) {
                        ((AbstractIndexModificationOperationCallback) abstractModCallback)
                                .setOp(AbstractIndexModificationOperationCallback.Operation.DELETE);
                    }
                    workingLSMAccessor.forceDelete(endTupleReference);
                }
            }
        }

        @Override
        public void fail() throws HyracksDataException {
        }

        @Override
        public void close() throws HyracksDataException {
        }
    }
}
