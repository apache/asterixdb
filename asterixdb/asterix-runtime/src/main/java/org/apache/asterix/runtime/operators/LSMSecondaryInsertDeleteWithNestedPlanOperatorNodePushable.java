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
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.tuples.ConcatenatingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMIndexInsertUpdateDeleteOperatorNodePushable;

public class LSMSecondaryInsertDeleteWithNestedPlanOperatorNodePushable
        extends LSMIndexInsertUpdateDeleteOperatorNodePushable {
    private final NestedTupleSourceRuntime[] startOfPipelines;
    private final int numberOfPrimaryKeyAndFilterFields;

    public LSMSecondaryInsertDeleteWithNestedPlanOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            int[] fieldPermutation, RecordDescriptor inputRecDesc, IndexOperation op,
            IIndexDataflowHelperFactory indexHelperFactory, IModificationOperationCallbackFactory modCallbackFactory,
            List<AlgebricksPipeline> secondaryKeysPipeline, ITuplePartitionerFactory tuplePartitionerFactory,
            int[][] partitionsMap) throws HyracksDataException {
        super(ctx, partition, indexHelperFactory, fieldPermutation, inputRecDesc, op, modCallbackFactory, null,
                tuplePartitionerFactory, partitionsMap);
        this.numberOfPrimaryKeyAndFilterFields = fieldPermutation.length;

        // Build our pipeline.
        startOfPipelines = new NestedTupleSourceRuntime[secondaryKeysPipeline.size()];
        PipelineAssembler[] pipelineAssemblers = new PipelineAssembler[secondaryKeysPipeline.size()];
        for (int p = 0; p < secondaryKeysPipeline.size(); p++) {
            AlgebricksPipeline pipeline = secondaryKeysPipeline.get(p);
            RecordDescriptor lastRecordDescriptorInPipeline =
                    pipeline.getRecordDescriptors()[pipeline.getRecordDescriptors().length - 1];

            IFrameWriter outputWriter;
            if (p == 0) {
                // Primary pipeline (the first). Here we append the PK, filters to the final variable.
                outputWriter = new IndexTupleInsertDelete(lastRecordDescriptorInPipeline);

            } else {
                IPushRuntime outputPushRuntime = PipelineAssembler.linkPipeline(pipeline, pipelineAssemblers, p);
                if (outputPushRuntime == null) {
                    throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "Invalid pipeline.");
                }
                outputPushRuntime.setInputRecordDescriptor(0, lastRecordDescriptorInPipeline);
                outputWriter = outputPushRuntime;
            }

            PipelineAssembler pipelineAssembler =
                    new PipelineAssembler(pipeline, 1, 1, inputRecDesc, lastRecordDescriptorInPipeline);
            startOfPipelines[p] = (NestedTupleSourceRuntime) pipelineAssembler.assemblePipeline(outputWriter, ctx);
            pipelineAssemblers[p] = pipelineAssembler;
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            // Pass the input to our pipeline. The last operator in this pipeline will pass all of the tuples
            // to our LSM accessor.
            tuple.reset(accessor, i);

            for (NestedTupleSourceRuntime nts : startOfPipelines) {
                nts.writeTuple(buffer, i);
            }

            int n = 0;
            try {
                for (; n < startOfPipelines.length; n++) {
                    NestedTupleSourceRuntime nts = startOfPipelines[n];
                    try {
                        nts.open();
                    } catch (Exception e) {
                        nts.fail();
                        throw e;
                    }
                }
            } finally {
                for (int j = n - 1; j >= 0; j--) {
                    startOfPipelines[j].close();
                }
            }
        }

        // No partial flushing was necessary. Forward entire frame.
        writeBuffer.ensureFrameSize(buffer.capacity());
        FrameUtils.copyAndFlip(buffer, writeBuffer.getBuffer());
        FrameUtils.flushFrame(writeBuffer.getBuffer(), writer);
    }

    private class IndexTupleInsertDelete implements IFrameWriter {
        private final RecordDescriptor inputRecordDescriptor;
        private FrameTupleAccessor endOfPipelineTupleAccessor;
        private FrameTupleReference endOfPipelineTupleReference;
        private ConcatenatingTupleReference endTupleReference;

        private IndexTupleInsertDelete(RecordDescriptor recordDescriptor) {
            this.inputRecordDescriptor = recordDescriptor;
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
                endTupleReference.reset();

                // Add the secondary keys.
                endTupleReference.addTuple(endOfPipelineTupleReference);

                // Add the primary keys and filter fields.
                endTupleReference.addTuple(tuple);

                int storagePartition = tuplePartitioner.partition(tuple.getFrameTupleAccessor(), tuple.getTupleIndex());
                int storageIdx = storagePartitionId2Index.get(storagePartition);
                ILSMIndexAccessor workingLSMAccessor = (ILSMIndexAccessor) indexAccessors[storageIdx];
                // Pass the tuple to our accessor. There are only two operations: insert or delete.
                if (op.equals(IndexOperation.INSERT)) {
                    try {
                        workingLSMAccessor.forceInsert(endTupleReference);
                    } catch (HyracksDataException e) {
                        if (!e.matches(org.apache.hyracks.api.exceptions.ErrorCode.DUPLICATE_KEY)) {
                            throw e;
                        }
                    }
                } else {
                    try {
                        workingLSMAccessor.forceDelete(endTupleReference);
                    } catch (HyracksDataException e) {
                        if (!e.matches(org.apache.hyracks.api.exceptions.ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                            throw e;
                        }
                    }
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
