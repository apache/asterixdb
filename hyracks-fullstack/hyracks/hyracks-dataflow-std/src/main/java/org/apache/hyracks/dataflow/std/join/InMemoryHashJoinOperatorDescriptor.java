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
package org.apache.hyracks.dataflow.std.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;

public class InMemoryHashJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final int[] keys0;
    private final int[] keys1;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories0;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories1;
    private final ITuplePairComparatorFactory comparatorFactory;
    private final IPredicateEvaluatorFactory predEvaluatorFactory;
    private final boolean isLeftOuter;
    private final IMissingWriterFactory[] nonMatchWriterFactories;
    private final int tableSize;
    // The maximum number of in-memory frames that this hash join can use.
    private final int memSizeInFrames;

    public InMemoryHashJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int[] keys0, int[] keys1,
            IBinaryHashFunctionFactory[] hashFunctionFactories0, IBinaryHashFunctionFactory[] hashFunctionFactories1,
            ITuplePairComparatorFactory comparatorFactory, RecordDescriptor recordDescriptor, int tableSize,
            IPredicateEvaluatorFactory predEvalFactory, int memSizeInFrames) {
        super(spec, 2, 1);
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.hashFunctionFactories0 = hashFunctionFactories0;
        this.hashFunctionFactories1 = hashFunctionFactories1;
        this.comparatorFactory = comparatorFactory;
        this.predEvaluatorFactory = predEvalFactory;
        outRecDescs[0] = recordDescriptor;
        this.isLeftOuter = false;
        this.nonMatchWriterFactories = null;
        this.tableSize = tableSize;
        this.memSizeInFrames = memSizeInFrames;
    }

    public InMemoryHashJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int[] keys0, int[] keys1,
            IBinaryHashFunctionFactory[] hashFunctionFactories0, IBinaryHashFunctionFactory[] hashFunctionFactories1,
            ITuplePairComparatorFactory comparatorFactory, IPredicateEvaluatorFactory predEvalFactory,
            RecordDescriptor recordDescriptor, boolean isLeftOuter, IMissingWriterFactory[] missingWriterFactories1,
            int tableSize, int memSizeInFrames) {
        super(spec, 2, 1);
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.hashFunctionFactories0 = hashFunctionFactories0;
        this.hashFunctionFactories1 = hashFunctionFactories1;
        this.comparatorFactory = comparatorFactory;
        this.predEvaluatorFactory = predEvalFactory;
        outRecDescs[0] = recordDescriptor;
        this.isLeftOuter = isLeftOuter;
        this.nonMatchWriterFactories = missingWriterFactories1;
        this.tableSize = tableSize;
        this.memSizeInFrames = memSizeInFrames;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId hbaId = new ActivityId(odId, 0);
        ActivityId hpaId = new ActivityId(odId, 1);
        HashBuildActivityNode hba = new HashBuildActivityNode(hbaId, hpaId);
        HashProbeActivityNode hpa = new HashProbeActivityNode(hpaId);

        builder.addActivity(this, hba);
        builder.addSourceEdge(1, hba, 0);

        builder.addActivity(this, hpa);
        builder.addSourceEdge(0, hpa, 0);

        builder.addTargetEdge(0, hpa, 0);

        builder.addBlockingEdge(hba, hpa);
    }

    public static class HashBuildTaskState extends AbstractStateObject {
        private InMemoryHashJoin joiner;

        public HashBuildTaskState() {
        }

        private HashBuildTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }
    }

    private class HashBuildActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId hpaId;

        public HashBuildActivityNode(ActivityId id, ActivityId hpaId) {
            super(id);
            this.hpaId = hpaId;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(hpaId, 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final ITuplePairComparator comparator = comparatorFactory.createTuplePairComparator(ctx);
            final IMissingWriter[] nullWriters1 =
                    isLeftOuter ? new IMissingWriter[nonMatchWriterFactories.length] : null;
            if (isLeftOuter) {
                for (int i = 0; i < nonMatchWriterFactories.length; i++) {
                    nullWriters1[i] = nonMatchWriterFactories[i].createMissingWriter();
                }
            }
            final IPredicateEvaluator predEvaluator =
                    (predEvaluatorFactory == null ? null : predEvaluatorFactory.createPredicateEvaluator());

            final int memSizeInBytes = memSizeInFrames * ctx.getInitialFrameSize();
            final IDeallocatableFramePool framePool = new DeallocatableFramePool(ctx, memSizeInBytes);
            final ISimpleFrameBufferManager bufferManager = new FramePoolBackedFrameBufferManager(framePool);

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private HashBuildTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    ITuplePartitionComputer hpc0 =
                            new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories0).createPartitioner(ctx);
                    ITuplePartitionComputer hpc1 =
                            new FieldHashPartitionComputerFactory(keys1, hashFunctionFactories1).createPartitioner(ctx);
                    state = new HashBuildTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    ISerializableTable table = new SerializableHashTable(tableSize, ctx, bufferManager);
                    state.joiner = new InMemoryHashJoin(ctx, new FrameTupleAccessor(rd0), hpc0,
                            new FrameTupleAccessor(rd1), rd1, hpc1, comparator, isLeftOuter, nullWriters1, table,
                            predEvaluator, bufferManager);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    ByteBuffer copyBuffer = allocateBuffer(buffer.capacity());
                    FrameUtils.copyAndFlip(buffer, copyBuffer);
                    state.joiner.build(copyBuffer);
                }

                private ByteBuffer allocateBuffer(int frameSize) throws HyracksDataException {
                    ByteBuffer newBuffer = bufferManager.acquireFrame(frameSize);
                    if (newBuffer != null) {
                        return newBuffer;
                    }
                    // At this moment, there is no enough memory since the newBuffer is null.
                    // But, there may be a chance if we can compact the table, one or more frame may be reclaimed.
                    if (state.joiner.compactHashTable() > 0) {
                        newBuffer = bufferManager.acquireFrame(frameSize);
                        if (newBuffer != null) {
                            return newBuffer;
                        }
                    }
                    // At this point, we have no way to get a frame.
                    throw new HyracksDataException(
                            "Can't allocate one more frame. Assign more memory to InMemoryHashJoin.");
                }

                @Override
                public void close() throws HyracksDataException {
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
            return op;
        }
    }

    private class HashProbeActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public HashProbeActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private HashBuildTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    writer.open();
                    state = (HashBuildTaskState) ctx
                            .getStateObject(new TaskId(new ActivityId(getOperatorId(), 0), partition));
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.joiner.join(buffer, writer);
                }

                @Override
                public void close() throws HyracksDataException {
                    try {
                        state.joiner.completeJoin(writer);
                    } finally {
                        try {
                            state.joiner.releaseMemory();
                        } finally {
                            writer.close();
                        }
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }
            };
            return op;
        }
    }
}
