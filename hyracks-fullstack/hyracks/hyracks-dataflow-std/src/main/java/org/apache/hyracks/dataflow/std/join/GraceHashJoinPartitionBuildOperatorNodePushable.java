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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

class GraceHashJoinPartitionBuildOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
    private final IHyracksTaskContext ctx;
    private final Object stateId;
    private final int numPartitions;
    private final IBinaryComparator[] comparators;
    private final FrameTupleAccessor accessor0;
    private final ITuplePartitionComputer hpc;
    private final FrameTupleAppender appender;
    private IFrame[] outbufs;
    private GraceHashJoinPartitionState state;

    GraceHashJoinPartitionBuildOperatorNodePushable(IHyracksTaskContext ctx, Object stateId, int[] keys,
            IBinaryHashFunctionFactory[] hashFunctionFactories, IBinaryComparatorFactory[] comparatorFactories,
            int numPartitions, RecordDescriptor inRecordDescriptor) {
        this.ctx = ctx;
        this.stateId = stateId;
        this.numPartitions = numPartitions;
        accessor0 = new FrameTupleAccessor(inRecordDescriptor);
        appender = new FrameTupleAppender();
        hpc = new FieldHashPartitionComputerFactory(keys, hashFunctionFactories).createPartitioner(ctx, -1);
        comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
    }

    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < numPartitions; i++) {
            ByteBuffer head = outbufs[i].getBuffer();
            accessor0.reset(head);
            if (accessor0.getTupleCount() > 0) {
                write(i, head);
            }
            closeWriter(i);
        }

        ctx.setStateObject(state);
    }

    private void closeWriter(int i) throws HyracksDataException {
        RunFileWriter writer = state.getRunWriters()[i];
        if (writer != null) {
            writer.close();
        }
    }

    private void write(int i, ByteBuffer head) throws HyracksDataException {
        RunFileWriter writer = state.getRunWriters()[i];
        if (writer == null) {
            FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                    GraceHashJoinOperatorDescriptor.class.getSimpleName());
            writer = new RunFileWriter(file, ctx.getIOManager());
            writer.open();
            state.getRunWriters()[i] = writer;
        }
        writer.nextFrame(head);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor0.reset(buffer);
        int tCount = accessor0.getTupleCount();
        for (int i = 0; i < tCount; ++i) {

            int entry = hpc.partition(accessor0, i, numPartitions);
            IFrame outbuf = outbufs[entry];
            appender.reset(outbuf, false);
            if (!appender.append(accessor0, i)) {
                // buffer is full, ie. we cannot fit the tuple
                // into the buffer -- write it to disk
                write(entry, outbuf.getBuffer());
                outbuf.reset();
                appender.reset(outbuf, true);
                if (!appender.append(accessor0, i)) {
                    throw new HyracksDataException("Item too big to fit in frame");
                }
            }
        }
    }

    @Override
    public void open() throws HyracksDataException {
        state = new GraceHashJoinPartitionState(ctx.getJobletContext().getJobId(), stateId);
        outbufs = new IFrame[numPartitions];
        state.setRunWriters(new RunFileWriter[numPartitions]);
        for (int i = 0; i < numPartitions; i++) {
            outbufs[i] = new VSizeFrame(ctx);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
    }
}
