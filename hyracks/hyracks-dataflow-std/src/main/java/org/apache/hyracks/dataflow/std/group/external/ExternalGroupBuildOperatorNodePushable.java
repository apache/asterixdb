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
package org.apache.hyracks.dataflow.std.group.external;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.ISpillableTable;
import org.apache.hyracks.dataflow.std.group.ISpillableTableFactory;

class ExternalGroupBuildOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
    private final IHyracksTaskContext ctx;
    private final Object stateId;
    private final int[] keyFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final INormalizedKeyComputerFactory firstNormalizerFactory;
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final int framesLimit;
    private final ISpillableTableFactory spillableTableFactory;
    private final RecordDescriptor inRecordDescriptor;
    private final RecordDescriptor outRecordDescriptor;
    private final FrameTupleAccessor accessor;

    private ExternalGroupState state;

    ExternalGroupBuildOperatorNodePushable(IHyracksTaskContext ctx, Object stateId, int[] keyFields, int framesLimit,
            IBinaryComparatorFactory[] comparatorFactories, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, ISpillableTableFactory spillableTableFactory) {
        this.ctx = ctx;
        this.stateId = stateId;
        this.framesLimit = framesLimit;
        this.aggregatorFactory = aggregatorFactory;
        this.keyFields = keyFields;
        this.comparatorFactories = comparatorFactories;
        this.firstNormalizerFactory = firstNormalizerFactory;
        this.spillableTableFactory = spillableTableFactory;
        this.inRecordDescriptor = inRecordDescriptor;
        this.outRecordDescriptor = outRecordDescriptor;
        this.accessor = new FrameTupleAccessor(inRecordDescriptor);
    }

    @Override
    public void open() throws HyracksDataException {
        state = new ExternalGroupState(ctx.getJobletContext().getJobId(), stateId);
        state.setRuns(new LinkedList<RunFileReader>());
        ISpillableTable table = spillableTableFactory.buildSpillableTable(ctx, keyFields, comparatorFactories,
                firstNormalizerFactory, aggregatorFactory, inRecordDescriptor, outRecordDescriptor, framesLimit);
        table.reset();
        state.setSpillableTable(table);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        ISpillableTable gTable = state.getSpillableTable();
        for (int i = 0; i < tupleCount; i++) {
            /**
             * If the group table is too large, flush the table into
             * a run file.
             */
            if (!gTable.insert(accessor, i)) {
                flushFramesToRun();
                if (!gTable.insert(accessor, i))
                    throw new HyracksDataException("Failed to insert a new buffer into the aggregate operator!");
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        //do nothing for failures
    }

    @Override
    public void close() throws HyracksDataException {
        ISpillableTable gTable = state.getSpillableTable();
        if (gTable.getFrameCount() >= 0) {
            if (state.getRuns().size() > 0) {
                /**
                 * flush the memory into the run file.
                 */
                flushFramesToRun();
                gTable.close();
                gTable = null;
            }
        }
        ctx.setStateObject(state);
    }

    private void flushFramesToRun() throws HyracksDataException {
        FileReference runFile;
        try {
            runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                    ExternalGroupOperatorDescriptor.class.getSimpleName());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        RunFileWriter writer = new RunFileWriter(runFile, ctx.getIOManager());
        writer.open();
        ISpillableTable gTable = state.getSpillableTable();
        try {
            gTable.sortFrames();
            gTable.flushFrames(writer, true);
        } catch (Exception ex) {
            throw new HyracksDataException(ex);
        } finally {
            writer.close();
        }
        gTable.reset();
        state.getRuns().add(writer.createDeleteOnCloseReader());
    }
}