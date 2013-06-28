/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.hash;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

class HashGroupBuildOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
    private final IHyracksTaskContext ctx;
    private final FrameTupleAccessor accessor;
    private final Object stateId;
    private final int[] keys;
    private final ITuplePartitionComputerFactory tpcf;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final int tableSize;
    private final RecordDescriptor inRecordDescriptor;
    private final RecordDescriptor outRecordDescriptor;

    private HashGroupState state;

    HashGroupBuildOperatorNodePushable(IHyracksTaskContext ctx, Object stateId, int[] keys,
            ITuplePartitionComputerFactory tpcf, IBinaryComparatorFactory[] comparatorFactories,
            IAggregatorDescriptorFactory aggregatorFactory, int tableSize, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor) {
        this.ctx = ctx;
        this.accessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecordDescriptor);
        this.stateId = stateId;
        this.keys = keys;
        this.tpcf = tpcf;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        this.tableSize = tableSize;
        this.inRecordDescriptor = inRecordDescriptor;
        this.outRecordDescriptor = outRecordDescriptor;
    }

    @Override
    public void open() throws HyracksDataException {
        state = new HashGroupState(ctx.getJobletContext().getJobId(), stateId);
        state.setHashTable(new GroupingHashTable(ctx, keys, comparatorFactories, tpcf, aggregatorFactory,
                inRecordDescriptor, outRecordDescriptor, tableSize));
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        GroupingHashTable table = state.getHashTable();
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            try {
                table.insert(accessor, i);
            } catch (Exception e) {
                System.out.println(e.toString());
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        ctx.setStateObject(state);
    }

    @Override
    public void fail() throws HyracksDataException {
        throw new HyracksDataException("HashGroupOperator is failed.");
    }
}