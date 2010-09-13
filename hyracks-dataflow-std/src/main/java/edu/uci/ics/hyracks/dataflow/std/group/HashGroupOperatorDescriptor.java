/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class HashGroupOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final String HASHTABLE = "hashtable";

    private static final long serialVersionUID = 1L;

    private final int[] keys;
    private final ITuplePartitionComputerFactory tpcf;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAccumulatingAggregatorFactory aggregatorFactory;
    private final int tableSize;

    public HashGroupOperatorDescriptor(JobSpecification spec, int[] keys, ITuplePartitionComputerFactory tpcf,
            IBinaryComparatorFactory[] comparatorFactories, IAccumulatingAggregatorFactory aggregatorFactory,
            RecordDescriptor recordDescriptor, int tableSize) {
        super(spec, 1, 1);
        this.keys = keys;
        this.tpcf = tpcf;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        recordDescriptors[0] = recordDescriptor;
        this.tableSize = tableSize;
    }

    @Override
    public void contributeTaskGraph(IActivityGraphBuilder builder) {
        HashBuildActivity ha = new HashBuildActivity();
        builder.addTask(ha);

        OutputActivity oa = new OutputActivity();
        builder.addTask(oa);

        builder.addSourceEdge(0, ha, 0);
        builder.addTargetEdge(0, oa, 0);
        builder.addBlockingEdge(ha, oa);
    }

    private class HashBuildActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksContext ctx, final IOperatorEnvironment env,
                final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final FrameTupleAccessor accessor = new FrameTupleAccessor(ctx,
                    recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0));
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private GroupingHashTable table;

                @Override
                public void open() throws HyracksDataException {
                    table = new GroupingHashTable(ctx, keys, comparatorFactories, tpcf, aggregatorFactory,
                            recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0), recordDescriptors[0],
                            tableSize);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor.reset(buffer);
                    int tupleCount = accessor.getTupleCount();
                    for (int i = 0; i < tupleCount; ++i) {
                        table.insert(accessor, i);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    env.set(HASHTABLE, table);
                }

                @Override
                public void flush() throws HyracksDataException {
                }
            };
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return HashGroupOperatorDescriptor.this;
        }
    }

    private class OutputActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    GroupingHashTable table = (GroupingHashTable) env.get(HASHTABLE);
                    writer.open();
                    table.write(writer);
                    writer.close();
                    env.set(HASHTABLE, null);
                }
            };
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return HashGroupOperatorDescriptor.this;
        }
    }
}