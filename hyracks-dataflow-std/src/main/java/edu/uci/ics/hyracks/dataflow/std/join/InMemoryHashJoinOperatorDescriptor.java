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
package edu.uci.ics.hyracks.dataflow.std.join;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class InMemoryHashJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final String JOINER = "joiner";

    private static final long serialVersionUID = 1L;
    private final int[] keys0;
    private final int[] keys1;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final boolean isLeftOuter;
    private final INullWriterFactory[] nullWriterFactories1;
    private final int tableSize;

    public InMemoryHashJoinOperatorDescriptor(JobSpecification spec, int[] keys0, int[] keys1,
            IBinaryHashFunctionFactory[] hashFunctionFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, int tableSize) {
        super(spec, 2, 1);
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.hashFunctionFactories = hashFunctionFactories;
        this.comparatorFactories = comparatorFactories;
        recordDescriptors[0] = recordDescriptor;
        this.isLeftOuter = false;
        this.nullWriterFactories1 = null;
        this.tableSize = tableSize;
    }

    public InMemoryHashJoinOperatorDescriptor(JobSpecification spec, int[] keys0, int[] keys1,
            IBinaryHashFunctionFactory[] hashFunctionFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, boolean isLeftOuter, INullWriterFactory[] nullWriterFactories1,
            int tableSize) {
        super(spec, 2, 1);
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.hashFunctionFactories = hashFunctionFactories;
        this.comparatorFactories = comparatorFactories;
        recordDescriptors[0] = recordDescriptor;
        this.isLeftOuter = isLeftOuter;
        this.nullWriterFactories1 = nullWriterFactories1;
        this.tableSize = tableSize;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        HashBuildActivityNode hba = new HashBuildActivityNode(new ActivityId(odId, 0));
        HashProbeActivityNode hpa = new HashProbeActivityNode(new ActivityId(odId, 1));

        builder.addActivity(hba);
        builder.addSourceEdge(1, hba, 0);

        builder.addActivity(hpa);
        builder.addSourceEdge(0, hpa, 0);

        builder.addTargetEdge(0, hpa, 0);

        builder.addBlockingEdge(hba, hpa);
    }

    private class HashBuildActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public HashBuildActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 1);
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }
            final INullWriter[] nullWriters1 = isLeftOuter ? new INullWriter[nullWriterFactories1.length] : null;
            if (isLeftOuter) {
                for (int i = 0; i < nullWriterFactories1.length; i++) {
                    nullWriters1[i] = nullWriterFactories1[i].createNullWriter();
                }
            }

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private InMemoryHashJoin joiner;

                @Override
                public void open() throws HyracksDataException {
                    ITuplePartitionComputer hpc0 = new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories)
                            .createPartitioner();
                    ITuplePartitionComputer hpc1 = new FieldHashPartitionComputerFactory(keys1, hashFunctionFactories)
                            .createPartitioner();
                    joiner = new InMemoryHashJoin(ctx, tableSize, new FrameTupleAccessor(ctx.getFrameSize(), rd0),
                            hpc0, new FrameTupleAccessor(ctx.getFrameSize(), rd1), hpc1, new FrameTuplePairComparator(
                                    keys0, keys1, comparators), isLeftOuter, nullWriters1);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    ByteBuffer copyBuffer = ctx.allocateFrame();
                    FrameUtils.copy(buffer, copyBuffer);
                    joiner.build(copyBuffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    env.set(JOINER, joiner);
                }

                @Override
                public void flush() throws HyracksDataException {
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
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private InMemoryHashJoin joiner;

                @Override
                public void open() throws HyracksDataException {
                    joiner = (InMemoryHashJoin) env.get(JOINER);
                    writer.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    joiner.join(buffer, writer);
                }

                @Override
                public void close() throws HyracksDataException {
                    joiner.closeJoin(writer);
                    writer.close();
                    env.set(JOINER, null);
                }

                @Override
                public void flush() throws HyracksDataException {
                    writer.flush();
                }
            };
            return op;
        }
    }
}