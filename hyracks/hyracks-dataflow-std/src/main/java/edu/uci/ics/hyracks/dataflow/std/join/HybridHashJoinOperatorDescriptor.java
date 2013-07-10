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
package edu.uci.ics.hyracks.dataflow.std.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluator;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.RepartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.structures.ISerializableTable;
import edu.uci.ics.hyracks.dataflow.std.structures.SerializableHashTable;

public class HybridHashJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int BUILD_AND_PARTITION_ACTIVITY_ID = 0;
    private static final int PARTITION_AND_JOIN_ACTIVITY_ID = 1;

    private final int memsize;
    private static final long serialVersionUID = 1L;
    private final int inputsize0;
    private final double factor;
    private final int recordsPerFrame;
    private final int[] keys0;
    private final int[] keys1;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IPredicateEvaluatorFactory predEvaluatorFactory;
    private final boolean isLeftOuter;
    private final INullWriterFactory[] nullWriterFactories1;

    /**
     * @param spec
     * @param memsize
     *            in frames
     * @param inputsize0
     *            in frames
     * @param recordsPerFrame
     * @param factor
     * @param keys0
     * @param keys1
     * @param hashFunctionFactories
     * @param comparatorFactories
     * @param recordDescriptor
     * @throws HyracksDataException
     */
    public HybridHashJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memsize, int inputsize0,
            int recordsPerFrame, double factor, int[] keys0, int[] keys1,
            IBinaryHashFunctionFactory[] hashFunctionFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, IPredicateEvaluatorFactory predEvalFactory) throws HyracksDataException {
        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.factor = factor;
        this.recordsPerFrame = recordsPerFrame;
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.hashFunctionFactories = hashFunctionFactories;
        this.comparatorFactories = comparatorFactories;
        this.predEvaluatorFactory = predEvalFactory;
        this.isLeftOuter = false;
        this.nullWriterFactories1 = null;
        recordDescriptors[0] = recordDescriptor;
    }

    public HybridHashJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memsize, int inputsize0,
            int recordsPerFrame, double factor, int[] keys0, int[] keys1,
            IBinaryHashFunctionFactory[] hashFunctionFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, IPredicateEvaluatorFactory predEvalFactory, boolean isLeftOuter, INullWriterFactory[] nullWriterFactories1)
            throws HyracksDataException {
        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.factor = factor;
        this.recordsPerFrame = recordsPerFrame;
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.hashFunctionFactories = hashFunctionFactories;
        this.comparatorFactories = comparatorFactories;
        this.predEvaluatorFactory = predEvalFactory;
        this.isLeftOuter = isLeftOuter;
        this.nullWriterFactories1 = nullWriterFactories1;
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId p1Aid = new ActivityId(odId, BUILD_AND_PARTITION_ACTIVITY_ID);
        ActivityId p2Aid = new ActivityId(odId, PARTITION_AND_JOIN_ACTIVITY_ID);
        BuildAndPartitionActivityNode phase1 = new BuildAndPartitionActivityNode(p1Aid, p2Aid);
        PartitionAndJoinActivityNode phase2 = new PartitionAndJoinActivityNode(p2Aid, p1Aid);

        builder.addActivity(this, phase1);
        builder.addSourceEdge(1, phase1, 0);

        builder.addActivity(this, phase2);
        builder.addSourceEdge(0, phase2, 0);

        builder.addBlockingEdge(phase1, phase2);

        builder.addTargetEdge(0, phase2, 0);
    }

    public static class BuildAndPartitionTaskState extends AbstractStateObject {
        private RunFileWriter[] fWriters;
        private InMemoryHashJoin joiner;
        private int nPartitions;
        private int memoryForHashtable;

        public BuildAndPartitionTaskState() {
        }

        private BuildAndPartitionTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }

    }

    private class BuildAndPartitionActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId joinAid;

        public BuildAndPartitionActivityNode(ActivityId id, ActivityId joinAid) {
            super(id);
            this.joinAid = joinAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) throws HyracksDataException {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(joinAid, 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
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
            final IPredicateEvaluator predEvaluator = ( predEvaluatorFactory == null ? null : predEvaluatorFactory.createPredicateEvaluator());

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private BuildAndPartitionTaskState state = new BuildAndPartitionTaskState(ctx.getJobletContext()
                        .getJobId(), new TaskId(getActivityId(), partition));
                private final FrameTupleAccessor accessorBuild = new FrameTupleAccessor(ctx.getFrameSize(), rd1);
                private final ITuplePartitionComputer hpcBuild = new FieldHashPartitionComputerFactory(keys1,
                        hashFunctionFactories).createPartitioner();
                private final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                private final FrameTupleAppender ftappender = new FrameTupleAppender(ctx.getFrameSize());
                private ByteBuffer[] bufferForPartitions;
                private final ByteBuffer inBuffer = ctx.allocateFrame();

                @Override
                public void close() throws HyracksDataException {
                    if (state.memoryForHashtable != 0)
                        build(inBuffer);

                    for (int i = 0; i < state.nPartitions; i++) {
                        ByteBuffer buf = bufferForPartitions[i];
                        accessorBuild.reset(buf);
                        if (accessorBuild.getTupleCount() > 0) {
                            write(i, buf);
                        }
                        closeWriter(i);
                    }

                    ctx.setStateObject(state);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {

                    if (state.memoryForHashtable != memsize - 2) {
                        accessorBuild.reset(buffer);
                        int tCount = accessorBuild.getTupleCount();
                        for (int i = 0; i < tCount; ++i) {
                            int entry = -1;
                            if (state.memoryForHashtable == 0) {
                                entry = hpcBuild.partition(accessorBuild, i, state.nPartitions);
                                boolean newBuffer = false;
                                ByteBuffer bufBi = bufferForPartitions[entry];
                                while (true) {
                                    appender.reset(bufBi, newBuffer);
                                    if (appender.append(accessorBuild, i)) {
                                        break;
                                    } else {
                                        write(entry, bufBi);
                                        bufBi.clear();
                                        newBuffer = true;
                                    }
                                }
                            } else {
                                entry = hpcBuild.partition(accessorBuild, i, (int) (inputsize0 * factor / nPartitions));
                                if (entry < state.memoryForHashtable) {
                                    while (true) {
                                        if (!ftappender.append(accessorBuild, i)) {
                                            build(inBuffer);

                                            ftappender.reset(inBuffer, true);
                                        } else {
                                            break;
                                        }
                                    }
                                } else {
                                    entry %= state.nPartitions;
                                    boolean newBuffer = false;
                                    ByteBuffer bufBi = bufferForPartitions[entry];
                                    while (true) {
                                        appender.reset(bufBi, newBuffer);
                                        if (appender.append(accessorBuild, i)) {
                                            break;
                                        } else {
                                            write(entry, bufBi);
                                            bufBi.clear();
                                            newBuffer = true;
                                        }
                                    }
                                }
                            }

                        }
                    } else {
                        build(buffer);
                    }

                }

                private void build(ByteBuffer inBuffer) throws HyracksDataException {
                    ByteBuffer copyBuffer = ctx.allocateFrame();
                    FrameUtils.copy(inBuffer, copyBuffer);
                    state.joiner.build(copyBuffer);
                }

                @Override
                public void open() throws HyracksDataException {
                    if (memsize > 1) {
                        if (memsize > inputsize0) {
                            state.nPartitions = 0;
                        } else {
                            state.nPartitions = (int) (Math.ceil((double) (inputsize0 * factor / nPartitions - memsize)
                                    / (double) (memsize - 1)));
                        }
                        if (state.nPartitions <= 0) {
                            // becomes in-memory HJ
                            state.memoryForHashtable = memsize - 2;
                            state.nPartitions = 0;
                        } else {
                            state.memoryForHashtable = memsize - state.nPartitions - 2;
                            if (state.memoryForHashtable < 0) {
                                state.memoryForHashtable = 0;
                                state.nPartitions = (int) Math.ceil(Math.sqrt(inputsize0 * factor / nPartitions));
                            }
                        }
                    } else {
                        throw new HyracksDataException("not enough memory");
                    }

                    ITuplePartitionComputer hpc0 = new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories)
                            .createPartitioner();
                    ITuplePartitionComputer hpc1 = new FieldHashPartitionComputerFactory(keys1, hashFunctionFactories)
                            .createPartitioner();
                    int tableSize = (int) (state.memoryForHashtable * recordsPerFrame * factor);
                    ISerializableTable table = new SerializableHashTable(tableSize, ctx);
                    state.joiner = new InMemoryHashJoin(ctx, tableSize,
                            new FrameTupleAccessor(ctx.getFrameSize(), rd0), hpc0, new FrameTupleAccessor(
                                    ctx.getFrameSize(), rd1), hpc1, new FrameTuplePairComparator(keys0, keys1,
                                    comparators), isLeftOuter, nullWriters1, table, predEvaluator);
                    bufferForPartitions = new ByteBuffer[state.nPartitions];
                    state.fWriters = new RunFileWriter[state.nPartitions];
                    for (int i = 0; i < state.nPartitions; i++) {
                        bufferForPartitions[i] = ctx.allocateFrame();
                    }

                    ftappender.reset(inBuffer, true);
                }

                @Override
                public void fail() throws HyracksDataException {
                }

                private void closeWriter(int i) throws HyracksDataException {
                    RunFileWriter writer = state.fWriters[i];
                    if (writer != null) {
                        writer.close();
                    }
                }

                private void write(int i, ByteBuffer head) throws HyracksDataException {
                    RunFileWriter writer = state.fWriters[i];
                    if (writer == null) {
                        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                                BuildAndPartitionActivityNode.class.getSimpleName());
                        writer = new RunFileWriter(file, ctx.getIOManager());
                        writer.open();
                        state.fWriters[i] = writer;
                    }
                    writer.nextFrame(head);
                }
            };
            return op;
        }
    }

    private class PartitionAndJoinActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId buildAid;

        public PartitionAndJoinActivityNode(ActivityId id, ActivityId buildAid) {
            super(id);
            this.buildAid = buildAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
                throws HyracksDataException {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(buildAid, 0);
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
            final IPredicateEvaluator predEvaluator = ( predEvaluatorFactory == null ? null : predEvaluatorFactory.createPredicateEvaluator());

            IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private BuildAndPartitionTaskState state;
                private final FrameTupleAccessor accessorProbe = new FrameTupleAccessor(ctx.getFrameSize(), rd0);
                private final ITuplePartitionComputerFactory hpcf0 = new FieldHashPartitionComputerFactory(keys0,
                        hashFunctionFactories);
                private final ITuplePartitionComputerFactory hpcf1 = new FieldHashPartitionComputerFactory(keys1,
                        hashFunctionFactories);
                private final ITuplePartitionComputer hpcProbe = hpcf0.createPartitioner();

                private final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                private final FrameTupleAppender ftap = new FrameTupleAppender(ctx.getFrameSize());
                private final ByteBuffer inBuffer = ctx.allocateFrame();
                private final ByteBuffer outBuffer = ctx.allocateFrame();
                private RunFileWriter[] buildWriters;
                private RunFileWriter[] probeWriters;
                private ByteBuffer[] bufferForPartitions;

                @Override
                public void open() throws HyracksDataException {
                    state = (BuildAndPartitionTaskState) ctx.getStateObject(new TaskId(new ActivityId(getOperatorId(),
                            BUILD_AND_PARTITION_ACTIVITY_ID), partition));
                    writer.open();
                    buildWriters = state.fWriters;
                    probeWriters = new RunFileWriter[state.nPartitions];
                    bufferForPartitions = new ByteBuffer[state.nPartitions];
                    for (int i = 0; i < state.nPartitions; i++) {
                        bufferForPartitions[i] = ctx.allocateFrame();
                    }
                    appender.reset(outBuffer, true);
                    ftap.reset(inBuffer, true);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (state.memoryForHashtable != memsize - 2) {
                        accessorProbe.reset(buffer);
                        int tupleCount0 = accessorProbe.getTupleCount();
                        for (int i = 0; i < tupleCount0; ++i) {

                            int entry = -1;
                            if (state.memoryForHashtable == 0) {
                                entry = hpcProbe.partition(accessorProbe, i, state.nPartitions);
                                boolean newBuffer = false;
                                ByteBuffer outbuf = bufferForPartitions[entry];
                                while (true) {
                                    appender.reset(outbuf, newBuffer);
                                    if (appender.append(accessorProbe, i)) {
                                        break;
                                    } else {
                                        write(entry, outbuf);
                                        outbuf.clear();
                                        newBuffer = true;
                                    }
                                }
                            } else {
                                entry = hpcProbe.partition(accessorProbe, i, (int) (inputsize0 * factor / nPartitions));
                                if (entry < state.memoryForHashtable) {
                                    while (true) {
                                        if (!ftap.append(accessorProbe, i)) {
                                            state.joiner.join(inBuffer, writer);
                                            ftap.reset(inBuffer, true);
                                        } else
                                            break;
                                    }

                                } else {
                                    entry %= state.nPartitions;
                                    boolean newBuffer = false;
                                    ByteBuffer outbuf = bufferForPartitions[entry];
                                    while (true) {
                                        appender.reset(outbuf, newBuffer);
                                        if (appender.append(accessorProbe, i)) {
                                            break;
                                        } else {
                                            write(entry, outbuf);
                                            outbuf.clear();
                                            newBuffer = true;
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        state.joiner.join(buffer, writer);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    state.joiner.join(inBuffer, writer);
                    state.joiner.closeJoin(writer);
                    ITuplePartitionComputer hpcRep0 = new RepartitionComputerFactory(state.nPartitions, hpcf0)
                            .createPartitioner();
                    ITuplePartitionComputer hpcRep1 = new RepartitionComputerFactory(state.nPartitions, hpcf1)
                            .createPartitioner();
                    if (state.memoryForHashtable != memsize - 2) {
                        for (int i = 0; i < state.nPartitions; i++) {
                            ByteBuffer buf = bufferForPartitions[i];
                            accessorProbe.reset(buf);
                            if (accessorProbe.getTupleCount() > 0) {
                                write(i, buf);
                            }
                            closeWriter(i);
                        }

                        inBuffer.clear();
                        int tableSize = -1;
                        if (state.memoryForHashtable == 0) {
                            tableSize = (int) (state.nPartitions * recordsPerFrame * factor);
                        } else {
                            tableSize = (int) (memsize * recordsPerFrame * factor);
                        }
                        for (int partitionid = 0; partitionid < state.nPartitions; partitionid++) {
                            RunFileWriter buildWriter = buildWriters[partitionid];
                            RunFileWriter probeWriter = probeWriters[partitionid];
                            if ((buildWriter == null && !isLeftOuter) || probeWriter == null) {
                                continue;
                            }
                            ISerializableTable table = new SerializableHashTable(tableSize, ctx);
                            InMemoryHashJoin joiner = new InMemoryHashJoin(ctx, tableSize, new FrameTupleAccessor(
                                    ctx.getFrameSize(), rd0), hpcRep0, new FrameTupleAccessor(ctx.getFrameSize(), rd1),
                                    hpcRep1, new FrameTuplePairComparator(keys0, keys1, comparators), isLeftOuter,
                                    nullWriters1, table, predEvaluator);

                            if (buildWriter != null) {
                                RunFileReader buildReader = buildWriter.createReader();
                                buildReader.open();
                                while (buildReader.nextFrame(inBuffer)) {
                                    ByteBuffer copyBuffer = ctx.allocateFrame();
                                    FrameUtils.copy(inBuffer, copyBuffer);
                                    joiner.build(copyBuffer);
                                    inBuffer.clear();
                                }
                                buildReader.close();
                            }

                            // probe
                            RunFileReader probeReader = probeWriter.createReader();
                            probeReader.open();
                            while (probeReader.nextFrame(inBuffer)) {
                                joiner.join(inBuffer, writer);
                                inBuffer.clear();
                            }
                            probeReader.close();
                            joiner.closeJoin(writer);
                        }
                    }
                    writer.close();
                }

                private void closeWriter(int i) throws HyracksDataException {
                    RunFileWriter writer = probeWriters[i];
                    if (writer != null) {
                        writer.close();
                    }
                }

                private void write(int i, ByteBuffer head) throws HyracksDataException {
                    RunFileWriter writer = probeWriters[i];
                    if (writer == null) {
                        FileReference file = ctx.createManagedWorkspaceFile(PartitionAndJoinActivityNode.class
                                .getSimpleName());
                        writer = new RunFileWriter(file, ctx.getIOManager());
                        writer.open();
                        probeWriters[i] = writer;
                    }
                    writer.nextFrame(head);
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