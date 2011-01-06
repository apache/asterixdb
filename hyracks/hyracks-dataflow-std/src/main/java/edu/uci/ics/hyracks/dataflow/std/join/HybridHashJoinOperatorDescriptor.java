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

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class HybridHashJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final String JOINER0 = "joiner0";
    private static final String SMALLRELATION = "RelR";
    private static final String LARGERELATION = "RelS";
    private static final String MEM_HASHTABLE = "MEMORY_HASHTABLE";
    private static final String NUM_PARTITION = "NUMBER_B_PARTITIONS"; // B
    private final int memsize;
    private static final long serialVersionUID = 1L;
    private final int inputsize0;
    private final double factor;
    private final int[] keys0;
    private final int[] keys1;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final int recordsPerFrame;

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
    public HybridHashJoinOperatorDescriptor(JobSpecification spec, int memsize, int inputsize0, int recordsPerFrame,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionFactory[] hashFunctionFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor)
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
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeTaskGraph(IActivityGraphBuilder builder) {
        BuildAndPartitionActivityNode phase1 = new BuildAndPartitionActivityNode(SMALLRELATION);
        PartitionAndJoinActivityNode phase2 = new PartitionAndJoinActivityNode(LARGERELATION);

        builder.addTask(phase1);
        builder.addSourceEdge(0, phase1, 0);

        builder.addTask(phase2);
        builder.addSourceEdge(1, phase2, 0);

        builder.addBlockingEdge(phase1, phase2);

        builder.addTargetEdge(0, phase2, 0);
    }

    private class BuildAndPartitionActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private String relationName;

        public BuildAndPartitionActivityNode(String relationName) {
            super();
            this.relationName = relationName;

        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksStageletContext ctx,
                final IOperatorEnvironment env, IRecordDescriptorProvider recordDescProvider, int partition,
                final int nPartitions) {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 1);
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private InMemoryHashJoin joiner0;
                private final FrameTupleAccessor accessor0 = new FrameTupleAccessor(ctx.getFrameSize(), rd0);
                ITuplePartitionComputer hpc0 = new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories)
                        .createPartitioner();
                private final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                private final FrameTupleAppender ftappender = new FrameTupleAppender(ctx.getFrameSize());
                private ByteBuffer[] bufferForPartitions;
                private final ByteBuffer inBuffer = ctx.allocateFrame();
                private RunFileWriter[] fWriters;
                private int memoryForHashtable;
                private int B;

                @Override
                public void close() throws HyracksDataException {
                    if (memoryForHashtable != 0)
                        build(inBuffer);

                    for (int i = 0; i < B; i++) {
                        ByteBuffer buf = bufferForPartitions[i];
                        accessor0.reset(buf);
                        if (accessor0.getTupleCount() > 0) {
                            write(i, buf);
                        }
                        closeWriter(i);
                    }

                    env.set(relationName, fWriters);
                    env.set(JOINER0, joiner0);
                    env.set(NUM_PARTITION, B);
                    env.set(MEM_HASHTABLE, memoryForHashtable);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {

                    if (memoryForHashtable != memsize - 2) {
                        accessor0.reset(buffer);
                        int tCount = accessor0.getTupleCount();
                        for (int i = 0; i < tCount; ++i) {
                            int entry = -1;
                            if (memoryForHashtable == 0) {
                                entry = hpc0.partition(accessor0, i, B);
                                boolean newBuffer = false;
                                ByteBuffer bufBi = bufferForPartitions[entry];
                                while (true) {
                                    appender.reset(bufBi, newBuffer);
                                    if (appender.append(accessor0, i)) {
                                        break;
                                    } else {
                                        write(entry, bufBi);
                                        bufBi.clear();
                                        newBuffer = true;
                                    }
                                }
                            } else {
                                entry = hpc0.partition(accessor0, i, (int) (inputsize0 * factor / nPartitions));
                                if (entry < memoryForHashtable) {
                                    while (true) {
                                        if (!ftappender.append(accessor0, i)) {
                                            build(inBuffer);

                                            ftappender.reset(inBuffer, true);
                                        } else
                                            break;
                                    }
                                } else {
                                    entry %= B;
                                    boolean newBuffer = false;
                                    ByteBuffer bufBi = bufferForPartitions[entry];
                                    while (true) {
                                        appender.reset(bufBi, newBuffer);
                                        if (appender.append(accessor0, i)) {
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
                    joiner0.build(copyBuffer);
                }

                @Override
                public void open() throws HyracksDataException {
                    if (memsize > 1) {
                        if (memsize > inputsize0) {
                            B = 0;
                        } else {
                            B = (int) (Math.ceil((double) (inputsize0 * factor / nPartitions - memsize)
                                    / (double) (memsize - 1)));
                        }
                        if (B <= 0) {
                            // becomes in-memory HJ
                            memoryForHashtable = memsize - 2;
                            B = 0;
                        } else {
                            memoryForHashtable = memsize - B - 2;
                            if (memoryForHashtable < 0) {
                                memoryForHashtable = 0;
                                B = (int) Math.ceil(Math.sqrt(inputsize0 * factor / nPartitions));
                            }
                        }
                    } else {
                        throw new HyracksDataException("not enough memory");
                    }

                    ITuplePartitionComputer hpc0 = new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories)
                            .createPartitioner();
                    ITuplePartitionComputer hpc1 = new FieldHashPartitionComputerFactory(keys1, hashFunctionFactories)
                            .createPartitioner();
                    int tableSize = (int) (memoryForHashtable * recordsPerFrame * factor);
                    joiner0 = new InMemoryHashJoin(ctx, tableSize, new FrameTupleAccessor(ctx.getFrameSize(), rd0),
                            hpc0, new FrameTupleAccessor(ctx.getFrameSize(), rd1), hpc1, new FrameTuplePairComparator(
                                    keys0, keys1, comparators));
                    bufferForPartitions = new ByteBuffer[B];
                    fWriters = new RunFileWriter[B];
                    for (int i = 0; i < B; i++) {
                        bufferForPartitions[i] = ctx.allocateFrame();
                    }

                    ftappender.reset(inBuffer, true);
                }

                @Override
                public void flush() throws HyracksDataException {
                }

                private void closeWriter(int i) throws HyracksDataException {
                    RunFileWriter writer = fWriters[i];
                    if (writer != null) {
                        writer.close();
                    }
                }

                private void write(int i, ByteBuffer head) throws HyracksDataException {
                    RunFileWriter writer = fWriters[i];
                    if (writer == null) {
                        FileReference file = ctx.getJobletContext().createWorkspaceFile(relationName);
                        writer = new RunFileWriter(file, ctx.getIOManager());
                        writer.open();
                        fWriters[i] = writer;
                    }
                    writer.nextFrame(head);
                }
            };
            return op;
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return HybridHashJoinOperatorDescriptor.this;
        }
    }

    private class PartitionAndJoinActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private String largeRelation;

        public PartitionAndJoinActivityNode(String relationName) {
            super();
            this.largeRelation = relationName;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksStageletContext ctx,
                final IOperatorEnvironment env, IRecordDescriptorProvider recordDescProvider, int partition,
                final int nPartitions) {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 1);
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }

            IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private InMemoryHashJoin joiner0;
                private final FrameTupleAccessor accessor1 = new FrameTupleAccessor(ctx.getFrameSize(), rd1);
                private ITuplePartitionComputerFactory hpcf0 = new FieldHashPartitionComputerFactory(keys0,
                        hashFunctionFactories);
                private ITuplePartitionComputerFactory hpcf1 = new FieldHashPartitionComputerFactory(keys1,
                        hashFunctionFactories);
                ITuplePartitionComputer hpc1 = hpcf1.createPartitioner();

                private final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                private final FrameTupleAppender ftap = new FrameTupleAppender(ctx.getFrameSize());
                private final ByteBuffer inBuffer = ctx.allocateFrame();
                private final ByteBuffer outBuffer = ctx.allocateFrame();
                private RunFileWriter[] rWriters;
                private RunFileWriter[] sWriters;
                private ByteBuffer[] bufferForPartitions;
                private int B;
                private int memoryForHashtable;

                @Override
                public void open() throws HyracksDataException {
                    joiner0 = (InMemoryHashJoin) env.get(JOINER0);
                    writer.open();
                    rWriters = (RunFileWriter[]) env.get(SMALLRELATION);
                    B = (Integer) env.get(NUM_PARTITION);
                    memoryForHashtable = (Integer) env.get(MEM_HASHTABLE);
                    sWriters = new RunFileWriter[B];
                    bufferForPartitions = new ByteBuffer[B];
                    for (int i = 0; i < B; i++) {
                        bufferForPartitions[i] = ctx.allocateFrame();
                    }
                    appender.reset(outBuffer, true);
                    ftap.reset(inBuffer, true);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (memoryForHashtable != memsize - 2) {
                        accessor1.reset(buffer);
                        int tupleCount1 = accessor1.getTupleCount();
                        for (int i = 0; i < tupleCount1; ++i) {

                            int entry = -1;
                            if (memoryForHashtable == 0) {
                                entry = hpc1.partition(accessor1, i, B);
                                boolean newBuffer = false;
                                ByteBuffer outbuf = bufferForPartitions[entry];
                                while (true) {
                                    appender.reset(outbuf, newBuffer);
                                    if (appender.append(accessor1, i)) {
                                        break;
                                    } else {
                                        write(entry, outbuf);
                                        outbuf.clear();
                                        newBuffer = true;
                                    }
                                }
                            } else {
                                entry = hpc1.partition(accessor1, i, (int) (inputsize0 * factor / nPartitions));
                                if (entry < memoryForHashtable) {
                                    while (true) {
                                        if (!ftap.append(accessor1, i)) {
                                            joiner0.join(inBuffer, writer);
                                            ftap.reset(inBuffer, true);
                                        } else
                                            break;
                                    }

                                } else {
                                    entry %= B;
                                    boolean newBuffer = false;
                                    ByteBuffer outbuf = bufferForPartitions[entry];
                                    while (true) {
                                        appender.reset(outbuf, newBuffer);
                                        if (appender.append(accessor1, i)) {
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
                        joiner0.join(buffer, writer);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    joiner0.join(inBuffer, writer);
                    joiner0.closeJoin(writer);
                    ITuplePartitionComputer hpcRep0 = new RepartitionComputerFactory(B, hpcf0).createPartitioner();
                    ITuplePartitionComputer hpcRep1 = new RepartitionComputerFactory(B, hpcf1).createPartitioner();
                    if (memoryForHashtable != memsize - 2) {
                        for (int i = 0; i < B; i++) {
                            ByteBuffer buf = bufferForPartitions[i];
                            accessor1.reset(buf);
                            if (accessor1.getTupleCount() > 0) {
                                write(i, buf);
                            }
                            closeWriter(i);
                        }

                        inBuffer.clear();
                        int tableSize = -1;
                        if (memoryForHashtable == 0) {
                            tableSize = (int) (B * recordsPerFrame * factor);
                        } else {
                            tableSize = (int) (memsize * recordsPerFrame * factor);
                        }
                        for (int partitionid = 0; partitionid < B; partitionid++) {
                            RunFileWriter rWriter = rWriters[partitionid];
                            RunFileWriter sWriter = sWriters[partitionid];
                            if (rWriter == null || sWriter == null) {
                                continue;
                            }

                            InMemoryHashJoin joiner = new InMemoryHashJoin(ctx, tableSize, new FrameTupleAccessor(
                                    ctx.getFrameSize(), rd0), hpcRep0, new FrameTupleAccessor(ctx.getFrameSize(), rd1),
                                    hpcRep1, new FrameTuplePairComparator(keys0, keys1, comparators));

                            RunFileReader rReader = rWriter.createReader();
                            rReader.open();
                            while (rReader.nextFrame(inBuffer)) {
                                ByteBuffer copyBuffer = ctx.allocateFrame();
                                FrameUtils.copy(inBuffer, copyBuffer);
                                joiner.build(copyBuffer);
                                inBuffer.clear();
                            }
                            rReader.close();

                            // probe
                            RunFileReader sReader = sWriter.createReader();
                            sReader.open();
                            while (sReader.nextFrame(inBuffer)) {
                                joiner.join(inBuffer, writer);
                                inBuffer.clear();
                            }
                            sReader.close();
                            joiner.closeJoin(writer);
                        }
                    }
                    writer.close();
                    env.set(LARGERELATION, null);
                    env.set(SMALLRELATION, null);
                    env.set(JOINER0, null);
                    env.set(MEM_HASHTABLE, null);
                    env.set(NUM_PARTITION, null);
                }

                @Override
                public void flush() throws HyracksDataException {
                    writer.flush();
                }

                private void closeWriter(int i) throws HyracksDataException {
                    RunFileWriter writer = sWriters[i];
                    if (writer != null) {
                        writer.close();
                    }
                }

                private void write(int i, ByteBuffer head) throws HyracksDataException {
                    RunFileWriter writer = sWriters[i];
                    if (writer == null) {
                        FileReference file = ctx.createWorkspaceFile(largeRelation);
                        writer = new RunFileWriter(file, ctx.getIOManager());
                        writer.open();
                        sWriters[i] = writer;
                    }
                    writer.nextFrame(head);
                }
            };
            return op;
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return HybridHashJoinOperatorDescriptor.this;
        }
    }
}