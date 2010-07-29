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
package edu.uci.ics.hyracks.coreops.join;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePullable;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.comm.util.FrameUtils;
import edu.uci.ics.hyracks.context.HyracksContext;
import edu.uci.ics.hyracks.coreops.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.coreops.RepartitionComputerFactory;
import edu.uci.ics.hyracks.coreops.base.AbstractActivityNode;
import edu.uci.ics.hyracks.coreops.base.AbstractOperatorDescriptor;

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

    private int numReadI1 = 0;
    private int numWriteI1 = 0;
    private int numReadI2 = 0;
    private int numWriteI2 = 0;

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
        public IOperatorNodePullable createPullRuntime(HyracksContext ctx, JobPlan plan, IOperatorEnvironment env,
                int partition, int nPartitions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final HyracksContext ctx, JobPlan plan,
                final IOperatorEnvironment env, int partition, final int nPartitions) {
            final RecordDescriptor rd0 = plan.getJobSpecification()
                    .getOperatorInputRecordDescriptor(getOperatorId(), 0);
            final RecordDescriptor rd1 = plan.getJobSpecification()
                    .getOperatorInputRecordDescriptor(getOperatorId(), 1);
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }

            IOperatorNodePushable op = new IOperatorNodePushable() {
                private InMemoryHashJoin joiner0;
                private final FrameTupleAccessor accessor0 = new FrameTupleAccessor(ctx, rd0);
                ITuplePartitionComputer hpc0 = new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories)
                        .createPartitioner();
                private final FrameTupleAppender appender = new FrameTupleAppender(ctx);
                private final FrameTupleAppender ftappender = new FrameTupleAppender(ctx);
                private ByteBuffer[] bufferForPartitions;
                private final ByteBuffer inBuffer = ctx.getResourceManager().allocateFrame();
                private File[] files;
                private FileChannel[] channels;
                private int memoryForHashtable;
                private int B;

                @Override
                public void setFrameWriter(int index, IFrameWriter writer) {
                    throw new IllegalArgumentException();
                }

                @Override
                public void close() throws HyracksDataException {
                    if (memoryForHashtable != 0)
                        build(inBuffer);

                    for (int i = 0; i < B; i++) {
                        try {
                            ByteBuffer buf = bufferForPartitions[i];
                            accessor0.reset(buf);
                            if (accessor0.getTupleCount() > 0) {
                                FileChannel wChannel = channels[i];
                                if (wChannel == null) {
                                    wChannel = new RandomAccessFile(files[i], "rw").getChannel();
                                    channels[i] = wChannel;
                                }
                                wChannel.write(buf);
                                numWriteI1++;
                            }
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                    }

                    env.set(relationName, channels);
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
                                        try {
                                            FileChannel wChannel = channels[entry];
                                            if (wChannel == null) {
                                                wChannel = new RandomAccessFile(files[entry], "rw").getChannel();
                                                channels[entry] = wChannel;
                                            }
                                            wChannel.write(bufBi);
                                            numWriteI1++;
                                            bufBi.clear();
                                            newBuffer = true;
                                        } catch (IOException e) {
                                            throw new HyracksDataException(e);
                                        }
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
                                            try {
                                                FileChannel wChannel;
                                                if (channels[entry] == null) {
                                                    wChannel = new RandomAccessFile(files[entry], "rw").getChannel();
                                                    channels[entry] = wChannel;
                                                } else {
                                                    wChannel = channels[entry];
                                                }
                                                wChannel.write(bufBi);
                                                numWriteI1++;
                                                bufBi.clear();
                                                newBuffer = true;
                                            } catch (IOException e) {
                                                throw new HyracksDataException(e);
                                            }
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
                    ByteBuffer copyBuffer = ctx.getResourceManager().allocateFrame();
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
                    joiner0 = new InMemoryHashJoin(ctx, tableSize, new FrameTupleAccessor(ctx, rd0), hpc0,
                            new FrameTupleAccessor(ctx, rd1), hpc1, new FrameTuplePairComparator(keys0, keys1,
                                    comparators));
                    files = new File[B];
                    channels = new FileChannel[B];
                    bufferForPartitions = new ByteBuffer[B];
                    for (int i = 0; i < B; i++) {
                        try {
                            files[i] = ctx.getResourceManager().createFile(relationName, null);
                            files[i].deleteOnExit();
                            bufferForPartitions[i] = ctx.getResourceManager().allocateFrame();
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                    }

                    ftappender.reset(inBuffer, true);
                }

            };
            return op;
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return HybridHashJoinOperatorDescriptor.this;
        }

        @Override
        public boolean supportsPullInterface() {
            return false;
        }

        @Override
        public boolean supportsPushInterface() {
            return true;
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
        public IOperatorNodePullable createPullRuntime(HyracksContext ctx, JobPlan plan, IOperatorEnvironment env,
                int partition, int nPartitions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final HyracksContext ctx, JobPlan plan,
                final IOperatorEnvironment env, int partition, final int nPartitions) {
            final RecordDescriptor rd0 = plan.getJobSpecification()
                    .getOperatorInputRecordDescriptor(getOperatorId(), 0);
            final RecordDescriptor rd1 = plan.getJobSpecification()
                    .getOperatorInputRecordDescriptor(getOperatorId(), 1);
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }

            IOperatorNodePushable op = new IOperatorNodePushable() {
                private InMemoryHashJoin joiner0;
                private final FrameTupleAccessor accessor1 = new FrameTupleAccessor(ctx, rd1);
                private ITuplePartitionComputerFactory hpcf0 = new FieldHashPartitionComputerFactory(keys0,
                        hashFunctionFactories);
                private ITuplePartitionComputerFactory hpcf1 = new FieldHashPartitionComputerFactory(keys1,
                        hashFunctionFactories);
                ITuplePartitionComputer hpc1 = hpcf1.createPartitioner();

                private final FrameTupleAppender appender = new FrameTupleAppender(ctx);
                private final FrameTupleAppender ftap = new FrameTupleAppender(ctx);
                private final ByteBuffer inBuffer = ctx.getResourceManager().allocateFrame();
                private final ByteBuffer outBuffer = ctx.getResourceManager().allocateFrame();
                private IFrameWriter writer;
                private FileChannel[] channelsR;
                private FileChannel[] channelsS;
                private File filesS[];
                private ByteBuffer[] bufferForPartitions;
                private int B;
                private int memoryForHashtable;

                @Override
                public void open() throws HyracksDataException {
                    joiner0 = (InMemoryHashJoin) env.get(JOINER0);
                    writer.open();
                    channelsR = (FileChannel[]) env.get(SMALLRELATION);
                    B = (Integer) env.get(NUM_PARTITION);
                    memoryForHashtable = (Integer) env.get(MEM_HASHTABLE);
                    filesS = new File[B];
                    channelsS = new FileChannel[B];
                    bufferForPartitions = new ByteBuffer[B];
                    for (int i = 0; i < B; i++) {
                        try {
                            filesS[i] = ctx.getResourceManager().createFile(largeRelation, null);
                            filesS[i].deleteOnExit();
                            bufferForPartitions[i] = ctx.getResourceManager().allocateFrame();
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
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
                                        try {
                                            FileChannel wChannel = channelsS[entry];
                                            if (wChannel == null) {
                                                wChannel = new RandomAccessFile(filesS[entry], "rw").getChannel();
                                                channelsS[entry] = wChannel;
                                            }

                                            wChannel.write(outbuf);
                                            numWriteI2++;
                                            outbuf.clear();
                                            newBuffer = true;
                                        } catch (IOException e) {
                                            throw new HyracksDataException(e);
                                        }
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
                                            try {
                                                FileChannel wChannel = channelsS[entry];
                                                if (wChannel == null) {
                                                    wChannel = new RandomAccessFile(filesS[entry], "rw").getChannel();
                                                    channelsS[entry] = wChannel;
                                                    wChannel = channelsS[entry];
                                                }
                                                wChannel.write(outbuf);
                                                numWriteI2++;
                                                outbuf.clear();
                                                newBuffer = true;
                                            } catch (IOException e) {
                                                throw new HyracksDataException(e);
                                            }
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
                        int[] memRi = new int[B];
                        for (int i = 0; i < B; i++) {
                            try {
                                FileChannel wChannel = channelsS[i];
                                if (wChannel != null) {
                                    ByteBuffer outbuf = bufferForPartitions[i];
                                    accessor1.reset(outbuf);
                                    if (accessor1.getTupleCount() > 0) {
                                        wChannel.write(outbuf);
                                        numWriteI2++;
                                    }
                                }
                            } catch (IOException e) {
                                throw new HyracksDataException(e);
                            }
                        }

                        inBuffer.clear();
                        int tableSize = -1;
                        if (memoryForHashtable == 0) {
                            tableSize = (int) (B * recordsPerFrame * factor);
                        } else {
                            tableSize = (int) (memsize * recordsPerFrame * factor);
                        }
                        for (int partitionid = 0; partitionid < B; partitionid++) {

                            int state = 0;
                            try {
                                FileChannel inChannel = channelsR[partitionid];
                                if (inChannel != null) {
                                    inChannel.position(0);
                                    InMemoryHashJoin joiner = new InMemoryHashJoin(ctx, tableSize,
                                            new FrameTupleAccessor(ctx, rd0), hpcRep0,
                                            new FrameTupleAccessor(ctx, rd1), hpcRep1, new FrameTuplePairComparator(
                                                    keys0, keys1, comparators));
                                    state = inChannel.read(inBuffer);
                                    while (state != -1) {
                                        numReadI1++;
                                        ByteBuffer copyBuffer = ctx.getResourceManager().allocateFrame();
                                        FrameUtils.copy(inBuffer, copyBuffer);
                                        joiner.build(copyBuffer);
                                        inBuffer.clear();
                                        memRi[partitionid]++;
                                        state = inChannel.read(inBuffer);
                                    }
                                    appender.reset(outBuffer, false);

                                    inBuffer.clear();

                                    FileChannel inChannelS = channelsS[partitionid];
                                    if (inChannelS != null) {
                                        inChannelS.position(0);
                                        while (inChannelS.read(inBuffer) != -1) {
                                            numReadI2++;
                                            joiner.join(inBuffer, writer);
                                            inBuffer.clear();
                                        }
                                        inChannelS.close();
                                        joiner.closeJoin(writer);
                                    }
                                    inChannel.close();
                                }
                            } catch (IOException e) {
                                throw new HyracksDataException(e);
                            }
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
                public void setFrameWriter(int index, IFrameWriter writer) {
                    if (index != 0) {
                        throw new IllegalStateException();
                    }
                    this.writer = writer;
                }
            };
            return op;
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return HybridHashJoinOperatorDescriptor.this;
        }

        @Override
        public boolean supportsPullInterface() {
            return false;
        }

        @Override
        public boolean supportsPushInterface() {
            return true;
        }
    }
}