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
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.comm.util.FrameUtils;
import edu.uci.ics.hyracks.coreops.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.coreops.RepartitionComputerFactory;
import edu.uci.ics.hyracks.coreops.base.AbstractActivityNode;
import edu.uci.ics.hyracks.coreops.base.AbstractOperatorDescriptor;

public class GraceHashJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final String SMALLRELATION = "RelR";
    private static final String LARGERELATION = "RelS";
    private static final String NUM_PARTITION = "NUMBER_PARTITIONS";
    private static final long serialVersionUID = 1L;
    private final int[] keys0;
    private final int[] keys1;
    private final int inputsize0;
    private final int recordsPerFrame;
    private final int memsize;
    private final double factor;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final IBinaryComparatorFactory[] comparatorFactories;

    private int numReadI1 = 0;
    private int numWrite = 0;
    private int numReadI2 = 0;

    public GraceHashJoinOperatorDescriptor(JobSpecification spec, int memsize, int inputsize0, int recordsPerFrame,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionFactory[] hashFunctionFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.recordsPerFrame = recordsPerFrame;
        this.factor = factor;
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.hashFunctionFactories = hashFunctionFactories;
        this.comparatorFactories = comparatorFactories;
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeTaskGraph(IActivityGraphBuilder builder) {
        HashPartitionActivityNode rpart = new HashPartitionActivityNode(SMALLRELATION, keys0, 0);
        HashPartitionActivityNode spart = new HashPartitionActivityNode(LARGERELATION, keys1, 1);
        JoinActivityNode join = new JoinActivityNode();

        builder.addTask(rpart);
        builder.addSourceEdge(0, rpart, 0);

        builder.addTask(spart);
        builder.addSourceEdge(1, spart, 0);

        builder.addTask(join);
        builder.addBlockingEdge(rpart, spart);
        builder.addBlockingEdge(spart, join);

        builder.addTargetEdge(0, join, 0);
    }

    public int getMemorySize() {
        return memsize;
    }

    private class HashPartitionActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private String relationName;
        private int operatorInputIndex;
        private int keys[];

        public HashPartitionActivityNode(String relationName, int keys[], int operatorInputIndex) {
            this.relationName = relationName;
            this.keys = keys;
            this.operatorInputIndex = operatorInputIndex;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksContext ctx, final IOperatorEnvironment env,
                final IRecordDescriptorProvider recordDescProvider, int partition, final int nPartitions) {
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }
            IOperatorNodePushable op = new IOperatorNodePushable() {
                private final FrameTupleAccessor accessor0 = new FrameTupleAccessor(ctx,
                        recordDescProvider.getInputRecordDescriptor(getOperatorId(), operatorInputIndex));

                ITuplePartitionComputer hpc = new FieldHashPartitionComputerFactory(keys, hashFunctionFactories)
                        .createPartitioner();

                private final FrameTupleAppender appender = new FrameTupleAppender(ctx);
                private ByteBuffer[] outbufs;
                private File[] files;
                private FileChannel[] channels;
                private final int numPartitions = (int) Math.ceil(Math.sqrt(inputsize0 * factor / nPartitions));

                @Override
                public void setFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                    throw new IllegalArgumentException();
                }

                @Override
                public void close() throws HyracksDataException {
                    for (int i = 0; i < numPartitions; i++) {
                        try {
                            ByteBuffer head = outbufs[i];
                            accessor0.reset(head);
                            if (accessor0.getTupleCount() > 0) {
                                FileChannel wChannel = channels[i];
                                if (wChannel == null) {
                                    wChannel = new RandomAccessFile(files[i], "rw").getChannel();
                                    channels[i] = wChannel;
                                }
                                wChannel.write(head);
                                numWrite++;
                            }
                        } catch (IOException e) {
                            throw new HyracksDataException("error generating partition " + files[i].getName());
                        }
                    }

                    env.set(relationName, channels);
                    env.set(NUM_PARTITION, numPartitions);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor0.reset(buffer);
                    int tCount = accessor0.getTupleCount();
                    for (int i = 0; i < tCount; ++i) {

                        int entry = hpc.partition(accessor0, i, numPartitions);
                        boolean newBuffer = false;
                        ByteBuffer outbuf = outbufs[entry];
                        while (true) {
                            appender.reset(outbuf, newBuffer);
                            if (appender.append(accessor0, i)) {
                                break;
                            } else {
                                // buffer is full, ie. we cannot fit the tuple
                                // into the buffer -- write it to disk
                                try {

                                    FileChannel wChannel = channels[entry];
                                    if (wChannel == null) {
                                        wChannel = new RandomAccessFile(files[entry], "rw").getChannel();
                                        channels[entry] = wChannel;
                                    }

                                    wChannel.write(outbuf);
                                    numWrite++;
                                    outbuf.clear();
                                    newBuffer = true;
                                } catch (IOException e) {
                                    throw new HyracksDataException("error generating partition "
                                            + files[entry].getName());
                                }
                            }
                        }
                    }
                }

                @Override
                public void open() throws HyracksDataException {
                    outbufs = new ByteBuffer[numPartitions];
                    files = new File[numPartitions];
                    channels = new FileChannel[numPartitions];
                    for (int i = 0; i < numPartitions; i++) {
                        try {
                            files[i] = ctx.getResourceManager().createFile(relationName, null);
                            files[i].deleteOnExit();
                            outbufs[i] = ctx.getResourceManager().allocateFrame();
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                    }
                }

            };
            return op;
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return GraceHashJoinOperatorDescriptor.this;
        }
    }

    private class JoinActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksContext ctx, final IOperatorEnvironment env,
                final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 1);

            IOperatorNodePushable op = new IOperatorNodePushable() {
                private InMemoryHashJoin joiner;

                private IFrameWriter writer;
                private FileChannel[] channelsR;
                private FileChannel[] channelsS;
                private int numPartitions;
                private int[] maxBufferRi;

                @Override
                public void open() throws HyracksDataException {
                    channelsR = (FileChannel[]) env.get(SMALLRELATION);
                    channelsS = (FileChannel[]) env.get(LARGERELATION);
                    numPartitions = (Integer) env.get(NUM_PARTITION);

                    ITuplePartitionComputer hpcRep0 = new RepartitionComputerFactory(numPartitions,
                            new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories)).createPartitioner();
                    ITuplePartitionComputer hpcRep1 = new RepartitionComputerFactory(numPartitions,
                            new FieldHashPartitionComputerFactory(keys1, hashFunctionFactories)).createPartitioner();

                    writer.open();// open for probe

                    maxBufferRi = new int[numPartitions];

                    ByteBuffer buffer = ctx.getResourceManager().allocateFrame();// input
                    // buffer
                    int tableSize = (int) (numPartitions * recordsPerFrame * factor);
                    for (int partitionid = 0; partitionid < numPartitions; partitionid++) {
                        int counter = 0;
                        int state = 0;
                        try {
                            FileChannel inChannelR = channelsR[partitionid];
                            if (inChannelR != null) {
                                inChannelR.position(0);
                                while (state != -1) {

                                    joiner = new InMemoryHashJoin(ctx, tableSize, new FrameTupleAccessor(ctx, rd0),
                                            hpcRep0, new FrameTupleAccessor(ctx, rd1), hpcRep1,
                                            new FrameTuplePairComparator(keys0, keys1, comparators));
                                    // build

                                    state = inChannelR.read(buffer);
                                    while (state != -1) {

                                        ByteBuffer copyBuffer = ctx.getResourceManager().allocateFrame();
                                        FrameUtils.copy(buffer, copyBuffer);
                                        joiner.build(copyBuffer);
                                        numReadI1++;
                                        counter++;
                                        if (counter > maxBufferRi[partitionid])
                                            maxBufferRi[partitionid] = counter;

                                        buffer.clear();
                                        state = inChannelR.read(buffer);
                                    }

                                    // probe

                                    buffer.clear();

                                    FileChannel inChannelS = channelsS[partitionid];
                                    if (inChannelS != null) {
                                        inChannelS.position(0);
                                        while (inChannelS.read(buffer) != -1) {
                                            joiner.join(buffer, writer);
                                            numReadI2++;
                                            buffer.clear();
                                        }
                                        inChannelS.close();
                                        joiner.closeJoin(writer);
                                    }
                                }
                                inChannelR.close();
                            }
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                    }
                    writer.close();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    throw new IllegalStateException();
                }

                @Override
                public void close() throws HyracksDataException {
                    env.set(LARGERELATION, null);
                    env.set(SMALLRELATION, null);
                    env.set(NUM_PARTITION, null);
                }

                @Override
                public void setFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
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
            return GraceHashJoinOperatorDescriptor.this;
        }
    }
}
