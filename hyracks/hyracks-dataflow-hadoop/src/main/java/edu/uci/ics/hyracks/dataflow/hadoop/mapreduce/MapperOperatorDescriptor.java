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
package edu.uci.ics.hyracks.dataflow.hadoop.mapreduce;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.sort.Algorithm;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortRunGenerator;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortRunMerger;

public class MapperOperatorDescriptor<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable>
        extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final int jobId;
    private final MarshalledWritable<Configuration> config;
    private final IInputSplitProviderFactory factory;

    public MapperOperatorDescriptor(IOperatorDescriptorRegistry spec, int jobId,
            MarshalledWritable<Configuration> config, IInputSplitProviderFactory factory) throws HyracksDataException {
        super(spec, 0, 1);
        this.jobId = jobId;
        this.config = config;
        this.factory = factory;
        HadoopHelper helper = new HadoopHelper(config);
        recordDescriptors[0] = helper.getMapOutputRecordDescriptor();
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        final HadoopHelper helper = new HadoopHelper(config);
        final Configuration conf = helper.getConfiguration();
        final Mapper<K1, V1, K2, V2> mapper = helper.getMapper();
        final InputFormat<K1, V1> inputFormat = helper.getInputFormat();
        final IInputSplitProvider isp = factory.createInputSplitProvider(partition);
        final TaskAttemptID taId = new TaskAttemptID("foo", jobId, true, partition, 0);
        final TaskAttemptContext taskAttemptContext = helper.createTaskAttemptContext(taId);

        final int framesLimit = helper.getSortFrameLimit(ctx);
        final IBinaryComparatorFactory[] comparatorFactories = helper.getSortComparatorFactories();

        class SortingRecordWriter extends RecordWriter<K2, V2> {
            private final ArrayTupleBuilder tb;
            private final ByteBuffer frame;
            private final FrameTupleAppender fta;
            private ExternalSortRunGenerator runGen;
            private int blockId;

            public SortingRecordWriter() throws HyracksDataException {
                tb = new ArrayTupleBuilder(2);
                frame = ctx.allocateFrame();
                fta = new FrameTupleAppender(ctx.getFrameSize());
                fta.reset(frame, true);
            }

            public void initBlock(int blockId) throws HyracksDataException {
                runGen = new ExternalSortRunGenerator(ctx, new int[] { 0 }, null, comparatorFactories,
                        helper.getMapOutputRecordDescriptorWithoutExtraFields(), Algorithm.MERGE_SORT, framesLimit);
                this.blockId = blockId;
            }

            @Override
            public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
            }

            @Override
            public void write(K2 key, V2 value) throws IOException, InterruptedException {
                DataOutput dos = tb.getDataOutput();
                tb.reset();
                key.write(dos);
                tb.addFieldEndOffset();
                value.write(dos);
                tb.addFieldEndOffset();
                if (!fta.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    runGen.nextFrame(frame);
                    fta.reset(frame, true);
                    if (!fta.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                        throw new HyracksDataException("Record size (" + tb.getSize() + ") larger than frame size ("
                                + frame.capacity() + ")");
                    }
                }
            }

            public void sortAndFlushBlock(final IFrameWriter writer) throws HyracksDataException {
                if (fta.getTupleCount() > 0) {
                    runGen.nextFrame(frame);
                    fta.reset(frame, true);
                }
                runGen.close();
                IFrameWriter delegatingWriter = new IFrameWriter() {
                    private final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                    private final ByteBuffer outFrame = ctx.allocateFrame();
                    private final FrameTupleAccessor fta = new FrameTupleAccessor(ctx.getFrameSize(),
                            helper.getMapOutputRecordDescriptorWithoutExtraFields());
                    private final ArrayTupleBuilder tb = new ArrayTupleBuilder(3);

                    @Override
                    public void open() throws HyracksDataException {
                        appender.reset(outFrame, true);
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        fta.reset(buffer);
                        int n = fta.getTupleCount();
                        for (int i = 0; i < n; ++i) {
                            tb.reset();
                            tb.addField(fta, i, 0);
                            tb.addField(fta, i, 1);
                            try {
                                tb.getDataOutput().writeInt(blockId);
                            } catch (IOException e) {
                                throw new HyracksDataException(e);
                            }
                            tb.addFieldEndOffset();
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                FrameUtils.flushFrame(outFrame, writer);
                                appender.reset(outFrame, true);
                                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                    throw new IllegalStateException();
                                }
                            }
                        }
                    }

                    @Override
                    public void close() throws HyracksDataException {
                        if (appender.getTupleCount() > 0) {
                            FrameUtils.flushFrame(outFrame, writer);
                        }
                    }

                    @Override
                    public void fail() throws HyracksDataException {
                        // TODO Auto-generated method stub

                    }
                };
                if (helper.hasCombiner()) {
                    Reducer<K2, V2, K2, V2> combiner = helper.getCombiner();
                    TaskAttemptID ctaId = new TaskAttemptID("foo", jobId, true, partition, 0);
                    TaskAttemptContext ctaskAttemptContext = helper.createTaskAttemptContext(taId);
                    final IFrameWriter outputWriter = delegatingWriter;
                    RecordWriter<K2, V2> recordWriter = new RecordWriter<K2, V2>() {
                        private final FrameTupleAppender fta = new FrameTupleAppender(ctx.getFrameSize());
                        private final ByteBuffer buffer = ctx.allocateFrame();
                        private final ArrayTupleBuilder tb = new ArrayTupleBuilder(2);

                        {
                            fta.reset(buffer, true);
                            outputWriter.open();
                        }

                        @Override
                        public void write(K2 key, V2 value) throws IOException, InterruptedException {
                            DataOutput dos = tb.getDataOutput();
                            tb.reset();
                            key.write(dos);
                            tb.addFieldEndOffset();
                            value.write(dos);
                            tb.addFieldEndOffset();
                            if (!fta.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                FrameUtils.flushFrame(buffer, outputWriter);
                                fta.reset(buffer, true);
                                if (!fta.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                    throw new IllegalStateException();
                                }
                            }
                        }

                        @Override
                        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                            if (fta.getTupleCount() > 0) {
                                FrameUtils.flushFrame(buffer, outputWriter);
                                outputWriter.close();
                            }
                        }
                    };
                    delegatingWriter = new ReduceWriter<K2, V2, K2, V2>(ctx, helper,
                            new int[] { HadoopHelper.KEY_FIELD_INDEX }, helper.getGroupingComparatorFactories(),
                            helper.getMapOutputRecordDescriptorWithoutExtraFields(), combiner, recordWriter, ctaId,
                            ctaskAttemptContext);
                }
                IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                for (int i = 0; i < comparatorFactories.length; ++i) {
                    comparators[i] = comparatorFactories[i].createBinaryComparator();
                }
                ExternalSortRunMerger merger = new ExternalSortRunMerger(ctx, runGen.getFrameSorter(),
                        runGen.getRuns(), new int[] { 0 }, comparators, null,
                        helper.getMapOutputRecordDescriptorWithoutExtraFields(), framesLimit, delegatingWriter);
                merger.process();
            }
        }

        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                writer.open();
                try {
                    SortingRecordWriter recordWriter = new SortingRecordWriter();
                    InputSplit split = null;
                    int blockId = 0;
                    while ((split = isp.next()) != null) {
                        try {
                            RecordReader<K1, V1> recordReader = inputFormat.createRecordReader(split,
                                    taskAttemptContext);
                            ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
                            try {
                                Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
                                recordReader.initialize(split, taskAttemptContext);
                            } finally {
                                Thread.currentThread().setContextClassLoader(ctxCL);
                            }
                            recordWriter.initBlock(blockId);
                            Mapper<K1, V1, K2, V2>.Context mCtx = mapper.new Context(conf, taId, recordReader,
                                    recordWriter, null, null, split);
                            mapper.run(mCtx);
                            recordReader.close();
                            recordWriter.sortAndFlushBlock(writer);
                            ++blockId;
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        } catch (InterruptedException e) {
                            throw new HyracksDataException(e);
                        }
                    }
                } finally {
                    writer.close();
                }
            }
        };
    }
}
