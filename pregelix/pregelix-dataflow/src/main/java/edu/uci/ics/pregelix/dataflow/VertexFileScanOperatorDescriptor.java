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
package edu.uci.ics.pregelix.dataflow;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.hdfs.ContextFactory;
import edu.uci.ics.hyracks.hdfs2.dataflow.FileSplitsFactory;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexInputFormat;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;

@SuppressWarnings("rawtypes")
public class VertexFileScanOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final FileSplitsFactory splitsFactory;
    private final IConfigurationFactory confFactory;
    private final int fieldSize = 2;
    private final String[] scheduledLocations;
    private final boolean[] executed;

    /**
     * @param spec
     */
    public VertexFileScanOperatorDescriptor(JobSpecification spec, RecordDescriptor rd, List<InputSplit> splits,
            String[] scheduledLocations, IConfigurationFactory confFactory) throws HyracksException {
        super(spec, 0, 1);
        List<FileSplit> fileSplits = new ArrayList<FileSplit>();
        for (int i = 0; i < splits.size(); i++) {
            fileSplits.add((FileSplit) splits.get(i));
        }
        this.splitsFactory = new FileSplitsFactory(fileSplits);
        this.confFactory = confFactory;
        this.scheduledLocations = scheduledLocations;
        this.executed = new boolean[scheduledLocations.length];
        Arrays.fill(executed, false);
        this.recordDescriptors[0] = rd;
    }

    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        final List<FileSplit> splits = splitsFactory.getSplits();

        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            private ContextFactory ctxFactory = new ContextFactory();

            @Override
            public void initialize() throws HyracksDataException {
                try {
                    Configuration conf = confFactory.createConfiguration(ctx);
                    writer.open();
                    for (int i = 0; i < scheduledLocations.length; i++) {
                        if (scheduledLocations[i].equals(ctx.getJobletContext().getApplicationContext().getNodeId())) {
                            /**
                             * pick one from the FileSplit queue
                             */
                            synchronized (executed) {
                                if (!executed[i]) {
                                    executed[i] = true;
                                } else {
                                    continue;
                                }
                            }
                            loadVertices(ctx, conf, i);
                        }
                    }
                    writer.close();
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

            /**
             * Load the vertices
             * 
             * @parameter IHyracks ctx
             * @throws IOException
             * @throws IllegalAccessException
             * @throws InstantiationException
             * @throws ClassNotFoundException
             * @throws InterruptedException
             */
            @SuppressWarnings("unchecked")
            private void loadVertices(final IHyracksTaskContext ctx, Configuration conf, int splitId)
                    throws IOException, ClassNotFoundException, InterruptedException, InstantiationException,
                    IllegalAccessException {
                ByteBuffer frame = ctx.allocateFrame();
                FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                appender.reset(frame, true);

                VertexInputFormat vertexInputFormat = BspUtils.createVertexInputFormat(conf);
                InputSplit split = splits.get(splitId);
                TaskAttemptContext mapperContext = ctxFactory.createContext(conf, splitId);
                mapperContext.getConfiguration().setClassLoader(ctx.getJobletContext().getClassLoader());

                VertexReader vertexReader = vertexInputFormat.createVertexReader(split, mapperContext);
                vertexReader.initialize(split, mapperContext);
                Vertex readerVertex = (Vertex) BspUtils.createVertex(mapperContext.getConfiguration());
                ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldSize);
                DataOutput dos = tb.getDataOutput();

                /**
                 * set context
                 */
                Vertex.setContext(mapperContext);

                /**
                 * empty vertex value
                 */
                Writable emptyVertexValue = (Writable) BspUtils.createVertexValue(conf);

                while (vertexReader.nextVertex()) {
                    readerVertex = vertexReader.getCurrentVertex();
                    tb.reset();
                    if (readerVertex.getVertexId() == null) {
                        throw new IllegalArgumentException("loadVertices: Vertex reader returned a vertex "
                                + "without an id!  - " + readerVertex);
                    }
                    if (readerVertex.getVertexValue() == null) {
                        readerVertex.setVertexValue(emptyVertexValue);
                    }
                    WritableComparable vertexId = readerVertex.getVertexId();
                    vertexId.write(dos);
                    tb.addFieldEndOffset();

                    readerVertex.write(dos);
                    tb.addFieldEndOffset();

                    if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                        if (appender.getTupleCount() <= 0)
                            throw new IllegalStateException("zero tuples in a frame!");
                        FrameUtils.flushFrame(frame, writer);
                        appender.reset(frame, true);
                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            throw new IllegalStateException();
                        }
                    }
                }

                vertexReader.close();
                if (appender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(frame, writer);
                }
                System.gc();
            }
        };
    }
}