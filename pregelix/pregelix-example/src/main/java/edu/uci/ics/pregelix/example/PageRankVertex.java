/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.pregelix.example;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Maps;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.graph.VertexCombiner;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.io.VertexWriter;
import edu.uci.ics.pregelix.api.io.generated.GeneratedVertexInputFormat;
import edu.uci.ics.pregelix.api.io.generated.GeneratedVertexReader;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat.TextVertexWriter;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.inputformat.TextPageRankInputFormat;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */
public class PageRankVertex extends Vertex<VLongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    public static final String ITERATIONS = "HyracksPageRankVertex.iteration";
    private DoubleWritable outputValue = new DoubleWritable();
    private DoubleWritable vertexValue = new DoubleWritable();

    /**
     * Test whether combiner is called by summing up the messages.
     */
    public static class SimpleSumCombiner implements VertexCombiner<VLongWritable, DoubleWritable> {
        private double sum = 0.0;
        private DoubleWritable agg = new DoubleWritable();

        @Override
        public void step(VLongWritable vertexIndex, DoubleWritable msg) throws IOException {
            sum += msg.get();
        }

        @Override
        public void init() {
            sum = 0.0;
        }

        @Override
        public DoubleWritable finish() {
            agg.set(sum);
            return agg;
        }
    }

    @Override
    public void compute(Iterator<DoubleWritable> msgIterator) {
        int maxIteration = this.getContext().getConfiguration().getInt(ITERATIONS, 10);
        if (getSuperstep() == 1) {
            vertexValue.set(1.0 / getNumVertices());
            setVertexValue(vertexValue);
        }
        if (getSuperstep() >= 2 && getSuperstep() <= maxIteration) {
            double sum = 0;
            while (msgIterator.hasNext()) {
                sum += msgIterator.next().get();
            }
            vertexValue.set((0.15 / getNumVertices()) + 0.85 * sum);
            setVertexValue(vertexValue);
        }

        if (getSuperstep() >= 1 && getSuperstep() < maxIteration) {
            long edges = getNumOutEdges();
            outputValue.set(getVertexValue().get() / edges);
            sendMsgToAllEdges(outputValue);
        } else {
            voteToHalt();
        }
    }

    /**
     * Simple VertexReader that supports {@link SimplePageRankVertex}
     */
    public static class SimplePageRankVertexReader extends
            GeneratedVertexReader<VLongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
        /** Class logger */
        private static final Logger LOG = Logger.getLogger(SimplePageRankVertexReader.class.getName());
        private Map<VLongWritable, FloatWritable> edges = Maps.newHashMap();

        public SimplePageRankVertexReader() {
            super();
        }

        @Override
        public boolean nextVertex() {
            return totalRecords > recordsRead;
        }

        @Override
        public Vertex<VLongWritable, DoubleWritable, FloatWritable, DoubleWritable> getCurrentVertex()
                throws IOException {
            Vertex<VLongWritable, DoubleWritable, FloatWritable, DoubleWritable> vertex = BspUtils
                    .createVertex(configuration);

            VLongWritable vertexId = new VLongWritable((inputSplit.getSplitIndex() * totalRecords) + recordsRead);
            DoubleWritable vertexValue = new DoubleWritable(vertexId.get() * 10d);
            long destVertexId = (vertexId.get() + 1) % (inputSplit.getNumSplits() * totalRecords);
            float edgeValue = vertexId.get() * 100f;
            edges.put(new VLongWritable(destVertexId), new FloatWritable(edgeValue));
            vertex.initialize(vertexId, vertexValue, edges, null);
            ++recordsRead;
            if (LOG.getLevel() == Level.FINE) {
                LOG.fine("next: Return vertexId=" + vertex.getVertexId().get() + ", vertexValue="
                        + vertex.getVertexValue() + ", destinationId=" + destVertexId + ", edgeValue=" + edgeValue);
            }
            return vertex;
        }
    }

    /**
     * Simple VertexInputFormat that supports {@link SimplePageRankVertex}
     */
    public static class SimplePageRankVertexInputFormat extends
            GeneratedVertexInputFormat<VLongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
        @Override
        public VertexReader<VLongWritable, DoubleWritable, FloatWritable, DoubleWritable> createVertexReader(
                InputSplit split, TaskAttemptContext context) throws IOException {
            return new SimplePageRankVertexReader();
        }
    }

    /**
     * Simple VertexWriter that supports {@link SimplePageRankVertex}
     */
    public static class SimplePageRankVertexWriter extends
            TextVertexWriter<VLongWritable, DoubleWritable, FloatWritable> {
        public SimplePageRankVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VLongWritable, DoubleWritable, FloatWritable, ?> vertex) throws IOException,
                InterruptedException {
            getRecordWriter().write(new Text(vertex.getVertexId().toString()),
                    new Text(vertex.getVertexValue().toString()));
        }
    }

    /**
     * Simple VertexOutputFormat that supports {@link SimplePageRankVertex}
     */
    public static class SimplePageRankVertexOutputFormat extends
            TextVertexOutputFormat<VLongWritable, DoubleWritable, FloatWritable> {

        @Override
        public VertexWriter<VLongWritable, DoubleWritable, FloatWritable> createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter = textOutputFormat.getRecordWriter(context);
            return new SimplePageRankVertexWriter(recordWriter);
        }
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(PageRankVertex.class.getSimpleName());
        job.setVertexClass(PageRankVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setVertexCombinerClass(PageRankVertex.SimpleSumCombiner.class);
        Client.run(args, job);
    }

}
