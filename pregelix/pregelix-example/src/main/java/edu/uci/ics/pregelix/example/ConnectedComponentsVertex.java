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

package edu.uci.ics.pregelix.example;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Edge;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.graph.VertexCombiner;
import edu.uci.ics.pregelix.api.io.VertexWriter;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat.TextVertexWriter;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.example.PageRankVertex.SimplePageRankVertexOutputFormat;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.inputformat.TextPageRankInputFormat;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * Demonstrates the basic Pregel connected components implementation.
 */
public class ConnectedComponentsVertex extends Vertex<VLongWritable, VLongWritable, FloatWritable, VLongWritable> {
    /**
     * Test whether combiner is called to get the minimum ID in the cluster
     */
    public static class SimpleMinCombiner implements VertexCombiner<VLongWritable, VLongWritable> {
        private long min = Long.MAX_VALUE;
        private VLongWritable agg = new VLongWritable();

        @Override
        public void step(VLongWritable vertexIndex, VLongWritable msg) throws IOException {
            long value = msg.get();
            if (min > value)
                min = value;
        }

        @Override
        public void init() {
            min = Long.MAX_VALUE;
        }

        @Override
        public VLongWritable finish() {
            agg.set(min);
            return agg;
        }
    }

    private VLongWritable outputValue = new VLongWritable();
    private VLongWritable vertexValue = new VLongWritable();
    private long minID;

    @Override
    public void compute(Iterator<VLongWritable> msgIterator) {
        if (getSuperstep() == 1) {
            minID = getVertexId().get();
            vertexValue.set(minID);
            setVertexValue(vertexValue);
            sendOutMsgs();
        } else {
            minID = getVertexId().get();
            while (msgIterator.hasNext()) {
                minID = Math.min(minID, msgIterator.next().get());
            }
            if (minID < getVertexValue().get()) {
                vertexValue.set(minID);
                setVertexValue(vertexValue);
                sendOutMsgs();
            }
        }
        voteToHalt();
    }

    private void sendOutMsgs() {
        for (Edge<VLongWritable, FloatWritable> edge : getEdges()) {
            outputValue.set(minID);
            sendMsg(edge.getDestVertexId(), outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(ConnectedComponentsVertex.class.getSimpleName());
        job.setVertexClass(ConnectedComponentsVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setVertexCombinerClass(ConnectedComponentsVertex.SimpleMinCombiner.class);
        Client.run(args, job);
    }

    /**
     * Simple VertexWriter that support
     */
    public static class SimpleConnectedComponentsVertexWriter extends
            TextVertexWriter<VLongWritable, VLongWritable, FloatWritable> {
        public SimpleConnectedComponentsVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VLongWritable, VLongWritable, FloatWritable, ?> vertex) throws IOException,
                InterruptedException {
            getRecordWriter().write(new Text(vertex.getVertexId().toString()),
                    new Text(vertex.getVertexValue().toString()));
        }
    }

    /**
     * output format for connected components
     */
    public static class SimpleConnectedComponentsVertexOutputFormat extends
            TextVertexOutputFormat<VLongWritable, VLongWritable, FloatWritable> {

        @Override
        public VertexWriter<VLongWritable, VLongWritable, FloatWritable> createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter = textOutputFormat.getRecordWriter(context);
            return new SimpleConnectedComponentsVertexWriter(recordWriter);
        }

    }

}
