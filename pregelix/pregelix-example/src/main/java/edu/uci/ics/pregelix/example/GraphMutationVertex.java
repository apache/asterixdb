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

package edu.uci.ics.pregelix.example;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat.TextVertexWriter;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.data.VLongNormalizedKeyComputer;
import edu.uci.ics.pregelix.example.inputformat.TextPageRankInputFormat;
import edu.uci.ics.pregelix.example.io.DoubleWritable;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * Demonstrates the basic graph vertex insert/delete implementation.
 */
public class GraphMutationVertex extends Vertex<VLongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    private VLongWritable vid = new VLongWritable();
    private GraphMutationVertex newVertex = null;
    private DoubleWritable msg = new DoubleWritable(0.0);

    @Override
    public void compute(Iterator<DoubleWritable> msgIterator) {
        if (Vertex.getSuperstep() == 1) {
            if (newVertex == null) {
                newVertex = new GraphMutationVertex();
            }
            if (getVertexId().get() < 100) {
                if ((getVertexId().get() % 2 == 0 || getVertexId().get() % 3 == 0)) {
                    deleteVertex(getVertexId());
                } else {
                    vid.set(100 * getVertexId().get());
                    newVertex.setVertexId(vid);
                    newVertex.setVertexValue(getVertexValue());
                    addVertex(vid, newVertex);
                    sendMsg(vid, msg);
                }
            }
            voteToHalt();
        } else {
            if (getVertexId().get() == 1900) {
                deleteVertex(getVertexId());
            }
            voteToHalt();
        }
    }

    /**
     * Simple VertexWriter that supports {@link SimplePageRankVertex}
     */
    public static class SimpleGraphMutationVertexWriter extends
            TextVertexWriter<VLongWritable, DoubleWritable, FloatWritable> {
        public SimpleGraphMutationVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VLongWritable, DoubleWritable, FloatWritable, ?> vertex) throws IOException,
                InterruptedException {
            getRecordWriter().write(new Text(vertex.getVertexId().toString()),
                    new Text(vertex.getVertexValue().toString()));
        }
    }

    @Override
    public String toString() {
        return getVertexId() + " " + getVertexValue();
    }

    /**
     * Simple VertexOutputFormat that supports {@link SimplePageRankVertex}
     */
    public static class SimpleGraphMutationVertexOutputFormat extends
            TextVertexOutputFormat<VLongWritable, DoubleWritable, FloatWritable> {

        @Override
        public VertexWriter<VLongWritable, DoubleWritable, FloatWritable> createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter = textOutputFormat.getRecordWriter(context);
            return new SimpleGraphMutationVertexWriter(recordWriter);
        }
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(GraphMutationVertex.class.getSimpleName());
        job.setVertexClass(GraphMutationVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimpleGraphMutationVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        Client.run(args, job);
    }

}
