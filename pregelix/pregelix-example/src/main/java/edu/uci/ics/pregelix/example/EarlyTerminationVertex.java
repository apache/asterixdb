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
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */
public class EarlyTerminationVertex extends Vertex<VLongWritable, VLongWritable, VLongWritable, VLongWritable> {
    private VLongWritable tempValue = new VLongWritable();

    @Override
    public void compute(Iterator<VLongWritable> msgIterator) {
        if (getSuperstep() == 1) {
            if (getVertexId().get() % 4 == 2) {
                terminatePartition();
            } else {
                tempValue.set(1);
                setVertexValue(tempValue);
            }
        }
        if (getSuperstep() == 2) {
            if (getVertexId().get() % 4 == 3) {
                terminatePartition();
            } else {
                tempValue.set(2);
                setVertexValue(tempValue);
                voteToHalt();
            }
        }
    }

    @Override
    public String toString() {
        return getVertexId() + " " + getVertexValue();
    }

    /**
     * Simple VertexWriter that support
     */
    public static class SimpleEarlyTerminattionVertexWriter extends
            TextVertexWriter<VLongWritable, VLongWritable, VLongWritable> {
        public SimpleEarlyTerminattionVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VLongWritable, VLongWritable, VLongWritable, ?> vertex) throws IOException,
                InterruptedException {
            getRecordWriter().write(new Text(vertex.getVertexId().toString()),
                    new Text(vertex.getVertexValue().toString()));
        }
    }

    public static class SimpleEarlyTerminattionVertexOutputFormat extends
            TextVertexOutputFormat<VLongWritable, VLongWritable, VLongWritable> {

        @Override
        public VertexWriter<VLongWritable, VLongWritable, VLongWritable> createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter = textOutputFormat.getRecordWriter(context);
            return new SimpleEarlyTerminattionVertexWriter(recordWriter);
        }

    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(EarlyTerminationVertex.class.getSimpleName());
        job.setVertexClass(EarlyTerminationVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimpleEarlyTerminattionVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setDynamicVertexValueSize(true);
        Client.run(args, job);
    }

}
