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
import java.util.Random;

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
public class MessageOverflowVertex extends Vertex<VLongWritable, VLongWritable, VLongWritable, VLongWritable> {

    private VLongWritable outputMsg = new VLongWritable(1);
    private Random rand = new Random(System.currentTimeMillis());
    private VLongWritable tmpVertexValue = new VLongWritable(0);
    private int numOfMsgClones = 10000;
    private int numIncomingMsgs = 0;

    @Override
    public void open() {
        if (getSuperstep() == 2) {
            numIncomingMsgs = 0;
        }
    }

    @Override
    public void compute(Iterator<VLongWritable> msgIterator) {
        if (getSuperstep() == 1) {
            for (int i = 0; i < numOfMsgClones; i++) {
                outputMsg.set(Math.abs(rand.nextLong()));
                sendMsgToAllEdges(outputMsg);
            }
            tmpVertexValue.set(0);
            setVertexValue(tmpVertexValue);
        }
        if (getSuperstep() == 2) {
            while (msgIterator.hasNext()) {
                msgIterator.next();
                numIncomingMsgs++;
            }
        }
    }

    @Override
    public void close() {
        if (getSuperstep() == 2) {
            tmpVertexValue.set(numIncomingMsgs);
            setVertexValue(tmpVertexValue);
            voteToHalt();
        }
    }

    @Override
    public String toString() {
        return getVertexId() + " " + getVertexValue();
    }

    /**
     * Simple VertexWriter that support
     */
    public static class SimpleMessageOverflowVertexWriter extends
            TextVertexWriter<VLongWritable, VLongWritable, VLongWritable> {
        public SimpleMessageOverflowVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VLongWritable, VLongWritable, VLongWritable, ?> vertex) throws IOException,
                InterruptedException {
            getRecordWriter().write(new Text(vertex.getVertexId().toString()),
                    new Text(vertex.getVertexValue().toString()));
        }
    }

    public static class SimpleMessageOverflowVertexOutputFormat extends
            TextVertexOutputFormat<VLongWritable, VLongWritable, VLongWritable> {

        @Override
        public VertexWriter<VLongWritable, VLongWritable, VLongWritable> createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter = textOutputFormat.getRecordWriter(context);
            return new SimpleMessageOverflowVertexWriter(recordWriter);
        }

    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(MessageOverflowVertex.class.getSimpleName());
        job.setVertexClass(MessageOverflowVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimpleMessageOverflowVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setDynamicVertexValueSize(true);
        Client.run(args, job);
    }

}
