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

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.Edge;
import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat.TextVertexWriter;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.data.VLongNormalizedKeyComputer;
import edu.uci.ics.pregelix.example.inputformat.TextConnectedComponentsInputFormat;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * Demonstrates the basic Pregel connected components implementation, for undirected graph (e.g., Facebook, LinkedIn graph).
 */
public class ConnectedComponentsVertex extends Vertex<VLongWritable, VLongWritable, FloatWritable, VLongWritable> {
    /**
     * Test whether combiner is called to get the minimum ID in the cluster
     */
    public static class SimpleMinCombiner extends MessageCombiner<VLongWritable, VLongWritable, VLongWritable> {
        private long min = Long.MAX_VALUE;
        private VLongWritable agg = new VLongWritable();
        private MsgList<VLongWritable> msgList;

        @Override
        public void stepPartial(VLongWritable vertexIndex, VLongWritable msg) throws HyracksDataException {
            long value = msg.get();
            if (min > value) {
                min = value;
            }
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public void init(MsgList msgList) {
            min = Long.MAX_VALUE;
            this.msgList = msgList;
        }

        @Override
        public void stepFinal(VLongWritable vertexIndex, VLongWritable partialAggregate) throws HyracksDataException {
            long value = partialAggregate.get();
            if (min > value) {
                min = value;
            }
        }

        @Override
        public VLongWritable finishPartial() {
            agg.set(min);
            return agg;
        }

        @Override
        public MsgList<VLongWritable> finishFinal() {
            agg.set(min);
            msgList.clear();
            msgList.add(agg);
            return msgList;
        }

        @Override
        public void stepPartial2(VLongWritable vertexIndex, VLongWritable partialAggregate) throws HyracksDataException {
            long value = partialAggregate.get();
            if (min > value) {
                min = value;
            }
        }

        @Override
        public VLongWritable finishPartial2() {
            agg.set(min);
            return agg;
        }
    }

    private VLongWritable tmpVertexValue = new VLongWritable();

    @Override
    public void compute(Iterator<VLongWritable> msgIterator) {
        long currentComponent = getVertexValue().get();
        // First superstep is special, because we can simply look at the neighbors
        if (getSuperstep() == 1) {
            for (Edge<VLongWritable, FloatWritable> edge : getEdges()) {
                long neighbor = edge.getDestVertexId().get();
                if (neighbor < currentComponent) {
                    currentComponent = neighbor;
                }
            }
            // Only need to send value if it is not the own id
            if (currentComponent != getVertexValue().get()) {
                tmpVertexValue.set(currentComponent);
                setVertexValue(tmpVertexValue);
                for (Edge<VLongWritable, FloatWritable> edge : getEdges()) {
                    VLongWritable neighbor = edge.getDestVertexId();
                    if (neighbor.get() > currentComponent) {
                        sendMsg(neighbor, tmpVertexValue);
                    }
                }
            }
        } else {
            boolean changed = false;
            // did we get a smaller id ?
            while (msgIterator.hasNext()) {
                VLongWritable message = msgIterator.next();
                long candidateComponent = message.get();
                if (candidateComponent < currentComponent) {
                    currentComponent = candidateComponent;
                    changed = true;
                }
            }

            // propagate new component id to the neighbors
            if (changed) {
                tmpVertexValue.set(currentComponent);
                setVertexValue(tmpVertexValue);
                sendMsgToAllEdges(tmpVertexValue);
            }
        }
        voteToHalt();
    }

    @Override
    public String toString() {
        return getVertexId() + " " + getVertexValue();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(ConnectedComponentsVertex.class.getSimpleName());
        job.setVertexClass(ConnectedComponentsVertex.class);
        job.setVertexInputFormatClass(TextConnectedComponentsInputFormat.class);
        job.setVertexOutputFormatClass(SimpleConnectedComponentsVertexOutputFormat.class);
        job.setMessageCombinerClass(ConnectedComponentsVertex.SimpleMinCombiner.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setDynamicVertexValueSize(true);
        job.setSkipCombinerKey(true);
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
