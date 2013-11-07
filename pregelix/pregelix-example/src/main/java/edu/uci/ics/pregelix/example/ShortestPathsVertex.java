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

import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.Edge;
import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.example.PageRankVertex.SimplePageRankVertexOutputFormat;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.data.VLongNormalizedKeyComputer;
import edu.uci.ics.pregelix.example.inputformat.TextShortestPathsInputFormat;
import edu.uci.ics.pregelix.example.io.DoubleWritable;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
public class ShortestPathsVertex extends Vertex<VLongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    /**
     * Test whether combiner is called by summing up the messages.
     */
    public static class SimpleMinCombiner extends MessageCombiner<VLongWritable, DoubleWritable, DoubleWritable> {
        private double min = Double.MAX_VALUE;
        private DoubleWritable agg = new DoubleWritable();
        private MsgList<DoubleWritable> msgList;

        @Override
        public void stepPartial(VLongWritable vertexIndex, DoubleWritable msg) throws HyracksDataException {
            double value = msg.get();
            if (min > value)
                min = value;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public void init(MsgList msgList) {
            min = Double.MAX_VALUE;
            this.msgList = msgList;
        }

        @Override
        public DoubleWritable finishPartial() {
            agg.set(min);
            return agg;
        }

        @Override
        public void stepFinal(VLongWritable vertexIndex, DoubleWritable partialAggregate) throws HyracksDataException {
            double value = partialAggregate.get();
            if (min > value)
                min = value;
        }

        @Override
        public MsgList<DoubleWritable> finishFinal() {
            agg.set(min);
            msgList.clear();
            msgList.add(agg);
            return msgList;
        }
    }

    private DoubleWritable outputValue = new DoubleWritable();
    private DoubleWritable tmpVertexValue = new DoubleWritable();
    /** The shortest paths id */
    public static final String SOURCE_ID = "SimpleShortestPathsVertex.sourceId";
    /** Default shortest paths id */
    public static final long SOURCE_ID_DEFAULT = 1;
    /** the source vertex id */
    private long sourceId = -1;

    /**
     * Is this vertex the source id?
     * 
     * @return True if the source id
     */
    private boolean isSource() {
        return (getVertexId().get() == sourceId);
    }

    @Override
    public void compute(Iterator<DoubleWritable> msgIterator) {
        if (sourceId < 0) {
            sourceId = getContext().getConfiguration().getLong(SOURCE_ID, SOURCE_ID_DEFAULT);
        }
        if (getSuperstep() == 1) {
            tmpVertexValue.set(Double.MAX_VALUE);
            setVertexValue(tmpVertexValue);
        }
        double minDist = isSource() ? 0d : Double.MAX_VALUE;
        while (msgIterator.hasNext()) {
            minDist = Math.min(minDist, msgIterator.next().get());
        }
        if (minDist < getVertexValue().get()) {
            tmpVertexValue.set(minDist);
            setVertexValue(tmpVertexValue);
            for (Edge<VLongWritable, FloatWritable> edge : getEdges()) {
                outputValue.set(minDist + edge.getEdgeValue().get());
                sendMsg(edge.getDestVertexId(), outputValue);
            }
        }
        voteToHalt();
    }

    @Override
    public String toString() {
        return getVertexId() + " " + getVertexValue();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(ShortestPathsVertex.class.getSimpleName());
        job.setVertexClass(ShortestPathsVertex.class);
        job.setVertexInputFormatClass(TextShortestPathsInputFormat.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setMessageCombinerClass(ShortestPathsVertex.SimpleMinCombiner.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.getConfiguration().setLong(SOURCE_ID, 0);
        Client.run(args, job);
    }

}
