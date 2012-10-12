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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;

import edu.uci.ics.pregelix.api.graph.Edge;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.graph.VertexCombiner;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.example.PageRankVertex.SimplePageRankVertexOutputFormat;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.inputformat.TextShortestPathsInputFormat;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
public class ShortestPathsVertex extends Vertex<VLongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    /**
     * Test whether combiner is called by summing up the messages.
     */
    public static class SimpleMinCombiner implements VertexCombiner<VLongWritable, DoubleWritable> {
        private double min = Double.MAX_VALUE;
        private DoubleWritable agg = new DoubleWritable();

        @Override
        public void step(VLongWritable vertexIndex, DoubleWritable msg) throws IOException {
            double value = msg.get();
            if (min > value)
                min = value;
        }

        @Override
        public void init() {
            min = Double.MAX_VALUE;
        }

        @Override
        public DoubleWritable finish() {
            agg.set(min);
            return agg;
        }
    }

    private DoubleWritable outputValue = new DoubleWritable();
    private DoubleWritable vertexValue = new DoubleWritable();
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(ShortestPathsVertex.class.getName());
    /** The shortest paths id */
    public static final String SOURCE_ID = "SimpleShortestPathsVertex.sourceId";
    /** Default shortest paths id */
    public static final long SOURCE_ID_DEFAULT = 1;

    /**
     * Is this vertex the source id?
     * 
     * @return True if the source id
     */
    private boolean isSource() {
        return (getVertexId().get() == getContext().getConfiguration().getLong(SOURCE_ID, SOURCE_ID_DEFAULT));
    }

    @Override
    public void compute(Iterator<DoubleWritable> msgIterator) {
        if (getSuperstep() == 1) {
            vertexValue.set(Double.MAX_VALUE);
            setVertexValue(vertexValue);
        }
        double minDist = isSource() ? 0d : Double.MAX_VALUE;
        while (msgIterator.hasNext()) {
            minDist = Math.min(minDist, msgIterator.next().get());
        }
        if (LOG.getLevel() == Level.FINE) {
            LOG.fine("Vertex " + getVertexId() + " got minDist = " + minDist + " vertex value = " + getVertexValue());
        }
        if (minDist < getVertexValue().get()) {
            vertexValue.set(minDist);
            setVertexValue(vertexValue);
            for (Edge<VLongWritable, FloatWritable> edge : getEdges()) {
                if (LOG.getLevel() == Level.FINE) {
                    LOG.fine("Vertex " + getVertexId() + " sent to " + edge.getDestVertexId() + " = "
                            + (minDist + edge.getEdgeValue().get()));
                }
                outputValue.set(minDist + edge.getEdgeValue().get());
                sendMsg(edge.getDestVertexId(), outputValue);
            }
        }
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(ShortestPathsVertex.class.getSimpleName());
        job.setVertexClass(ShortestPathsVertex.class);
        job.setVertexInputFormatClass(TextShortestPathsInputFormat.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setVertexCombinerClass(ShortestPathsVertex.SimpleMinCombiner.class);
        job.getConfiguration().setLong(SOURCE_ID, 0);
        Client.run(args, job);
    }

}
