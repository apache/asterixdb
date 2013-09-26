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

package edu.uci.ics.pregelix.benchmark;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.VLongWritable;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */
public class PageRankVertex extends Vertex<VLongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    public static final String ITERATIONS = "HyracksPageRankVertex.iteration";
    private final DoubleWritable vertexValue = new DoubleWritable();
    private final DoubleWritable msg = new DoubleWritable();
    private int maxIteration = -1;

    @Override
    public void compute(Iterable<DoubleWritable> msgIterator) {
        if (maxIteration < 0) {
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 10);
        }
        if (getSuperstep() == 1) {
            vertexValue.set(1.0 / getTotalNumVertices());
        }
        if (getSuperstep() >= 2 && getSuperstep() <= maxIteration) {
            double sum = 0;
            for (DoubleWritable msg : msgIterator) {
                sum += msg.get();
            }
            vertexValue.set((0.15 / getTotalNumVertices()) + 0.85 * sum);
        }

        if (getSuperstep() >= 1 && getSuperstep() < maxIteration) {
            long edges = getNumEdges();
            msg.set(vertexValue.get() / edges);
            sendMessageToAllEdges(msg);
        } else {
            voteToHalt();
        }
    }

}
