/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.pregelix.benchmark.vertex;

import java.io.IOException;

import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VLongWritable;

/**
 * Implementation of PageRank in which vertex ids are ints, page rank values
 * are floats, and graph is unweighted.
 */
public class PageRankVertex extends Vertex<VLongWritable, DoubleWritable, NullWritable, DoubleWritable> {
    /** Number of supersteps */
    public static final int maxSuperStep = 4;

    @Override
    public void compute(Iterable<DoubleWritable> messages) throws IOException {
        if (getSuperstep() >= 1) {
            float sum = 0;
            for (DoubleWritable message : messages) {
                sum += message.get();
            }
            getValue().set((0.15f / getTotalNumVertices()) + 0.85f * sum);
        }

        if (getSuperstep() < maxSuperStep) {
            sendMessageToAllEdges(new DoubleWritable(getValue().get() / getNumEdges()));
        } else {
            voteToHalt();
        }
    }

    public static class SumCombiner extends Combiner<VLongWritable, DoubleWritable> {

        @Override
        public void combine(VLongWritable vertexIndex, DoubleWritable originalMessage, DoubleWritable messageToCombine) {
            double oldValue = messageToCombine.get();
            messageToCombine.set(oldValue + originalMessage.get());
        }

        @Override
        public DoubleWritable createInitialMessage() {
            return new DoubleWritable(0.0);
        }

    }
}