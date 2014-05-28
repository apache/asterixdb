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

package edu.uci.ics.pregelix.benchmark.vertex;

import java.io.IOException;

import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VLongWritable;

public class ConnectedComponentsVertex extends Vertex<VLongWritable, VLongWritable, NullWritable, VLongWritable> {
    /**
     * Propagates the smallest vertex id to all neighbors. Will always choose to
     * halt and only reactivate if a smaller id has been sent to it.
     * 
     * @param messages
     *            Iterator of messages from the previous superstep.
     * @throws IOException
     */
    @Override
    public void compute(Iterable<VLongWritable> messages) throws IOException {
        long currentComponent = getValue().get();

        // First superstep is special, because we can simply look at the neighbors
        if (getSuperstep() == 0) {
            for (Edge<VLongWritable, NullWritable> edge : getEdges()) {
                long neighbor = edge.getTargetVertexId().get();
                if (neighbor < currentComponent) {
                    currentComponent = neighbor;
                }
            }
            // Only need to send value if it is not the own id
            if (currentComponent != getValue().get()) {
                setValue(new VLongWritable(currentComponent));
                for (Edge<VLongWritable, NullWritable> edge : getEdges()) {
                    VLongWritable neighbor = edge.getTargetVertexId();
                    if (neighbor.get() > currentComponent) {
                        sendMessage(neighbor, getValue());
                    }
                }
            }

            voteToHalt();
            return;
        }

        boolean changed = false;
        // did we get a smaller id ?
        for (VLongWritable message : messages) {
            long candidateComponent = message.get();
            if (candidateComponent < currentComponent) {
                currentComponent = candidateComponent;
                changed = true;
            }
        }

        // propagate new component id to the neighbors
        if (changed) {
            setValue(new VLongWritable(currentComponent));
            sendMessageToAllEdges(getValue());
        }
        voteToHalt();
    }

    public static class MinCombiner extends Combiner<VLongWritable, VLongWritable> {

        @Override
        public void combine(VLongWritable vertexIndex, VLongWritable originalMessage, VLongWritable messageToCombine) {
            long oldValue = messageToCombine.get();
            long newValue = originalMessage.get();
            if (newValue < oldValue) {
                messageToCombine.set(newValue);
            }
        }

        @Override
        public VLongWritable createInitialMessage() {
            return new VLongWritable(Integer.MAX_VALUE);
        }

    }
}
