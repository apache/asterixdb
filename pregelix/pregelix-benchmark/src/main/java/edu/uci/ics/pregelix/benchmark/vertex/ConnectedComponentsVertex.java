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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class ConnectedComponentsVertex extends Vertex<LongWritable, LongWritable, NullWritable, LongWritable> {
    /**
     * Propagates the smallest vertex id to all neighbors. Will always choose to
     * halt and only reactivate if a smaller id has been sent to it.
     * 
     * @param messages
     *            Iterator of messages from the previous superstep.
     * @throws IOException
     */
    @Override
    public void compute(Iterable<LongWritable> messages) throws IOException {
        long currentComponent = getValue().get();

        // First superstep is special, because we can simply look at the neighbors
        if (getSuperstep() == 0) {
            for (Edge<LongWritable, NullWritable> edge : getEdges()) {
                long neighbor = edge.getTargetVertexId().get();
                if (neighbor < currentComponent) {
                    currentComponent = neighbor;
                }
            }
            // Only need to send value if it is not the own id
            if (currentComponent != getValue().get()) {
                setValue(new LongWritable(currentComponent));
                for (Edge<LongWritable, NullWritable> edge : getEdges()) {
                    LongWritable neighbor = edge.getTargetVertexId();
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
        for (LongWritable message : messages) {
            long candidateComponent = message.get();
            if (candidateComponent < currentComponent) {
                currentComponent = candidateComponent;
                changed = true;
            }
        }

        // propagate new component id to the neighbors
        if (changed) {
            setValue(new LongWritable(currentComponent));
            sendMessageToAllEdges(getValue());
        }
        voteToHalt();
    }
}
