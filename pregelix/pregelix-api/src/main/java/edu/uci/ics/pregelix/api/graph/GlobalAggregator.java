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
package edu.uci.ics.pregelix.api.graph;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.io.WritableSizable;

/**
 * This is the abstract class to implement for aggregating the state of all the vertices globally in the graph.
 * </p>
 * The global aggregation of vertices in a distributed cluster include two phase:
 * 1. a local phase which aggregates vertice sent from a single machine and produces
 * the partially aggregated state;
 * 2. a final phase which aggregates all partially aggregated states
 * 
 * @param <I extends Writable> vertex identifier type
 * @param <E extends Writable> vertex value type
 * @param <E extends Writable> edge type
 * @param <M extends Writable> message type
 * @param <P extends Writable>
 *        the type of the partial aggregate state
 * @param <F extends Writable> the type of the final aggregate value
 */

@SuppressWarnings("rawtypes")
public abstract class GlobalAggregator<I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable, P extends Writable, F extends Writable> {
    /**
     * initialize aggregator
     */
    public abstract void init();

    /**
     * step through all vertex at each slave partition
     * 
     * @param vertexIndex
     * @param msg
     * @throws IOException
     */
    public abstract void step(Vertex<I, V, E, M> v) throws HyracksDataException;

    /**
     * step through all intermediate aggregate result
     * 
     * @param partialResult
     *            partial aggregate value
     */
    public abstract void step(P partialResult);

    /**
     * finish partial aggregate
     * 
     * @return the final aggregate value
     */
    public abstract P finishPartial();

    /**
     * finish final aggregate
     * 
     * @return the final aggregate value
     */
    public abstract F finishFinal();
}
