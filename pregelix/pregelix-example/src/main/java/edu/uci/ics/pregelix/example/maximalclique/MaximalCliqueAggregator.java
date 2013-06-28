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
package edu.uci.ics.pregelix.example.maximalclique;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * The global aggregator aggregates the count of triangles
 */
public class MaximalCliqueAggregator
        extends
        GlobalAggregator<VLongWritable, CliquesWritable, NullWritable, AdjacencyListWritable, CliquesWritable, CliquesWritable> {

    private CliquesWritable state = new CliquesWritable();

    @Override
    public void init() {
        state.reset();
    }

    @Override
    public void step(Vertex<VLongWritable, CliquesWritable, NullWritable, AdjacencyListWritable> v)
            throws HyracksDataException {
        CliquesWritable cliques = v.getVertexValue();
        updateAggregateState(cliques);
    }

    /**
     * Update the current aggregate state
     * 
     * @param cliques the incoming cliques
     */
    private void updateAggregateState(CliquesWritable cliques) {
        if (cliques.getSizeOfClique() > state.getSizeOfClique()) {
            //reset the vertex state
            state.reset();
            state.setCliqueSize(cliques.getSizeOfClique());
            state.addCliques(cliques);
        } else if (cliques.getSizeOfClique() == state.getSizeOfClique()) {
            //add the new cliques
            state.addCliques(cliques);
        } else {
            return;
        }
    }

    @Override
    public void step(CliquesWritable partialResult) {
        updateAggregateState(partialResult);
    }

    @Override
    public CliquesWritable finishPartial() {
        return state;
    }

    @Override
    public CliquesWritable finishFinal() {
        return state;
    }

}
