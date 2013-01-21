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
            state.setCliques(cliques.getVertexes());
        } else if (cliques.getSizeOfClique() == state.getSizeOfClique()) {
            //add the new cliques
            state.getVertexes().addAll(cliques.getVertexes());
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
