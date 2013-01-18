package edu.uci.ics.pregelix.example;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * The global aggregator aggregates the count of triangles
 */
public class TriangleCountingAggregator extends
        GlobalAggregator<VLongWritable, VLongWritable, VLongWritable, VLongWritable, VLongWritable, VLongWritable> {

    private VLongWritable state = new VLongWritable(0);

    @Override
    public void init() {
        state.set(0);
    }

    @Override
    public void step(Vertex<VLongWritable, VLongWritable, VLongWritable, VLongWritable> v) throws HyracksDataException {
        state.set(state.get() + v.getVertexValue().get());
    }

    @Override
    public void step(VLongWritable partialResult) {
        state.set(state.get() + partialResult.get());
    }

    @Override
    public VLongWritable finishPartial() {
        return state;
    }

    @Override
    public VLongWritable finishFinal() {
        return state;
    }

}
