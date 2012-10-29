package edu.uci.ics.pregelix.api.util;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.Vertex;

@SuppressWarnings("rawtypes")
public class GlobalCountAggregator<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
        extends GlobalAggregator<I, V, E, M, LongWritable, LongWritable> {

    private LongWritable state = new LongWritable(0);

    @Override
    public void init() {
        state.set(0);
    }

    @Override
    public void step(Vertex<I, V, E, M> v) throws HyracksDataException {
        state.set(state.get() + 1);
    }

    @Override
    public void step(LongWritable partialResult) {
        state.set(state.get() + partialResult.get());
    }

    @Override
    public LongWritable finishPartial() {
        return state;
    }

    @Override
    public LongWritable finishFinal() {
        return state;
    }

}
