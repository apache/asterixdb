package edu.uci.ics.pregelix.api.graph;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

@SuppressWarnings("rawtypes")
public abstract class GlobalAggregator<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable, P extends Writable, F extends Writable> {
    /**
     * initialize combiner
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
     * @param partialResult partial aggregate value
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
