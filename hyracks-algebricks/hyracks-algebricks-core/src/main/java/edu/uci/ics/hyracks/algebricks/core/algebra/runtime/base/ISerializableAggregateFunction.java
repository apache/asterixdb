package edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base;

import java.io.DataOutput;

import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public interface ISerializableAggregateFunction {
    /**
     * initialize the space occupied by internal state
     * 
     * @param state
     * @throws AlgebricksException
     * @return length of the intermediate state
     */
    public void init(DataOutput state) throws AlgebricksException;

    /**
     * update the internal state
     * 
     * @param tuple
     * @param state
     * @throws AlgebricksException
     */
    public void step(IFrameTupleReference tuple, byte[] data, int start, int len) throws AlgebricksException;

    /**
     * output the state to result
     * 
     * @param state
     * @param result
     * @throws AlgebricksException
     */
    public void finish(byte[] data, int start, int len, DataOutput result) throws AlgebricksException;

    /**
     * output the partial state to partial result
     * 
     * @param state
     * @param partialResult
     * @throws AlgebricksException
     */
    public void finishPartial(byte[] data, int start, int len, DataOutput partialResult) throws AlgebricksException;
}
