package edu.uci.ics.pregelix.dataflow.std.base;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IUpdateFunction extends IFunction {

    /**
     * update the tuple pointed by tupleRef called after process,
     * one-input-tuple-at-a-time
     * 
     * @param tupleRef
     * @throws HyracksDataException
     */
    public void update(ITupleReference tupleRef) throws HyracksDataException;

}
