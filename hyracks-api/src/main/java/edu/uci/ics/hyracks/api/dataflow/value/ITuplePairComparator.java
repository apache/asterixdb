package edu.uci.ics.hyracks.api.dataflow.value;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface ITuplePairComparator {

    public int compare(IFrameTupleAccessor outerRef, int outerIndex, IFrameTupleAccessor innerRef, int innerIndex)
            throws HyracksDataException;

}
