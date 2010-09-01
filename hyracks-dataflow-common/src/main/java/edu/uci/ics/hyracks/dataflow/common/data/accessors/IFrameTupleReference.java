package edu.uci.ics.hyracks.dataflow.common.data.accessors;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;

public interface IFrameTupleReference {
    public IFrameTupleAccessor getFrameTupleAccessor();

    public int getTupleIndex();
}