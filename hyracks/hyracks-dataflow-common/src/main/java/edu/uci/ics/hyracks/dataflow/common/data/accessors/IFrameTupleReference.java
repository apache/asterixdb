package edu.uci.ics.hyracks.dataflow.common.data.accessors;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;

public interface IFrameTupleReference extends ITupleReference {
    public IFrameTupleAccessor getFrameTupleAccessor();

    public int getTupleIndex();
}