package edu.uci.ics.hyracks.dataflow.common.data.accessors;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;

public final class FrameTupleReference implements IFrameTupleReference {
    private IFrameTupleAccessor fta;
    private int tIndex;

    public void reset(IFrameTupleAccessor fta, int tIndex) {
        this.fta = fta;
        this.tIndex = tIndex;
    }

    @Override
    public IFrameTupleAccessor getFrameTupleAccessor() {
        return fta;
    }

    @Override
    public int getTupleIndex() {
        return tIndex;
    }
}