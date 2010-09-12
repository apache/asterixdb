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

    @Override
    public int getFieldCount() {
        return fta.getFieldCount();
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return fta.getBuffer().array();
    }

    @Override
    public int getFieldStart(int fIdx) {
    	return fta.getTupleStartOffset(tIndex) + fta.getFieldSlotsLength() + fta.getFieldStartOffset(tIndex, fIdx);
    }

    @Override
    public int getFieldLength(int fIdx) {
        return fta.getFieldLength(tIndex, fIdx);
    }
}