package edu.uci.ics.asterix.runtime.aggregates.base;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SingleFieldFrameTupleReference implements IFrameTupleReference {

    private byte[] fieldData;
    private int start;
    private int length;
    
    public void reset(byte[] fieldData, int start, int length) {
        this.fieldData = fieldData;
        this.start = start;
        this.length = length;
    }
    
    @Override
    public int getFieldCount() {
        return 1;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return fieldData;
    }

    @Override
    public int getFieldStart(int fIdx) {
       return start;
    }

    @Override
    public int getFieldLength(int fIdx) {
       return length;
    }

    @Override
    public IFrameTupleAccessor getFrameTupleAccessor() {
        return null;
    }

    @Override
    public int getTupleIndex() {
        return 0;
    }
}
