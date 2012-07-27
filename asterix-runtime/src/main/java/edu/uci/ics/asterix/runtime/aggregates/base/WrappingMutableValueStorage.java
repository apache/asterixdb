package edu.uci.ics.asterix.runtime.aggregates.base;

import java.io.DataOutput;

import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;

public class WrappingMutableValueStorage implements IMutableValueStorage {

    private byte[] bytes;
    private int start;
    private int length;
    private DataOutput dataOutput;
    
    @Override
    public byte[] getByteArray() {
        return bytes;
    }

    @Override
    public int getStartOffset() {
        return start;
    }

    @Override
    public int getLength() {
       return length;
    }

    @Override
    public DataOutput getDataOutput() {
       return dataOutput;
    }

    @Override
    public void reset() {
        dataOutput = null;
        bytes = null;
        start = -1;
        length = -1;
    }
    
    public void wrap(DataOutput dataOutput) {
        this.dataOutput = dataOutput;
    }
    
    public void wrap(byte[] bytes, int start, int length) {
        this.bytes = bytes;
        this.start = start;
        this.length = length;
    }
}
