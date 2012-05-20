package edu.uci.ics.asterix.runtime.accessors;

import edu.uci.ics.asterix.runtime.accessors.base.IBinaryAccessor;

public abstract class AbstractBinaryAccessor implements IBinaryAccessor {

    private byte[] data;
    private int start;
    private int len;
    
    @Override
    public byte[] getBytes() {
        return data;
    }

    @Override
    public int getLength() {
        return len;
    }

    @Override
    public int getStartIndex() {
        return start;
    }

    @Override
    public void reset(byte[] b, int start, int len) {
        this.data = b;
        this.start = start;
        this.len = len;
    }

}
