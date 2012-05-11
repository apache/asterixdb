package edu.uci.ics.asterix.runtime.util;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.IValueReference;

public class SimpleValueReference implements IValueReference {

    private byte[] data;
    private int start;
    private int len;

    public void reset(byte[] data, int start, int len) {
        this.data = data;
        this.start = start;
        this.len = len;
    }

    public void reset(IValueReference ivf) {
        this.data = ivf.getBytes();
        this.start = ivf.getStartIndex();
        this.len = ivf.getLength();
    }

    @Override
    public byte[] getBytes() {
        return data;
    }

    @Override
    public int getStartIndex() {
        return start;
    }

    @Override
    public int getLength() {
        return len;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IValueReference))
            return false;
        IValueReference ivf = (IValueReference) o;
        byte[] odata = ivf.getBytes();
        int ostart = ivf.getStartIndex();
        int olen = ivf.getLength();

        if (len != olen)
            return false;
        for (int i = 0; i < len; i++) {
            if (data[start + i] != odata[ostart + i])
                return false;
        }
        return true;
    }
}
