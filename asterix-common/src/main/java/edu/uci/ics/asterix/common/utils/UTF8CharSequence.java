package edu.uci.ics.asterix.common.utils;

import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class UTF8CharSequence implements CharSequence {

    private int start;
    private int len;
    private char[] buf;

    public UTF8CharSequence(IValueReference valueRef, int start) {
        reset(valueRef, start);
    }

    public UTF8CharSequence() {
    }

    @Override
    public char charAt(int index) {
        if (index >= len || index < 0) {
            throw new IndexOutOfBoundsException("No index " + index + " for string of length " + len);
        }
        return buf[index];
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        UTF8CharSequence carSeq = new UTF8CharSequence();
        carSeq.len = end - start + 1;
        carSeq.buf = new char[carSeq.len];
        System.arraycopy(buf, start, carSeq.buf, 0, carSeq.len);
        return carSeq;
    }

    public void reset(IValueReference valueRef, int start) {
        this.start = start;
        resetLength(valueRef);
        if (buf == null || buf.length < len) {
            buf = new char[len];
        }
        int sStart = start + 2;
        int c = 0;
        int i = 0;
        byte[] bytes = valueRef.getByteArray();
        while (c < len) {
            buf[i++] = UTF8StringPointable.charAt(bytes, sStart + c);
            c += UTF8StringPointable.charSize(bytes, sStart + c);
        }

    }

    private void resetLength(IValueReference valueRef) {
        this.len = UTF8StringPointable.getUTFLength(valueRef.getByteArray(), start);
    }

}
