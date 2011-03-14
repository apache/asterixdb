package edu.uci.ics.hyracks.storage.am.rtree.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;

public class RTreeSplitKey {
    public byte[] data = null;
    public ByteBuffer buf = null;
    public ITreeIndexTupleReference tuple;
    public int keySize = 0;

    public RTreeSplitKey(ITreeIndexTupleReference tuple) {
        this.tuple = tuple;
    }

    public void initData(int keySize) {
        // try to reuse existing memory from a lower-level split if possible
        this.keySize = keySize;
        if (data != null) {
            if (data.length < keySize + 4) {
                data = new byte[keySize + 4]; // add 4 for the page
                buf = ByteBuffer.wrap(data);
            }
        } else {
            data = new byte[keySize + 4]; // add 4 for the page
            buf = ByteBuffer.wrap(data);
        }

        tuple.resetByTupleOffset(buf, 0);
    }

    public void reset() {
        data = null;
        buf = null;
    }

    public ByteBuffer getBuffer() {
        return buf;
    }

    public ITreeIndexTupleReference getTuple() {
        return tuple;
    }

    public int getPage() {
        return buf.getInt(keySize);
    }

    public void setPage(int Page) {
        buf.putInt(keySize, Page);
    }

    public RTreeSplitKey duplicate(ITreeIndexTupleReference copyTuple) {
        RTreeSplitKey copy = new RTreeSplitKey(copyTuple);
        copy.data = data.clone();
        copy.buf = ByteBuffer.wrap(copy.data);
        copy.tuple.setFieldCount(tuple.getFieldCount());
        copy.tuple.resetByTupleOffset(copy.buf, 0);
        return copy;
    }
}