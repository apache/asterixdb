package edu.uci.ics.hyracks.api.comm;

import java.nio.ByteBuffer;

public interface IFrameTupleAccessor {
    public int getFieldSlotsLength();

    public int getFieldEndOffset(int tupleIndex, int fIdx);

    public int getFieldStartOffset(int tupleIndex, int fIdx);

    public int getTupleEndOffset(int tupleIndex);

    public int getTupleStartOffset(int tupleIndex);

    public int getTupleCount();

    public ByteBuffer getBuffer();

    public void reset(ByteBuffer buffer);
}