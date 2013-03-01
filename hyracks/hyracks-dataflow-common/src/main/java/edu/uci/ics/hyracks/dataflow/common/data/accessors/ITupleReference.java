package edu.uci.ics.hyracks.dataflow.common.data.accessors;

public interface ITupleReference {
    public int getFieldCount();

    public byte[] getFieldData(int fIdx);

    public int getFieldStart(int fIdx);

    public int getFieldLength(int fIdx);
}