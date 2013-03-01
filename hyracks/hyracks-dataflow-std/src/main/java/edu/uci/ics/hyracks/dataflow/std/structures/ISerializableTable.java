package edu.uci.ics.hyracks.dataflow.std.structures;

public interface ISerializableTable {

    public void insert(int entry, TuplePointer tuplePointer);

    public void getTuplePointer(int entry, int offset, TuplePointer tuplePointer);

    public int getFrameCount();

    public int getTupleCount();

    public void reset();

    public void close();
}
