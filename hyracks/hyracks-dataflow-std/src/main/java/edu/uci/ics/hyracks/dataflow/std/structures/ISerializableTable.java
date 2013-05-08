package edu.uci.ics.hyracks.dataflow.std.structures;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface ISerializableTable {

    public void insert(int entry, TuplePointer tuplePointer) throws HyracksDataException;

    public void getTuplePointer(int entry, int offset, TuplePointer tuplePointer);

    public int getFrameCount();

    public int getTupleCount();

    public void reset();

    public void close();
}
