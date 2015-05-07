package edu.uci.ics.hyracks.dataflow.common.data.partition.range;

import edu.uci.ics.hyracks.data.std.api.IPointable;

public interface IRangeMap {
    public IPointable getFieldSplit(int columnIndex, int splitIndex);

    public int getSplitCount();

    public byte[] getByteArray(int columnIndex, int splitIndex);

    public int getStartOffset(int columnIndex, int splitIndex);

    public int getLength(int columnIndex, int splitIndex);

    public int getTag(int columnIndex, int splitIndex);
}
