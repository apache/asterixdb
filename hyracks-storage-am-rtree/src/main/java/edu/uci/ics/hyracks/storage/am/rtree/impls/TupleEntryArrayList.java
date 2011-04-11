package edu.uci.ics.hyracks.storage.am.rtree.impls;

import java.util.Arrays;
import java.util.Collections;

public class TupleEntryArrayList {
    private TupleEntry[] data;
    private int size;
    private final int growth;
    private final double doubleEpsilon;

    public TupleEntryArrayList(int initialCapacity, int growth, SpatialUtils spatialUtils) {
        doubleEpsilon = spatialUtils.getDoubleEpsilon();
        data = new TupleEntry[initialCapacity];
        size = 0;
        this.growth = growth;
    }

    public double getDoubleEpsilon() {
        return doubleEpsilon;
    }

    public int size() {
        return size;
    }

    public void add(int tupleIndex, double value) {
        if (size == data.length) {
            TupleEntry[] newData = new TupleEntry[data.length + growth];
            System.arraycopy(data, 0, newData, 0, data.length);
            data = newData;
        }
        if (data[size] == null) {
            data[size] = new TupleEntry(doubleEpsilon);
        }
        data[size].setTupleIndex(tupleIndex);
        data[size].setValue(value);
        size++;
    }

    public void removeLast() {
        if (size > 0)
            size--;
    }

    // WARNING: caller is responsible for checking size > 0
    public TupleEntry getLast() {
        return data[size - 1];
    }

    public TupleEntry get(int i) {
        return data[i];
    }

    public void clear() {
        size = 0;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public void sort(EntriesOrder order, int tupleCount) {
        if (order == EntriesOrder.ASCENDING) {
            Arrays.sort(data, 0, tupleCount);
        } else {
            Arrays.sort(data, 0, tupleCount, Collections.reverseOrder());
        }
    }
}
