package edu.uci.ics.hyracks.storage.am.rtree.impls;


public class TupleEntry implements Comparable <TupleEntry> {
    private int tupleIndex;
    private double value;
    private final double doubleEpsilon;
    
    public TupleEntry(double doubleEpsilon) {
        this.doubleEpsilon = doubleEpsilon;
    }
    
    public int getTupleIndex() {
        return tupleIndex;
    }
    
    public void setTupleIndex(int tupleIndex) {
        this.tupleIndex = tupleIndex;
    }
    
    public double getValue() {
        return value;
    }
    
    public void setValue(double value) {
        this.value = value;
    }
    
    public double getDoubleEpsilon() {
        return doubleEpsilon;
    }

    public int compareTo(TupleEntry tupleEntry) {
        double cmp = this.getValue() - tupleEntry.getValue();
        if (cmp > getDoubleEpsilon())
            return 1;
        cmp = tupleEntry.getValue() - this.getValue();
        if (cmp > getDoubleEpsilon())
            return -1;
        return 0;
    }
}
