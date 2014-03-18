package edu.uci.ics.hyracks.storage.am.common.tuples;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class DualTupleReference implements ITupleReference {

    private PermutingTupleReference permutingTuple;
    private ITupleReference tuple;

    public DualTupleReference(int[] fieldPermutation) {
        permutingTuple = new PermutingTupleReference(fieldPermutation);
    }

    @Override
    public int getFieldCount() {
        return tuple.getFieldCount();
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return tuple.getFieldData(fIdx);
    }

    @Override
    public int getFieldStart(int fIdx) {
        return tuple.getFieldStart(fIdx);
    }

    @Override
    public int getFieldLength(int fIdx) {
        return tuple.getFieldLength(fIdx);
    }

    public void reset(ITupleReference tuple) {
        this.tuple = tuple;
        permutingTuple.reset(tuple);
    }

    public ITupleReference getPermutingTuple() {
        return permutingTuple;
    }
}