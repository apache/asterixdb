package edu.uci.ics.hyracks.storage.am.common.datagen;

public class SortedIntegerFieldValueGenerator implements IFieldValueGenerator<Integer> {
    private int val = 0;

    public SortedIntegerFieldValueGenerator() {
    }
    
    public SortedIntegerFieldValueGenerator(int startVal) {
        val = startVal;
    }
    
    @Override
    public Integer next() {
        return val++;
    }
}
