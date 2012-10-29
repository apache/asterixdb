package edu.uci.ics.hyracks.storage.am.common.datagen;

public class SortedFloatFieldValueGenerator implements IFieldValueGenerator<Float> {
    private float val = 0.0f;

    public SortedFloatFieldValueGenerator() {
    }
    
    public SortedFloatFieldValueGenerator(float startVal) {
        val = startVal;
    }
    
    @Override
    public Float next() {
        return val++;
    }
}
