package edu.uci.ics.hyracks.storage.am.common.datagen;

public class SortedDoubleFieldValueGenerator implements IFieldValueGenerator<Double> {
    private double val = 0.0d;

    public SortedDoubleFieldValueGenerator() {
    }
    
    public SortedDoubleFieldValueGenerator(double startVal) {
        val = startVal;
    }
    
    @Override
    public Double next() {
        return val++;
    }
}
