package edu.uci.ics.hyracks.storage.am.common.datagen;

import java.util.Random;

public class DoubleFieldValueGenerator implements IFieldValueGenerator<Double> {
    protected final Random rnd;

    public DoubleFieldValueGenerator(Random rnd) {
        this.rnd = rnd;
    }

    @Override
    public Double next() {
        return rnd.nextDouble();
    }
}
