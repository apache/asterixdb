package edu.uci.ics.hyracks.storage.am.common.datagen;

import java.util.Random;

public class FloatFieldValueGenerator implements IFieldValueGenerator<Float> {
    protected final Random rnd;

    public FloatFieldValueGenerator(Random rnd) {
        this.rnd = rnd;
    }

    @Override
    public Float next() {
        return rnd.nextFloat();
    }
}
