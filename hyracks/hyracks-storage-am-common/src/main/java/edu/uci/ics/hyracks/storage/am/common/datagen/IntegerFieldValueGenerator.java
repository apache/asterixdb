package edu.uci.ics.hyracks.storage.am.common.datagen;

import java.util.Random;

public class IntegerFieldValueGenerator implements IFieldValueGenerator<Integer> {
    protected final Random rnd;

    public IntegerFieldValueGenerator(Random rnd) {
        this.rnd = rnd;
    }

    @Override
    public Integer next() {
        return rnd.nextInt();
    }
}
