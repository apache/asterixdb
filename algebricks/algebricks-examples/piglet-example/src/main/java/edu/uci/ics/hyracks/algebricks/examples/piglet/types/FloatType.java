package edu.uci.ics.hyracks.algebricks.examples.piglet.types;

public class FloatType extends Type {
    public static final Type INSTANCE = new FloatType();

    private FloatType() {
    }

    @Override
    public Tag getTag() {
        return Tag.FLOAT;
    }
}