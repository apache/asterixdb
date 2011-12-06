package edu.uci.ics.hyracks.algebricks.examples.piglet.types;

public class CharArrayType extends Type {
    public static final Type INSTANCE = new CharArrayType();

    private CharArrayType() {
    }

    @Override
    public Tag getTag() {
        return Tag.CHAR_ARRAY;
    }
}