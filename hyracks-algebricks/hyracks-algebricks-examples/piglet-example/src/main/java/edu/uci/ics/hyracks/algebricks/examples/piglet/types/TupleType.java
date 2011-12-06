package edu.uci.ics.hyracks.algebricks.examples.piglet.types;

public class TupleType extends Type {
    @Override
    public Tag getTag() {
        return Tag.TUPLE;
    }
}