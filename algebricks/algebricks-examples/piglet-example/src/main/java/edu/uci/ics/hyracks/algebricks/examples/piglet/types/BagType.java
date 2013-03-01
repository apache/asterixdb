package edu.uci.ics.hyracks.algebricks.examples.piglet.types;

public class BagType extends Type {
    @Override
    public Tag getTag() {
        return Tag.BAG;
    }
}