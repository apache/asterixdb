package edu.uci.ics.hyracks.algebricks.examples.piglet.types;

public class MapType extends Type {
    @Override
    public Tag getTag() {
        return Tag.MAP;
    }
}