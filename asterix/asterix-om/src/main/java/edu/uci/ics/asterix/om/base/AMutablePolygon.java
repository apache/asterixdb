package edu.uci.ics.asterix.om.base;

public class AMutablePolygon extends APolygon {

    public AMutablePolygon(APoint[] points) {
        super(points);
    }

    public void setValue(APoint[] points) {
        this.points = points;
    }

}
