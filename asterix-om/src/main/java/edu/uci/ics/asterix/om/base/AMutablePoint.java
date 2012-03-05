package edu.uci.ics.asterix.om.base;

public class AMutablePoint extends APoint {

    public AMutablePoint(double x, double y) {
        super(x, y);
    }

    public void setValue(double x, double y) {
        this.x = x;
        this.y = y;
    }

}
