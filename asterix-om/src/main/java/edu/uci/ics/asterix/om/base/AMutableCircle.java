package edu.uci.ics.asterix.om.base;

public class AMutableCircle extends ACircle {

    public AMutableCircle(APoint center, double radius) {
        super(center, radius);
    }

    public void setValue(APoint center, double radius) {
        this.center = center;
        this.radius = radius;
    }

}
