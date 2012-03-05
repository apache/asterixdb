package edu.uci.ics.asterix.om.base;

public class AMutablePoint3D extends APoint3D {

    public AMutablePoint3D(double x, double y, double z) {
        super(x, y, z);
    }

    public void setValue(double x, double y, double z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

}
