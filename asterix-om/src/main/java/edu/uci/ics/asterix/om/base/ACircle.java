package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ACircle implements IAObject {

    protected APoint center;
    protected double radius;

    public ACircle(APoint center, double radius) {
        this.center = center;
        this.radius = radius;
    }

    public APoint getP() {
        return center;
    }

    public void setP(APoint p) {
        this.center = p;
    }

    public double getRadius() {
        return radius;
    }

    public void setRadius(double radius) {
        this.radius = radius;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitACircle(this);
    }

    @Override
    public IAType getType() {
        return BuiltinType.ACIRCLE;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof ACircle)) {
            return false;
        }
        ACircle c = (ACircle) obj;
        return radius == c.radius && center.deepEqual(c.center);
    }

    @Override
    public int hash() {
        return (int) (center.hash() + radius);
    }

    @Override
    public String toString() {
        return "ACircle: { center: " + center + ", radius: " + radius + "}";
    }
}
