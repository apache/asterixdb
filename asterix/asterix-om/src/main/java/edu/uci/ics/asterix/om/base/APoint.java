package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class APoint implements IAObject {

    protected double x;
    protected double y;

    public APoint(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAPoint(this);
    }

    @Override
    public IAType getType() {
        return BuiltinType.APOINT;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof APoint)) {
            return false;
        } else {
            APoint p = (APoint) obj;
            return x == p.x && y == p.y;
        }
    }

    @Override
    public int hash() {
        return InMemUtils.hashDouble(x) + 31 * InMemUtils.hashDouble(y);
    }

    @Override
    public String toString() {
        return "APoint: { x: " + x + ", y: " + y + " }";
    }
}
