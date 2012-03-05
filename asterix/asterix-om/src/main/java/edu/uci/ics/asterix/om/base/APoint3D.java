package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class APoint3D implements IAObject {

    protected double x;
    protected double y;
    protected double z;

    public APoint3D(double x, double y, double z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public double getZ() {
        return z;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAPoint3D(this);
    }

    @Override
    public IAType getType() {
        return BuiltinType.APOINT3D;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof APoint3D)) {
            return false;
        } else {
            APoint3D p = (APoint3D) obj;
            return x == p.x && y == p.y && z == p.z;
        }
    }

    @Override
    public int hash() {
        return InMemUtils.hashDouble(x) + 31 * InMemUtils.hashDouble(y) + 31 * 31 * InMemUtils.hashDouble(z);
    }

    @Override
    public String toString() {
        return "APoint3D: { x: " + x + ", y: " + y + ", z: " + z + " }";
    }
}
