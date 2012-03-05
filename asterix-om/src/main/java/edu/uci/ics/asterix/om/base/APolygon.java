package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class APolygon implements IAObject {

    protected APoint[] points;

    public APolygon(APoint[] points) {
        this.points = points;
    }

    public int getNumberOfPoints() {
        return points.length;
    }

    public APoint[] getPoints() {
        return points;
    }

    @Override
    public IAType getType() {
        return BuiltinType.APOLYGON;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAPolygon(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof APolygon)) {
            return false;
        } else {
            APolygon p = (APolygon) obj;
            if (p.getPoints().length != points.length) {
                return false;
            }
            for (int i = 0; i < points.length; i++) {
                if (!points[i].deepEqual(p.getPoints()[i])) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public int hash() {
        int h = 0;
        for (int i = 0; i < points.length; i++) {
            h += 31 * h + points[i].hash();
        }
        return h;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("APolygon: [ ");
        for (int i = 0; i < points.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(points[i].toString());
        }
        sb.append(" ]");
        return sb.toString();
    }
}