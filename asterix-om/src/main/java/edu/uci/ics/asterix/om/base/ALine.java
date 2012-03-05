package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ALine implements IAObject {

    protected APoint p1;
    protected APoint p2;

    public ALine(APoint p1, APoint p2) {
        this.p1 = p1;
        this.p2 = p2;
    }

    public APoint getP1() {
        return p1;
    }

    public APoint getP2() {
        return p2;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ALINE;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitALine(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof ALine)) {
            return false;
        } else {
            ALine x = (ALine) obj;
            return p1.deepEqual(x.p1) && p2.deepEqual(x.p2);
        }
    }

    @Override
    public int hash() {
        return p1.hash() + 31 * p2.hash();
    }

    @Override
    public String toString() {
        return "ALine: { p1: " + p1 + ", p2: " + p2 + "}";
    }
}