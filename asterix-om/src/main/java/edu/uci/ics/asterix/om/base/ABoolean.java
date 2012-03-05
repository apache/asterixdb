package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public final class ABoolean implements IAObject {

    public static ABoolean TRUE = new ABoolean(true);
    public static ABoolean FALSE = new ABoolean(false);

    private final Boolean bVal;

    private ABoolean(boolean b) {
        bVal = new Boolean(b);
    }

    public Boolean getBoolean() {
        return bVal;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ABOOLEAN;
    }

    @Override
    public String toString() {
        return "ABoolean: {" + bVal + "}";
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof ABoolean)) {
            return false;
        } else {
            return bVal.equals(((ABoolean) obj).getBoolean());
        }
    }

    @Override
    public int hashCode() {
        return bVal.hashCode();
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitABoolean(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return obj == this;
    }

    @Override
    public int hash() {
        return bVal.hashCode();
    }
}
