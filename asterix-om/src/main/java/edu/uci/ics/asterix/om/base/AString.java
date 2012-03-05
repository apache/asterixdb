package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class AString implements IAObject {

    protected String value;

    public AString(String value) {
        this.value = value;
    }

    public String getStringValue() {
        return value;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ASTRING;
    }

    @Override
    public String toString() {
        return "AString: {" + value + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AString))
            return false;
        return value.equals(((AString) obj).getStringValue());
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAString(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    @Override
    public int hash() {
        return hashCode();
    }
}
