package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class AFloat implements IAObject {

    protected float value;

    public AFloat(float value) {
        this.value = value;
    }

    public float getFloatValue() {
        return value;
    }

    @Override
    public IAType getType() {
        return BuiltinType.AFLOAT;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AFloat)) {
            return false;
        } else {
            return value == (((AFloat) o).getFloatValue());
        }
    }

    @Override
    public int hashCode() {
        return Float.floatToIntBits(value);
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAFloat(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    @Override
    public int hash() {
        return hashCode();
    }

    @Override
    public String toString() {
        return "AFloat: {" + value + "}";
    }
}
