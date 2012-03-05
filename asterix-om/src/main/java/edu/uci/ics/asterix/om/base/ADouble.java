package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ADouble implements IAObject {

    protected double value;

    public ADouble(double value) {
        super();
        this.value = value;
    }

    public double getDoubleValue() {
        return value;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ADOUBLE;
    }

    @Override
    public String toString() {
        return "ADouble: {" + value + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ADouble))
            return false;
        return value == (((ADouble) obj).getDoubleValue());
    }

    @Override
    public int hashCode() {
        long bits = Double.doubleToLongBits(value);
        return (int) (bits ^ (bits >>> 32));
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitADouble(this);
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
