package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class AInt64 implements IAObject {

    protected long value;

    public AInt64(Long value) {
        this.value = value;
    }

    public long getLongValue() {
        return value;
    }

    @Override
    public IAType getType() {
        return BuiltinType.AINT64;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAInt64(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof AInt64)) {
            return false;
        }
        return value == ((AInt64) obj).getLongValue();
    }

    @Override
    public int hash() {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return "AInt64: {" + value + "}";
    }
}
