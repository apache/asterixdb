package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class AInt16 implements IAObject {

    protected short value;

    public AInt16(short value) {
        this.value = value;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAInt16(this);
    }

    @Override
    public IAType getType() {
        return BuiltinType.AINT16;
    }

    public short getShortValue() {
        return value;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof AInt16)) {
            return false;
        } else {
            return value == ((AInt16) obj).getShortValue();
        }
    }

    @Override
    public int hash() {
        return value;
    }

    @Override
    public String toString() {
        return "AInt16: {" + value + "}";
    }
}
