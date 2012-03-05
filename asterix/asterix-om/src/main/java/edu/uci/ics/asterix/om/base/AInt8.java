package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class AInt8 implements IAObject {

    protected byte value;

    public AInt8(Byte value) {
        this.value = value;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAInt8(this);
    }

    @Override
    public IAType getType() {
        return BuiltinType.AINT8;
    }

    public byte getByteValue() {
        return value;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof AInt8)) {
            return false;
        }
        return value == ((AInt8) obj).getByteValue();
    }

    @Override
    public int hash() {
        return value;
    }

    @Override
    public String toString() {
        return "AInt8: {" + value + "}";
    }
}
