package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class AInt32 implements IAObject {

    protected int value;

    public AInt32(int value) {
        super();
        this.value = value;
    }

    public AInt32(byte[] bytes, int offset, int length) {
        value = valueFromBytes(bytes, offset, length);
    }

    public Integer getIntegerValue() {
        return value;
    }

    @Override
    public IAType getType() {
        return BuiltinType.AINT32;
    }

    @Override
    public String toString() {
        return "AInt32: {" + value + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AInt32)) {
            return false;
        } else {
            return value == (((AInt32) obj).getIntegerValue());
        }
    }

    @Override
    public int hashCode() {
        return value;
    }

    private static Integer valueFromBytes(byte[] bytes, int offset, int length) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }

    public byte[] toBytes() {
        return new byte[] { (byte) (value >>> 24), (byte) (value >> 16 & 0xff), (byte) (value >> 8 & 0xff),
                (byte) (value & 0xff) };
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAInt32(this);
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