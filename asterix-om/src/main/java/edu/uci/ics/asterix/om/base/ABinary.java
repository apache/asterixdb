package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ABinary implements IAObject {

    private static final int HASH_PREFIX = 31;

    private final byte[] bytes;

    public ABinary(byte[] byteArray) {
        this.bytes = byteArray;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitABinary(this);
    }

    @Override
    public IAType getType() {
        return BuiltinType.ABINARY;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof ABinary)) {
            return false;
        }
        byte[] x = ((ABinary) obj).getBytes();
        if (bytes.length != x.length) {
            return false;
        }
        for (int k = 0; k < bytes.length; k++) {
            if (bytes[k] != x[k]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hash() {
        int m = HASH_PREFIX <= bytes.length ? HASH_PREFIX : bytes.length;
        int h = 0;
        for (int i = 0; i < m; i++) {
            h += 31 * h + bytes[i];
        }
        return h;
    }

    @Override
    public String toString() {
        return "ABinary";
    }
}
