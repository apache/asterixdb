package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public final class ABitArray implements IAObject {

    private int numberOfBits;
    private int[] intArray;

    public ABitArray(int nBits) {
        numberOfBits = nBits;
        intArray = new int[nBits / 32 + 1];
    }

    public int[] getIntArray() {
        return intArray;
    }

    public final boolean get(int index) {
        int r = index % 32;
        int q = index / 32;
        int p = 1 << r;
        return (intArray[q] & p) != 0;
    }

    public final int numberOfBits() {
        return this.numberOfBits;
    }

    public final void set(int index, boolean value) {
        int r = index % 32;
        int q = index / 32;
        int p = 1 << r;
        if (value) {
            intArray[q] |= p;
        } else {
            p = ~p;
            intArray[q] &= p;
        }
    }

    public final void setBit(int index) {
        set(index, true);
    }

    public IAType getType() {
        return BuiltinType.ABITARRAY;
    }

    public void or(ABitArray bitArray) {
        int n2 = bitArray.numberOfBits();
        int[] a2 = bitArray.getIntArray();
        int q = n2 / 32;
        for (int i = 0; i < q; i++) {
            this.intArray[i] |= a2[i];
        }
        int r = n2 % 32;
        if (r > 0) {
            int mask = 0;
            for (int i = 0; i < r; i++) {
                mask = (mask << 1) + 1;
            }
            this.intArray[q] |= a2[q] & mask;
        }
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (int i = 0; i < intArray.length; i++) {
            h = h * 31 + intArray[i];
        }
        return h;
    }

    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitABitArray(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof ABitArray)) {
            return false;
        }
        int[] x = ((ABitArray) obj).getIntArray();
        if (intArray.length != x.length) {
            return false;
        }
        for (int k = 0; k < intArray.length; k++) {
            if (intArray[k] != x[k]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hash() {
        return hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ABitArray: [ ");
        for (int i = 0; i < intArray.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(intArray[i]);
        }
        sb.append(" ]");
        return sb.toString();
    }
}
