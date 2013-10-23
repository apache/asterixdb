package edu.uci.ics.asterix.om.base;

public class AMutableUUID extends AUUID {
    private final long[] uuidBits;
    private final byte[] randomBytes;

    public AMutableUUID(long msb, long lsb) {
        super(msb, lsb);
        randomBytes = new byte[16];
        uuidBits = new long[2];
    }

    public void nextUUID() {
        Holder.srnd.nextBytes(randomBytes);
        uuidBitsFromBytes(uuidBits, randomBytes);
        msb = uuidBits[0];
        lsb = uuidBits[1];
    }
}
