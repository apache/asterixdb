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

    // Since AUUID is a wrapper of java.util.uuid,
    // we can use the same method that creates a UUID from a String.
    public void fromStringToAMuatbleUUID(String value) {
        String[] components = value.split("-");
        if (components.length != 5)
            throw new IllegalArgumentException("Invalid UUID string: " + value);
        for (int i = 0; i < 5; i++)
            components[i] = "0x" + components[i];

        msb = Long.decode(components[0]).longValue();
        msb <<= 16;
        msb |= Long.decode(components[1]).longValue();
        msb <<= 16;
        msb |= Long.decode(components[2]).longValue();

        lsb = Long.decode(components[3]).longValue();
        lsb <<= 48;
        lsb |= Long.decode(components[4]).longValue();
    }
}
