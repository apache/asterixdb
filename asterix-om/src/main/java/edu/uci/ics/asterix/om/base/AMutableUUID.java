/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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

    // Set the most significant bits and the least significant bits.
    public void setValue(long msb, long lsb) {
        this.msb = msb;
        this.lsb = lsb;
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
