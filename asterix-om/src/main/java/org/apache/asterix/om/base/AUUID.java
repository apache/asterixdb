/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.om.base;

import java.security.SecureRandom;
import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.visitors.IOMVisitor;

public class AUUID implements IAObject {

    protected static class Holder {
        static final SecureRandom srnd = new SecureRandom();
    }

    protected long msb;
    protected long lsb;

    public AUUID(UUID uuid) {
        msb = uuid.getMostSignificantBits();
        lsb = uuid.getLeastSignificantBits();
    }

    public AUUID(long msb, long lsb) {
        this.msb = msb;
        this.lsb = lsb;
    }

    public long getMostSignificantBits() {
        return msb;
    }

    public long getLeastSignificantBits() {
        return lsb;
    }

    public static AUUID randomUUID() {
        long[] bits = new long[2];
        byte[] randomBytes = new byte[16];
        Holder.srnd.nextBytes(randomBytes);
        uuidBitsFromBytes(bits, randomBytes);
        return new AUUID(bits[0], bits[1]);
    }

    public void generateNextRandomUUID() {
        byte[] randomBytes = new byte[16];
        Holder.srnd.nextBytes(randomBytes);
        uuidBitsFromBytes(randomBytes);
    }

    protected void uuidBitsFromBytes(byte[] randomBytes) {
        this.msb = 0;
        this.lsb = 0;
        randomBytes[6] &= 0x0f; /* clear version        */
        randomBytes[6] |= 0x40; /* set to version 4     */
        randomBytes[8] &= 0x3f; /* clear variant        */
        randomBytes[8] |= 0x80; /* set to IETF variant  */
        for (int i = 0; i < 8; ++i) {
            this.msb = (this.msb << 8) | (randomBytes[i] & 0xff);
        }
        for (int i = 8; i < 16; ++i) {
            this.lsb = (this.lsb << 8) | (randomBytes[i] & 0xff);
        }
    }

    protected static void uuidBitsFromBytes(long[] bits, byte[] randomBytes) {
        bits[0] = 0;
        bits[1] = 0;
        randomBytes[6] &= 0x0f; /* clear version        */
        randomBytes[6] |= 0x40; /* set to version 4     */
        randomBytes[8] &= 0x3f; /* clear variant        */
        randomBytes[8] |= 0x80; /* set to IETF variant  */
        for (int i = 0; i < 8; ++i) {
            bits[0] = (bits[0] << 8) | (randomBytes[i] & 0xff);
        }
        for (int i = 8; i < 16; ++i) {
            bits[1] = (bits[1] << 8) | (randomBytes[i] & 0xff);
        }
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("AUUID", toString());
        return json;
    }

    @Override
    public IAType getType() {
        return BuiltinType.AUUID;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAUUID(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof AUUID)) {
            return false;
        }
        AUUID oUUID = (AUUID) obj;
        return oUUID.msb == this.msb && oUUID.lsb == this.lsb;
    }

    @Override
    public int hash() {
        long hilo = msb ^ lsb;
        return ((int) (hilo >> 32)) ^ (int) hilo;
    }

    @Override
    public String toString() {
        return "AUUID: {"
                + (digits(msb >> 32, 8) + "-" + digits(msb >> 16, 4) + "-" + digits(msb, 4) + "-"
                        + digits(lsb >> 48, 4) + "-" + digits(lsb, 12)) + "}";
    }

    public String toStringLiteralOnly() {
        return digits(msb >> 32, 8) + "-" + digits(msb >> 16, 4) + "-" + digits(msb, 4) + "-" + digits(lsb >> 48, 4)
                + "-" + digits(lsb, 12);
    }

    // Since AUUID is a wrapper of java.util.uuid,
    // we can use the same method that creates a UUID from a String.
    public static AUUID fromString(String name) {
        String[] components = name.split("-");
        if (components.length != 5)
            throw new IllegalArgumentException("Invalid UUID string: " + name);
        for (int i = 0; i < 5; i++)
            components[i] = "0x" + components[i];

        long msb = Long.decode(components[0]).longValue();
        msb <<= 16;
        msb |= Long.decode(components[1]).longValue();
        msb <<= 16;
        msb |= Long.decode(components[2]).longValue();

        long lsb = Long.decode(components[3]).longValue();
        lsb <<= 48;
        lsb |= Long.decode(components[4]).longValue();

        return new AUUID(msb, lsb);
    }

    private static String digits(long val, int digits) {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }
}
