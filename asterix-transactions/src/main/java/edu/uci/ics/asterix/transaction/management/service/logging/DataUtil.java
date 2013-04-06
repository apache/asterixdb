/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.util.zip.CRC32;

/**
 * A utility class for doing bit level operations such as forming checksum or
 * converting between Integer and byte array. Used extensively during writing
 * and reading of logs.
 */
public class DataUtil {

    public static long getChecksum(IBuffer buffer, int offset, int length) {
        CRC32 checksumEngine = new CRC32();
        byte[] bytes = new byte[1];
        for (int i = 0; i < length; i++) {
            bytes[0] = buffer.getByte(offset + i);
            checksumEngine.update(bytes, 0, 1);
        }
        return checksumEngine.getValue();
    }

    public static int byteArrayToInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }

    public static byte[] intToByteArray(int value) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((value >>> 24) & 0xFF);
        bytes[1] = (byte) ((value >>> 16) & 0xFF);
        bytes[2] = (byte) ((value >>> 8) & 0xFF);
        bytes[3] = (byte) ((value >>> 0) & 0xFF);
        return bytes;
    }

    public static long byteArrayToLong(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 56) + ((bytes[offset + 1] & 0xff) << 48) + ((bytes[offset + 2] & 0xff) << 40)
                + ((bytes[offset + 3] & 0xff) << 32) + ((bytes[offset + 4] & 0xff) << 24)
                + ((bytes[offset + 5] & 0xff) << 16) + ((bytes[offset + 6] & 0xff) << 8)
                + ((bytes[offset + 7] & 0xff) << 0);
    }

    public static byte[] longToByteArray(long value) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) ((value >>> 56) & 0xFF);
        bytes[1] = (byte) ((value >>> 48) & 0xFF);
        bytes[2] = (byte) ((value >>> 40) & 0xFF);
        bytes[3] = (byte) ((value >>> 32) & 0xFF);
        bytes[4] = (byte) ((value >>> 24) & 0xFF);
        bytes[5] = (byte) ((value >>> 16) & 0xFF);
        bytes[6] = (byte) ((value >>> 8) & 0xFF);
        bytes[7] = (byte) ((value >>> 0) & 0xFF);
        return bytes;
    }

}