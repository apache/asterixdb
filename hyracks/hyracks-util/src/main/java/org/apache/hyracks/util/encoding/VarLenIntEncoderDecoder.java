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

package org.apache.hyracks.util.encoding;

import java.io.DataInput;
import java.io.IOException;

/**
 * Encodes positive integers in a variable-bytes format.
 * Each byte stores seven bits of the number. The first bit of each byte notifies if it is the last byte.
 * Specifically, if the first bit is set, then we need to shift the current value by seven and
 * continue to read the next byte util we meet a byte whose first byte is unset.
 * e.g. if the number is < 128, it will be stored using one byte and the byte value keeps as original.
 * To store the number 255 (0xff) , it will be encoded as [0x81,0x7f]. To decode that value, it reads the 0x81
 * to know that the current value is (0x81 & 0x7f)= 0x01, and the first bit tells that there are more bytes to
 * be read. When it meets 0x7f, whose first flag is unset, it knows that it is the final byte to decode.
 * Finally it will return ( 0x01 << 7) + 0x7f === 255.
 */
public class VarLenIntEncoderDecoder {
    // sometimes the dec number is easier to get the sense of how big it is.
    public static final int BOUND_ONE_BYTE = 128; // 1 << 7
    public static final int BOUND_TWO_BYTE = 16384; // 1 << 14
    public static final int BOUND_THREE_BYTE = 2097152; // 1 << 21
    public static final int BOUND_FOUR_BYTE = 268435456; // 1 << 28
    public static final int BOUND_FIVE_BYTE = Integer.MAX_VALUE;

    public static final int ENCODE_MASK = 0x0000007F;
    public static final byte CONTINUE_CHUNK = (byte) 0x80;
    public static final byte DECODE_MASK = 0x7F;

    // calculate the number of bytes needed for encoding
    public static int getBytesRequired(int length) {
        if (length < 0) {
            throw new IllegalArgumentException("The length must be an non-negative value");
        }

        int byteCount = 0;
        while (length > ENCODE_MASK) {
            length = length >>> 7;
            byteCount++;
        }
        return byteCount + 1;
    }

    public static int decode(DataInput in) throws IOException {
        int sum = 0;
        byte b = in.readByte();
        while ((b & CONTINUE_CHUNK) == CONTINUE_CHUNK) {
            sum = (sum + (b & DECODE_MASK)) << 7;
            b = in.readByte();
        }
        sum += b;
        return sum;
    }

    public static int decode(byte[] srcBytes, int startPos) {
        int sum = 0;
        while (startPos < srcBytes.length && (srcBytes[startPos] & CONTINUE_CHUNK) == CONTINUE_CHUNK) {
            sum = (sum + (srcBytes[startPos] & DECODE_MASK)) << 7;
            startPos++;
        }
        if (startPos < srcBytes.length) {
            sum += srcBytes[startPos];
        } else {
            throw new IllegalStateException("Corrupted string bytes: trying to access entry " + startPos
                    + " in a byte array of length " + srcBytes.length);
        }
        return sum;
    }

    public static int encode(int lengthVal, byte[] destBytes, int startPos) {
        if (lengthVal < 0) {
            throw new IllegalArgumentException("The length must be an non-negative value");
        }
        int nextPos = startPos;
        while (lengthVal > ENCODE_MASK) {
            destBytes[nextPos++] = (byte) (lengthVal & ENCODE_MASK);
            lengthVal = lengthVal >>> 7;
        }
        destBytes[nextPos++] = (byte) lengthVal;

        // reverse order to optimize for decoding speed
        int length = nextPos - startPos;
        int i = 0;
        for (; i < length / 2; i++) {
            byte b = destBytes[startPos + i];
            destBytes[startPos + i] = (byte) (destBytes[startPos + length - 1 - i] | CONTINUE_CHUNK);
            destBytes[startPos + length - 1 - i] = (byte) (b | CONTINUE_CHUNK);
        }
        destBytes[startPos + i] |= CONTINUE_CHUNK;
        destBytes[nextPos - 1] &= ENCODE_MASK;
        return length;
    }

    public static VarLenIntDecoder createDecoder() {
        return new VarLenIntDecoder();
    }

    // keep the stateful version for the ease of the continuously decoding behaviors.
    public static class VarLenIntDecoder {

        private byte[] bytes = null;
        private int pos = 0;

        public VarLenIntDecoder reset(byte[] bytes, int pos) {
            this.bytes = bytes;
            this.pos = pos;
            return this;
        }

        /**
         * @return the int value
         */
        public int decode() {
            int sum = 0;
            while ((bytes[pos] & CONTINUE_CHUNK) == CONTINUE_CHUNK) {
                sum = (sum + (bytes[pos] & DECODE_MASK)) << 7;
                pos++;
            }
            sum += bytes[pos++];
            return sum;
        }

        public int getPos() {
            return pos;
        }

    }

}
