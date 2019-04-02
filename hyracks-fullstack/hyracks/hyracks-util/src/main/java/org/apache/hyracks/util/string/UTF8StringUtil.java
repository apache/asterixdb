/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.util.string;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;

import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

/**
 * A helper package to operate the UTF8String in Hyracks.
 * Most of the codes were migrated from asterix-fuzzyjoin and hyracks-storage-am-invertedindex
 */
public class UTF8StringUtil {

    private UTF8StringUtil() {
    }

    public static char charAt(byte[] b, int s) {
        if (s >= b.length) {
            throw new ArrayIndexOutOfBoundsException(s);
        }
        int c = b[s] & 0xff;
        switch (c >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                return (char) c;

            case 12:
            case 13:
                return (char) (((c & 0x1F) << 6) | ((b[s + 1]) & 0x3F));

            case 14:
                return (char) (((c & 0x0F) << 12) | (((b[s + 1]) & 0x3F) << 6) | (b[s + 2] & 0x3F));

            default:
                throw new IllegalArgumentException();
        }
    }

    public static int charSize(byte[] b, int s) {
        int c = b[s] & 0xff;
        switch (c >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                return 1;

            case 12:
            case 13:
                return 2;

            case 14:
                return 3;

            default:
                throw new IllegalStateException();
        }
    }

    public static boolean isCharStart(byte[] b, int s) {
        int c = b[s] & 0xff;
        return (c >> 6) != 2;
    }

    public static int getModifiedUTF8Len(char c) {
        if (c >= 0x0001 && c <= 0x007F) {
            return 1;
        } else if (c <= 0x07FF) {
            return 2;
        } else {
            return 3;
        }
    }

    public static int writeCharAsModifiedUTF8(char c, DataOutput dos) throws IOException {
        if (c >= 0x0001 && c <= 0x007F) {
            dos.writeByte(c);
            return 1;
        } else if (c <= 0x07FF) {
            dos.writeByte((byte) (0xC0 | ((c >> 6) & 0x3F)));
            dos.writeByte((byte) (0x80 | (c & 0x3F)));
            return 2;
        } else {
            dos.writeByte((byte) (0xE0 | ((c >> 12) & 0x0F)));
            dos.writeByte((byte) (0x80 | ((c >> 6) & 0x3F)));
            dos.writeByte((byte) (0x80 | (c & 0x3F)));
            return 3;
        }
    }

    public static int writeCharAsModifiedUTF8(char c, OutputStream dos) throws IOException {
        if (c >= 0x0001 && c <= 0x007F) {
            dos.write(c);
            return 1;
        } else if (c <= 0x07FF) {
            dos.write((byte) (0xC0 | ((c >> 6) & 0x3F)));
            dos.write((byte) (0x80 | (c & 0x3F)));
            return 2;
        } else {
            dos.write((byte) (0xE0 | ((c >> 12) & 0x0F)));
            dos.write((byte) (0x80 | ((c >> 6) & 0x3F)));
            dos.write((byte) (0x80 | (c & 0x3F)));
            return 3;
        }
    }

    public static int getStringLength(byte[] b, int s) {
        int len = getUTFLength(b, s);
        int pos = s + getNumBytesToStoreLength(len);
        int end = pos + len;
        int charCount = 0;
        while (pos < end) {
            charCount++;
            pos += charSize(b, pos);
        }
        return charCount;
    }

    public static int getUTFLength(byte[] b, int s) {
        return VarLenIntEncoderDecoder.decode(b, s);
    }

    public static int getNumBytesToStoreLength(int strlen) {
        return VarLenIntEncoderDecoder.getBytesRequired(strlen);
    }

    public static int UTF8ToCodePoint(byte[] b, int s) {
        if (b[s] >> 7 == 0) {
            // 1 byte
            return b[s];
        } else if ((b[s] & 0xe0) == 0xc0) { /*0xe0 = 0b1110000*/
            // 2 bytes
            return (b[s] & 0x1f) << 6 | /*0x3f = 0b00111111*/
                    (b[s + 1] & 0x3f);
        } else if ((b[s] & 0xf0) == 0xe0) {
            // 3bytes
            return (b[s] & 0xf) << 12 | (b[s + 1] & 0x3f) << 6 | (b[s + 2] & 0x3f);
        } else if ((b[s] & 0xf8) == 0xf0) {
            // 4bytes
            return (b[s] & 0x7) << 18 | (b[s + 1] & 0x3f) << 12 | (b[s + 2] & 0x3f) << 6 | (b[s + 3] & 0x3f);
        } else if ((b[s] & 0xfc) == 0xf8) {
            // 5bytes
            return (b[s] & 0x3) << 24 | (b[s + 1] & 0x3f) << 18 | (b[s + 2] & 0x3f) << 12 | (b[s + 3] & 0x3f) << 6
                    | (b[s + 4] & 0x3f);
        } else if ((b[s] & 0xfe) == 0xfc) {
            // 6bytes
            return (b[s] & 0x1) << 30 | (b[s + 1] & 0x3f) << 24 | (b[s + 2] & 0x3f) << 18 | (b[s + 3] & 0x3f) << 12
                    | (b[s + 4] & 0x3f) << 6 | (b[s + 5] & 0x3f);
        }
        return 0;
    }

    public static int codePointToUTF8(int c, byte[] outputUTF8) {
        if (c < 0x80) {
            outputUTF8[0] = (byte) (c & 0x7F /* mask 7 lsb: 0b1111111 */);
            return 1;
        } else if (c < 0x0800) {
            outputUTF8[0] = (byte) (c >> 6 & 0x1F | 0xC0);
            outputUTF8[1] = (byte) (c & 0x3F | 0x80);
            return 2;
        } else if (c < 0x010000) {
            outputUTF8[0] = (byte) (c >> 12 & 0x0F | 0xE0);
            outputUTF8[1] = (byte) (c >> 6 & 0x3F | 0x80);
            outputUTF8[2] = (byte) (c & 0x3F | 0x80);
            return 3;
        } else if (c < 0x200000) {
            outputUTF8[0] = (byte) (c >> 18 & 0x07 | 0xF0);
            outputUTF8[1] = (byte) (c >> 12 & 0x3F | 0x80);
            outputUTF8[2] = (byte) (c >> 6 & 0x3F | 0x80);
            outputUTF8[3] = (byte) (c & 0x3F | 0x80);
            return 4;
        } else if (c < 0x4000000) {
            outputUTF8[0] = (byte) (c >> 24 & 0x03 | 0xF8);
            outputUTF8[1] = (byte) (c >> 18 & 0x3F | 0x80);
            outputUTF8[2] = (byte) (c >> 12 & 0x3F | 0x80);
            outputUTF8[3] = (byte) (c >> 6 & 0x3F | 0x80);
            outputUTF8[4] = (byte) (c & 0x3F | 0x80);
            return 5;
        } else if (c < 0x80000000) {
            outputUTF8[0] = (byte) (c >> 30 & 0x01 | 0xFC);
            outputUTF8[1] = (byte) (c >> 24 & 0x3F | 0x80);
            outputUTF8[2] = (byte) (c >> 18 & 0x3F | 0x80);
            outputUTF8[3] = (byte) (c >> 12 & 0x3F | 0x80);
            outputUTF8[4] = (byte) (c >> 6 & 0x3F | 0x80);
            outputUTF8[5] = (byte) (c & 0x3F | 0x80);
            return 6;
        }
        return 0;
    }

    /**
     * Compute the normalized key of the UTF8 string.
     * The normalized key in Hyracks is mainly used to speedup the comparison between pointable data.
     * In the UTF8StringPTR case, we compute the integer value by using the first 2 chars.
     * The comparator will first use this integer to get the result ( <,>, or =), it will check
     * the actual bytes only if the normalized key is equal. Thus this normalized key must be
     * consistent with the comparison result.
     */
    public static int normalize(byte[] bytes, int start) {
        int len = getUTFLength(bytes, start);
        long nk = 0;
        int offset = start + getNumBytesToStoreLength(len);
        for (int i = 0; i < 2; ++i) {
            nk <<= 16;
            if (i < len) {
                nk += (charAt(bytes, offset)) & 0xffff;
                offset += charSize(bytes, offset);
            }
        }
        return (int) (nk >> 1); // make it always positive.
    }

    public static int compareTo(byte[] thisBytes, int thisStart, byte[] thatBytes, int thatStart) {
        return compareTo(thisBytes, thisStart, thatBytes, thatStart, false, false);
    }

    // the start and length of each are the ones calculated by UTF8StringPointable. caller should provide proper values
    public static int compareTo(byte[] thisBytes, int thisStart, int thisLength, byte[] thatBytes, int thatStart,
            int thatLength) {
        return compareTo(thisBytes, thisStart, thisLength, thatBytes, thatStart, thatLength, false, false);
    }

    /**
     * This function provides the raw bytes-based comparison for UTF8 strings.
     * Note that the comparison may not deliver the correct ordering for certain languages that include 2 or 3 bytes characters.
     * But it works for single-byte character languages.
     */
    public static int rawByteCompareTo(byte[] thisBytes, int thisStart, byte[] thatBytes, int thatStart) {
        return compareTo(thisBytes, thisStart, thatBytes, thatStart, false, true);
    }

    public static int lowerCaseCompareTo(byte[] thisBytes, int thisStart, byte[] thatBytes, int thatStart) {
        return compareTo(thisBytes, thisStart, thatBytes, thatStart, true, false);
    }

    // Certain type of string does not include lengthByte in the beginning and
    // the length of the given string is given explicitly as a parameter. (e.g., token in a string)
    public static int lowerCaseCompareTo(byte[] thisBytes, int thisStart, int thisLength, byte[] thatBytes,
            int thatStart, int thatLength) {
        return compareTo(thisBytes, thisStart, thisLength, thatBytes, thatStart, thatLength, true, false);
    }

    public static int hash(byte[] bytes, int start, int coefficient, int r) {
        return hash(bytes, start, false, false, coefficient, r);
    }

    public static int hash(byte[] bytes, int start) {
        return hash(bytes, start, false, false, 31, Integer.MAX_VALUE);
    }

    private static int hash(byte[] bytes, int start, boolean useLowerCase, boolean useRawByte, int coefficient, int r) {
        int utflen = getUTFLength(bytes, start);
        int sStart = start + getNumBytesToStoreLength(utflen);
        return hash(bytes, sStart, utflen, useLowerCase, useRawByte, coefficient, r);
    }

    /**
     * This function provides the raw bytes-based hash function for UTF8 strings.
     * Note that the hash values may not deliver the correct ordering for certain languages that include 2 or 3 bytes characters.
     * But it works for single-byte character languages.
     */
    public static int rawBytehash(byte[] bytes, int start) {
        return hash(bytes, start, false, true, 31, Integer.MAX_VALUE);
    }

    public static int lowerCaseHash(byte[] bytes, int start) {
        return hash(bytes, start, true, false, 31, Integer.MAX_VALUE);
    }

    // Certain type of string does not include lengthByte in the beginning and
    // the length of the given string is given explicitly as a parameter.
    public static int lowerCaseHash(byte[] bytes, int start, int length) {
        return hash(bytes, start, length, true, false, 31, Integer.MAX_VALUE);
    }

    public static String toString(byte[] bytes, int start) {
        StringBuilder builder = new StringBuilder();
        return toString(builder, bytes, start).toString();
    }

    public static StringBuilder toString(StringBuilder builder, byte[] bytes, int start) {
        int utfLen = getUTFLength(bytes, start);
        int offset = getNumBytesToStoreLength(utfLen);
        while (utfLen > 0) {
            char c = charAt(bytes, start + offset);
            builder.append(c);
            int cLen = getModifiedUTF8Len(c);
            offset += cLen;
            utfLen -= cLen;
        }
        return builder;
    }

    public static void printUTF8StringWithQuotes(byte[] b, int s, int l, OutputStream os) throws IOException {
        printUTF8String(b, s, l, os, true);
    }

    public static void printUTF8StringNoQuotes(byte[] b, int s, int l, OutputStream os) throws IOException {
        printUTF8String(b, s, l, os, false);
    }

    public static void printUTF8StringWithQuotes(String str, OutputStream os) throws IOException {
        printUTF8String(str, os, true);
    }

    public static void printUTF8StringNoQuotes(String str, OutputStream os) throws IOException {
        printUTF8String(str, os, false);
    }

    public static int encodeUTF8Length(int length, byte[] bytes, int start) {
        return VarLenIntEncoderDecoder.encode(length, bytes, start);
    }

    public static int writeUTF8Length(int length, byte[] bytes, DataOutput out) throws IOException {
        int nbytes = encodeUTF8Length(length, bytes, 0);
        out.write(bytes, 0, nbytes);
        return nbytes;
    }

    private static void printUTF8String(byte[] b, int s, int l, OutputStream os, boolean useQuotes) throws IOException {
        int stringLength = getUTFLength(b, s);
        int position = s + getNumBytesToStoreLength(stringLength);
        int maxPosition = position + stringLength;
        if (useQuotes) {
            os.write('\"');
        }
        while (position < maxPosition) {
            char c = charAt(b, position);
            if (c == '\\' || c == '"') {
                // escape
                os.write('\\');
            }
            int sz = charSize(b, position);
            while (sz > 0) {
                os.write(b[position]);
                position++;
                sz--;
            }
        }
        if (useQuotes) {
            os.write('\"');
        }
    }

    private static void printUTF8String(String string, OutputStream os, boolean useQuotes) throws IOException {
        if (useQuotes) {
            os.write('\"');
        }
        for (int i = 0; i < string.length(); i++) {
            char ch = string.charAt(i);
            writeCharAsModifiedUTF8(ch, os);
        }
        if (useQuotes) {
            os.write('\"');
        }
    }

    private static int compareTo(byte[] thisBytes, int thisStart, byte[] thatBytes, int thatStart, boolean useLowerCase,
            boolean useRawByte) {
        int thisLength = getUTFLength(thisBytes, thisStart);
        int thatLength = getUTFLength(thatBytes, thatStart);
        int thisActualStart = thisStart + getNumBytesToStoreLength(thisLength);
        int thatActualStart = thatStart + getNumBytesToStoreLength(thatLength);
        return compareTo(thisBytes, thisActualStart, thisLength, thatBytes, thatActualStart, thatLength, useLowerCase,
                useRawByte);
    }

    private static int compareTo(byte[] thisBytes, int thisActualStart, int thisLength, byte[] thatBytes,
            int thatActualStart, int thatLength, boolean useLowerCase, boolean useRawByte) {
        int c1 = 0;
        int c2 = 0;

        while (c1 < thisLength && c2 < thatLength) {
            char ch1, ch2;
            if (useRawByte) {
                ch1 = (char) thisBytes[thisActualStart + c1];
                ch2 = (char) thatBytes[thatActualStart + c2];
            } else {
                ch1 = charAt(thisBytes, thisActualStart + c1);
                ch2 = charAt(thatBytes, thatActualStart + c2);

                if (useLowerCase) {
                    ch1 = Character.toLowerCase(ch1);
                    ch2 = Character.toLowerCase(ch2);
                }
            }

            if (ch1 != ch2) {
                return ch1 - ch2;
            }
            c1 += charSize(thisBytes, thisActualStart + c1);
            c2 += charSize(thatBytes, thatActualStart + c2);
        }
        return thisLength - thatLength;
    }

    private static int hash(byte[] bytes, int start, int length, boolean useLowerCase, boolean useRawByte,
            int coefficient, int r) {
        int h = 0;
        int c = 0;

        while (c < length) {
            char ch;
            if (useRawByte) {
                ch = (char) bytes[start + c];
            } else {
                ch = charAt(bytes, start + c);
                if (useLowerCase) {
                    ch = Character.toLowerCase(ch);
                }
            }
            h = (coefficient * h + ch) % r;
            c += charSize(bytes, start + c);
        }
        return h;
    }

    public static byte[] writeStringToBytes(String string) {
        UTF8StringWriter writer = new UTF8StringWriter();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try {
            writer.writeUTF8(string, dos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return bos.toByteArray();
    }

    /**
     * Reads from the
     * stream <code>in</code> a representation
     * of a Unicode character string encoded in
     * <a href="DataInput.html#modified-utf-8">modified UTF-8</a> format;
     * this string of characters is then returned as a <code>String</code>.
     * The details of the modified UTF-8 representation
     * are exactly the same as for the <code>readUTF</code>
     * method of <code>DataInput</code>.
     *
     * @param in
     *            a data input stream.
     * @return a Unicode string.
     * @throws EOFException
     *             if the input stream reaches the end
     *             before all the bytes.
     * @throws IOException
     *             the stream has been closed and the contained
     *             input stream does not support reading after close, or
     *             another I/O error occurs.
     * @throws UTFDataFormatException
     *             if the bytes do not represent a
     *             valid modified UTF-8 encoding of a Unicode string.
     * @see java.io.DataInputStream#readUnsignedShort()
     */
    public static String readUTF8(DataInput in) throws IOException {
        return readUTF8(in, null);
    }

    public static String readUTF8(DataInput in, UTF8StringReader reader) throws IOException {
        int utflen = VarLenIntEncoderDecoder.decode(in);
        byte[] bytearr;
        char[] chararr;

        if (reader == null) {
            bytearr = new byte[utflen * 2];
            chararr = new char[utflen * 2];
        } else {
            if (reader.bytearr == null || reader.bytearr.length < utflen) {
                reader.bytearr = new byte[utflen * 2];
                reader.chararr = new char[utflen * 2];
            }
            bytearr = reader.bytearr;
            chararr = reader.chararr;
        }

        int c, char2, char3;
        int count = 0;
        int chararr_count = 0;

        in.readFully(bytearr, 0, utflen);

        while (count < utflen) {
            c = bytearr[count] & 0xff;
            if (c > 127) {
                break;
            }
            count++;
            chararr[chararr_count++] = (char) c;
        }

        while (count < utflen) {
            c = bytearr[count] & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    /* 0xxxxxxx*/
                    count++;
                    chararr[chararr_count++] = (char) c;
                    break;
                case 12:
                case 13:
                    /* 110x xxxx   10xx xxxx*/
                    count += 2;
                    if (count > utflen) {
                        throw new UTFDataFormatException("malformed input: partial character at end");
                    }
                    char2 = bytearr[count - 1];
                    if ((char2 & 0xC0) != 0x80) {
                        throw new UTFDataFormatException("malformed input around byte " + count);
                    }
                    chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
                    count += 3;
                    if (count > utflen) {
                        throw new UTFDataFormatException("malformed input: partial character at end");
                    }
                    char2 = bytearr[count - 2];
                    char3 = bytearr[count - 1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                        throw new UTFDataFormatException("malformed input around byte " + (count - 1));
                    }
                    chararr[chararr_count++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | (char3 & 0x3F));
                    break;
                default:
                    /* 10xx xxxx,  1111 xxxx */
                    throw new UTFDataFormatException("malformed input around byte " + count);
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararr_count);
    }

    /**
     * Write a UTF8 String <code>str</code> into the DataOutput <code>out</code>
     *
     * @param str,
     *            a Unicode string;
     * @param out,
     *            a Data output stream.
     * @throws IOException
     */
    public static void writeUTF8(CharSequence str, DataOutput out) throws IOException {
        writeUTF8(str, out, null);
    }

    public static void writeUTF8(CharSequence str, DataOutput out, UTF8StringWriter writer) throws IOException {
        int strlen = str.length();
        int utflen = 0;
        char c;
        int count = 0;

        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            utflen += UTF8StringUtil.getModifiedUTF8Len(c);
        }

        byte[] tempBytes = getTempBytes(writer, utflen);
        count += VarLenIntEncoderDecoder.encode(utflen, tempBytes, count);
        int i = 0;
        for (; i < strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) {
                break;
            }
            tempBytes[count++] = (byte) c;
        }

        for (; i < strlen; i++) {
            c = str.charAt(i);
            count += writeToBytes(tempBytes, count, c);
        }
        out.write(tempBytes, 0, count);
    }

    public static void writeUTF8(char[] buffer, int start, int length, DataOutput out, UTF8StringWriter writer)
            throws IOException {
        int utflen = 0;
        int count = 0;
        char c;

        for (int i = 0; i < length; i++) {
            c = buffer[i + start];
            utflen += UTF8StringUtil.getModifiedUTF8Len(c);
        }

        byte[] tempBytes = getTempBytes(writer, utflen);
        count += VarLenIntEncoderDecoder.encode(utflen, tempBytes, count);

        int i = 0;
        for (; i < length; i++) {
            c = buffer[i + start];
            if (!((c >= 0x0001) && (c <= 0x007F))) {
                break;
            }
            tempBytes[count++] = (byte) c;
        }

        for (; i < length; i++) {
            c = buffer[i + start];
            count += writeToBytes(tempBytes, count, c);
        }
        out.write(tempBytes, 0, count);
    }

    private static int writeToBytes(byte[] tempBytes, int count, char c) {
        int orig = count;
        if ((c >= 0x0001) && (c <= 0x007F)) {
            tempBytes[count++] = (byte) c;
        } else if (c > 0x07FF) {
            tempBytes[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
            tempBytes[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
            tempBytes[count++] = (byte) (0x80 | (c & 0x3F));
        } else {
            tempBytes[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
            tempBytes[count++] = (byte) (0x80 | (c & 0x3F));
        }
        return count - orig;
    }

    private static byte[] getTempBytes(UTF8StringWriter writer, int utflen) {
        byte[] tempBytes;
        if (writer == null) {
            tempBytes = new byte[utflen + 5];
        } else {
            if (writer.tempBytes == null || writer.tempBytes.length < utflen + 5) {
                writer.tempBytes = new byte[utflen + 5];
            }
            tempBytes = writer.tempBytes;
        }
        return tempBytes;
    }
}
