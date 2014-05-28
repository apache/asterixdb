package edu.uci.ics.pregelix.example.utils;

import java.io.IOException;

public class SerDeUtils {

    /**
     * Reads a zero-compressed encoded long from input stream and returns it.
     * 
     * @param stream
     *            Binary input stream
     * @throws java.io.IOException
     * @return deserialized long from stream.
     */
    public static long readVLong(byte[] data, int start, int length) throws IOException {
        byte firstByte = data[start];
        int len = decodeVIntSize(firstByte);
        if (len == 1) {
            return firstByte;
        }
        long i = 0;
        for (int idx = 0; idx < len - 1; idx++) {
            i = i << 8;
            i = i | (data[++start] & 0xFF);
        }
        return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
    }

    /**
     * Parse the first byte of a vint/vlong to determine the number of bytes
     * 
     * @param value
     *            the first byte of the vint/vlong
     * @return the total number of bytes (1 to 9)
     */
    public static int decodeVIntSize(byte value) {
        if (value >= -112) {
            return 1;
        } else if (value < -120) {
            return -119 - value;
        }
        return -111 - value;
    }

    /**
     * Given the first byte of a vint/vlong, determine the sign
     * 
     * @param value
     *            the first byte
     * @return is the value negative
     */
    public static boolean isNegativeVInt(byte value) {
        return value < -120 || (value >= -112 && value < 0);
    }

    /**
     * read a long value from an offset
     * 
     * @param data
     * @param offset
     * @return the long value
     */
    public static long readLong(byte[] data, int offset) {
        return (((long) data[0] << 56) + ((long) (data[1] & 255) << 48)
                + ((long) (data[2] & 255) << 40) + ((long) (data[3] & 255) << 32)
                + ((long) (data[4] & 255) << 24) + ((data[5] & 255) << 16) + ((data[6] & 255) << 8) + ((data[7] & 255) << 0));
    }

    /**
     * write a long value to a byte region
     * 
     * @param v
     * @param data
     * @param offset
     */
    public static void writeLong(long v, byte[] data, int offset) {
        data[0] = (byte) (v >>> 56);
        data[1] = (byte) (v >>> 48);
        data[2] = (byte) (v >>> 40);
        data[3] = (byte) (v >>> 32);
        data[4] = (byte) (v >>> 24);
        data[5] = (byte) (v >>> 16);
        data[6] = (byte) (v >>> 8);
        data[7] = (byte) (v >>> 0);
    }

}
