package edu.uci.ics.asterix.runtime.aggregates.serializable.std;

public class BufferSerDeUtil {

    public static double getDouble(byte[] bytes, int offset) {
        return Double.longBitsToDouble(getLong(bytes, offset));
    }

    public static float getFloat(byte[] bytes, int offset) {
        return Float.intBitsToFloat(getInt(bytes, offset));
    }

    public static boolean getBoolean(byte[] bytes, int offset) {
        if (bytes[offset] == 0)
            return false;
        else
            return true;
    }

    public static int getInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }

    public static long getLong(byte[] bytes, int offset) {
        return (((long) (bytes[offset] & 0xff)) << 56) + (((long) (bytes[offset + 1] & 0xff)) << 48)
                + (((long) (bytes[offset + 2] & 0xff)) << 40) + (((long) (bytes[offset + 3] & 0xff)) << 32)
                + (((long) (bytes[offset + 4] & 0xff)) << 24) + (((long) (bytes[offset + 5] & 0xff)) << 16)
                + (((long) (bytes[offset + 6] & 0xff)) << 8) + (((long) (bytes[offset + 7] & 0xff)) << 0);
    }

    public static void writeBoolean(boolean value, byte[] bytes, int offset) {
        if (value)
            bytes[offset] = (byte) 1;
        else
            bytes[offset] = (byte) 0;
    }

    public static void writeInt(int value, byte[] bytes, int offset) {
        bytes[offset++] = (byte) (value >> 24);
        bytes[offset++] = (byte) (value >> 16);
        bytes[offset++] = (byte) (value >> 8);
        bytes[offset++] = (byte) (value);
    }

    public static void writeLong(long value, byte[] bytes, int offset) {
        bytes[offset++] = (byte) (value >> 56);
        bytes[offset++] = (byte) (value >> 48);
        bytes[offset++] = (byte) (value >> 40);
        bytes[offset++] = (byte) (value >> 32);
        bytes[offset++] = (byte) (value >> 24);
        bytes[offset++] = (byte) (value >> 16);
        bytes[offset++] = (byte) (value >> 8);
        bytes[offset++] = (byte) (value);
    }

    public static void writeDouble(double value, byte[] bytes, int offset) {
        long lValue = Double.doubleToLongBits(value);
        writeLong(lValue, bytes, offset);
    }

    public static void writeFloat(float value, byte[] bytes, int offset) {
        int iValue = Float.floatToIntBits(value);
        writeInt(iValue, bytes, offset);
    }

}
