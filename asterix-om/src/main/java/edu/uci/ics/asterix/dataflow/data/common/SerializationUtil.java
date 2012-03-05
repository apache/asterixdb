package edu.uci.ics.asterix.dataflow.data.common;

public final class SerializationUtil {

    public static void writeIntToByteArray(byte[] array, int value, int offset) {
        array[offset] = (byte) (0xff & (value >> 24));
        array[offset + 1] = (byte) (0xff & (value >> 16));
        array[offset + 2] = (byte) (0xff & (value >> 8));
        array[offset + 3] = (byte) (0xff & value);
    }

    public static void writeShortToByteArray(byte[] array, short value, int offset) {
        array[offset] = (byte) (0xff & (value >> 8));
        array[offset + 1] = (byte) (0xff & value);
    }

}
