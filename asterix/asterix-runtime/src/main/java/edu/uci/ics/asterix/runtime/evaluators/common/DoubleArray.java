package edu.uci.ics.asterix.runtime.evaluators.common;

import java.util.Arrays;

public class DoubleArray {
    private static final int SIZE = 1;

    private double[] data;
    private int length;

    public DoubleArray() {
        data = new double[SIZE];
        length = 0;
    }

    public void add(double d) {
        if (length == data.length) {
            data = Arrays.copyOf(data, data.length << 1);
        }
        data[length++] = d;
    }

    public double[] get() {
        return data;
    }

    public double get(int i) {
        return data[i];
    }

    public int length() {
        return length;
    }

    public void reset() {
        length = 0;
    }

    public void sort() {
        sort(0, length);
    }

    public void sort(int start, int end) {
        Arrays.sort(data, start, end);
    }

    @Override
    public String toString() {
        StringBuilder out = new StringBuilder();
        out.append('[');
        for (int i = 0; i < length; ++i) {
            out.append(data[i]);
            if (i < length - 1) {
                out.append(',');
                out.append(' ');
            }
        }
        out.append(']');
        return out.toString();
    }
}
