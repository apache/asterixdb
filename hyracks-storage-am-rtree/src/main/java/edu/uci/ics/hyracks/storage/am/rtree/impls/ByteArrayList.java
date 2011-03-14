package edu.uci.ics.hyracks.storage.am.rtree.impls;

public class ByteArrayList {
    private byte[] data;
    private int size;
    private final int growth;

    public ByteArrayList(int initialCapacity, int growth) {
        data = new byte[initialCapacity];
        size = 0;
        this.growth = growth;
    }

    public int size() {
        return size;
    }

    public void add(byte i) {
        if (size == data.length) {
            byte[] newData = new byte[data.length + growth];
            System.arraycopy(data, 0, newData, 0, data.length);
            data = newData;
        }

        data[size++] = i;
    }

    public void removeLast() {
        if (size > 0)
            size--;
    }

    // WARNING: caller is responsible for checking size > 0
    public int getLast() {
        return data[size - 1];
    }

    public int get(int i) {
        return data[i];
    }

    public void clear() {
        size = 0;
    }

    public boolean isEmpty() {
        return size == 0;
    }
}
