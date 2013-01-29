package edu.uci.ics.hyracks.data.std.util;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

public class ArrayBackedValueStorage implements IMutableValueStorage {
   
    private final GrowableArray data = new GrowableArray();

    @Override
    public void reset() {
        data.reset();
    }

    @Override
    public DataOutput getDataOutput() {
        return data.getDataOutput();
    }

    @Override
    public byte[] getByteArray() {
        return data.getByteArray();
    }

    @Override
    public int getStartOffset() {
        return 0;
    }

    @Override
    public int getLength() {
        return data.getLength();
    }

    public void append(IValueReference value) {
        try {
            data.append(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void assign(IValueReference value) {
        reset();
        append(value);
    }
}