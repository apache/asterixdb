package edu.uci.ics.hyracks.dataflow.common.data.accessors;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;

public class ArrayBackedValueStorage implements IValueReference, IDataOutputProvider {
    private final ByteArrayAccessibleOutputStream baaos;
    private final DataOutputStream dos;

    public ArrayBackedValueStorage() {
        baaos = new ByteArrayAccessibleOutputStream();
        dos = new DataOutputStream(baaos);
    }

    public void reset() {
        baaos.reset();
    }

    @Override
    public DataOutput getDataOutput() {
        return dos;
    }

    @Override
    public byte[] getByteArray() {
        return baaos.getByteArray();
    }

    @Override
    public int getStartOffset() {
        return 0;
    }

    @Override
    public int getLength() {
        return baaos.size();
    }

    public void append(IValueReference value) {
        try {
            dos.write(value.getByteArray(), value.getStartOffset(), value.getLength());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void assign(IValueReference value) {
        reset();
        append(value);
    }
}