package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class AInt64SerializerDeserializer implements ISerializerDeserializer<AInt64> {

    private static final long serialVersionUID = 1L;

    public static final AInt64SerializerDeserializer INSTANCE = new AInt64SerializerDeserializer();

    private AInt64SerializerDeserializer() {
    }

    @Override
    public AInt64 deserialize(DataInput in) throws HyracksDataException {
        try {
            return new AInt64(in.readLong());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }

    }

    @Override
    public void serialize(AInt64 instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeLong(instance.getLongValue());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    public static long getLong(byte[] bytes, int offset) {
        return (((long) (bytes[offset] & 0xff)) << 56) + (((long) (bytes[offset + 1] & 0xff)) << 48)
                + (((long) (bytes[offset + 2] & 0xff)) << 40) + (((long) (bytes[offset + 3] & 0xff)) << 32)
                + (((long) (bytes[offset + 4] & 0xff)) << 24) + (((long) (bytes[offset + 5] & 0xff)) << 16)
                + (((long) (bytes[offset + 6] & 0xff)) << 8) + (((long) (bytes[offset + 7] & 0xff)) << 0);
    }

}
