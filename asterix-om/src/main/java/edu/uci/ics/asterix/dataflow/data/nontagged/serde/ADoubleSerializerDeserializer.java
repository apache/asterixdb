package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ADoubleSerializerDeserializer implements ISerializerDeserializer<ADouble> {

    private static final long serialVersionUID = 1L;

    public static final ADoubleSerializerDeserializer INSTANCE = new ADoubleSerializerDeserializer();

    private ADoubleSerializerDeserializer() {
    }

    @Override
    public ADouble deserialize(DataInput in) throws HyracksDataException {
        try {
            return new ADouble(in.readDouble());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ADouble instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeDouble(instance.getDoubleValue());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    public static double getDouble(byte[] bytes, int offset) {
        return Double.longBitsToDouble(getLongBits(bytes, offset));
    }

    public static long getLongBits(byte[] bytes, int offset) {
        return AInt64SerializerDeserializer.getLong(bytes, offset);
    }

}
