package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.AInt8;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class AInt8SerializerDeserializer implements ISerializerDeserializer<AInt8> {

    private static final long serialVersionUID = 1L;

    public static final AInt8SerializerDeserializer INSTANCE = new AInt8SerializerDeserializer();

    @Override
    public AInt8 deserialize(DataInput in) throws HyracksDataException {
        try {
            return new AInt8(in.readByte());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    @Override
    public void serialize(AInt8 instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeByte(instance.getByteValue());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    public static byte getByte(byte[] bytes, int offset) {
        return bytes[offset];
    }

}
