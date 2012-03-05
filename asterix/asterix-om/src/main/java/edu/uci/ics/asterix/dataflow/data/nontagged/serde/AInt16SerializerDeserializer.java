package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.AInt16;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class AInt16SerializerDeserializer implements ISerializerDeserializer<AInt16> {

    private static final long serialVersionUID = 1L;

    public static final AInt16SerializerDeserializer INSTANCE = new AInt16SerializerDeserializer();

    @Override
    public AInt16 deserialize(DataInput in) throws HyracksDataException {
        try {
            return new AInt16(in.readShort());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    @Override
    public void serialize(AInt16 instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeShort(instance.getShortValue());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    public static short getShort(byte[] bytes, int offset) {
        return (short) (((bytes[offset] & 0xff) << 8) + ((bytes[offset + 1] & 0xff) << 0));
    }

    public static int getUnsignedShort(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 8) + ((bytes[offset + 1] & 0xff) << 0);
    }

}
