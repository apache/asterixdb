package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;

import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class AInt32SerializerDeserializer implements ISerializerDeserializer<AInt32> {

    private static final long serialVersionUID = 1L;

    public static final AInt32SerializerDeserializer INSTANCE = new AInt32SerializerDeserializer();

    private AInt32SerializerDeserializer() {
    }

    @Override
    public AInt32 deserialize(DataInput in) throws HyracksDataException {
        Integer i = IntegerSerializerDeserializer.INSTANCE.deserialize(in);
        return new AInt32(i);
    }

    @Override
    public void serialize(AInt32 instance, DataOutput out) throws HyracksDataException {
        IntegerSerializerDeserializer.INSTANCE.serialize(instance.getIntegerValue(), out);
    }

    public static int getInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }
}
