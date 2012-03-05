package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;

public class AFloatSerializerDeserializer implements ISerializerDeserializer<AFloat> {

    private static final long serialVersionUID = 1L;

    public static final AFloatSerializerDeserializer INSTANCE = new AFloatSerializerDeserializer();

    private AFloatSerializerDeserializer() {
    }

    @Override
    public AFloat deserialize(DataInput in) throws HyracksDataException {
        return new AFloat(FloatSerializerDeserializer.INSTANCE.deserialize(in));
    }

    @Override
    public void serialize(AFloat instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeFloat(instance.getFloatValue());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    public static float getFloat(byte[] bytes, int offset) {
        return FloatSerializerDeserializer.getFloat(bytes, offset);
    }

}
