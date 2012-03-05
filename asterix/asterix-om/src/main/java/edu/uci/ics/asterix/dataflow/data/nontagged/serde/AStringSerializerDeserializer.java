package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

public class AStringSerializerDeserializer implements ISerializerDeserializer<AString> {

    private static final long serialVersionUID = 1L;

    public static final AStringSerializerDeserializer INSTANCE = new AStringSerializerDeserializer();

    private AStringSerializerDeserializer() {
    }

    @Override
    public AString deserialize(DataInput in) throws HyracksDataException {
        try {
            return new AString(UTF8StringSerializerDeserializer.INSTANCE.deserialize(in));
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(AString instance, DataOutput out) throws HyracksDataException {
        try {
            UTF8StringSerializerDeserializer.INSTANCE.serialize(instance.getStringValue(), out);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}
