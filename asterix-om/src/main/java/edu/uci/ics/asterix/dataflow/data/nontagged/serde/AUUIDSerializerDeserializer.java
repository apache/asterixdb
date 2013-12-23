package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.AUUID;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;

public class AUUIDSerializerDeserializer implements ISerializerDeserializer<AUUID> {

    private static final long serialVersionUID = 1L;

    public static final AUUIDSerializerDeserializer INSTANCE = new AUUIDSerializerDeserializer();

    @Override
    public AUUID deserialize(DataInput in) throws HyracksDataException {
        long msb = Integer64SerializerDeserializer.INSTANCE.deserialize(in);
        long lsb = Integer64SerializerDeserializer.INSTANCE.deserialize(in);
        return new AUUID(msb, lsb);
    }

    @Override
    public void serialize(AUUID instance, DataOutput out) throws HyracksDataException {
        try {
            Integer64SerializerDeserializer.INSTANCE.serialize(instance.getMostSignificantBits(), out);
            Integer64SerializerDeserializer.INSTANCE.serialize(instance.getLeastSignificantBits(), out);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

}
