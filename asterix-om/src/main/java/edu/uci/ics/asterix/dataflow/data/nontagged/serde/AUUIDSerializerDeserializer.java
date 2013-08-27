package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

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
        UUID uuid = new UUID(msb, lsb);
        return new AUUID(uuid);
    }

    @Override
    public void serialize(AUUID instance, DataOutput out) throws HyracksDataException {
        UUID uuid = instance.getValue();
        try {
            Integer64SerializerDeserializer.INSTANCE.serialize(uuid.getMostSignificantBits(), out);
            Integer64SerializerDeserializer.INSTANCE.serialize(uuid.getLeastSignificantBits(), out);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

}
