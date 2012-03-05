package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ABooleanSerializerDeserializer implements ISerializerDeserializer<ABoolean> {

    private static final long serialVersionUID = 1L;

    public static final ABooleanSerializerDeserializer INSTANCE = new ABooleanSerializerDeserializer();

    private ABooleanSerializerDeserializer() {
    }

    @Override
    public ABoolean deserialize(DataInput in) throws HyracksDataException {
        try {
            return (in.readBoolean()) ? ABoolean.TRUE : ABoolean.FALSE;
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    @Override
    public void serialize(ABoolean instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeBoolean(instance.getBoolean());
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    public static boolean getBoolean(byte[] bytes, int offset) {
        if (bytes[offset] == 0) {
            return false;
        } else {
            return true;
        }
    }

}
