package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;

import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ANullSerializerDeserializer implements ISerializerDeserializer<ANull> {

    private static final long serialVersionUID = 1L;

    public static final ANullSerializerDeserializer INSTANCE = new ANullSerializerDeserializer();

    private ANullSerializerDeserializer() {
    }

    @Override
    public ANull deserialize(DataInput in) throws HyracksDataException {
        return ANull.NULL;
    }

    @Override
    public void serialize(ANull instance, DataOutput out) throws HyracksDataException {
    }

}
