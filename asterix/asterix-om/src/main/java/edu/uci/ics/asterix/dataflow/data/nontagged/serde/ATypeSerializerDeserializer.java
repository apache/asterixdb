package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;

import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ATypeSerializerDeserializer implements ISerializerDeserializer<IAType> {

    private static final long serialVersionUID = 1L;

    public static final ATypeSerializerDeserializer INSTANCE = new ATypeSerializerDeserializer();

    private ATypeSerializerDeserializer() {
    }

    @Override
    public IAType deserialize(DataInput in) throws HyracksDataException {
        throw new NotImplementedException();
    }

    @Override
    public void serialize(IAType instance, DataOutput out) throws HyracksDataException {
        throw new NotImplementedException();
    }
}
