package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableTime;
import edu.uci.ics.asterix.om.base.ATime;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ATimeSerializerDeserializer implements ISerializerDeserializer<ATime> {

    private static final long serialVersionUID = 1L;

    public static final ATimeSerializerDeserializer INSTANCE = new ATimeSerializerDeserializer();

    @SuppressWarnings("unchecked")
    private static ISerializerDeserializer<ATime> timeSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ATIME);
    private static AMutableTime aTime = new AMutableTime(0);
    //private static String errorMessage = " can not be an instance of time";

    private int ora;

    private ATimeSerializerDeserializer() {
    }

    @Override
    public ATime deserialize(DataInput in) throws HyracksDataException {
        try {
            ora = in.readInt();
            
            return new ATime(ora);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ATime instance, DataOutput out) throws HyracksDataException {
        try {
        	out.writeInt(instance.getOra());

        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void parse(String time, DataOutput out) throws HyracksDataException {
    	GregorianCalendarSystem.getInstance().parseStringForATime(time, aTime);
    	timeSerde.serialize(aTime, out);
    }
}