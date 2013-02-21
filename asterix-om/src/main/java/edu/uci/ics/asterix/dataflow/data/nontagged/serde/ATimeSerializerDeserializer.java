package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableTime;
import edu.uci.ics.asterix.om.base.ATime;
import edu.uci.ics.asterix.om.base.temporal.ATimeParserFactory;
import edu.uci.ics.asterix.om.base.temporal.StringCharSequenceAccessor;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ATimeSerializerDeserializer implements ISerializerDeserializer<ATime> {

    private static final long serialVersionUID = 1L;

    public static final ATimeSerializerDeserializer INSTANCE = new ATimeSerializerDeserializer();

    @SuppressWarnings("unchecked")
    private static final ISerializerDeserializer<ATime> timeSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ATIME);

    private ATimeSerializerDeserializer() {
    }

    @Override
    public ATime deserialize(DataInput in) throws HyracksDataException {
        try {
            return new ATime(in.readInt());

        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ATime instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeInt(instance.getChrononTime());

        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void parse(String time, DataOutput out) throws HyracksDataException {
        AMutableTime aTime = new AMutableTime(0);
        int chrononTimeInMs;

        try {
            StringCharSequenceAccessor charAccessor = new StringCharSequenceAccessor();
            charAccessor.reset(time, 0, time.length());
            chrononTimeInMs = ATimeParserFactory.parseTimePart(charAccessor);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        aTime.setValue(chrononTimeInMs);

        timeSerde.serialize(aTime, out);
    }

    public static int getChronon(byte[] byteArray, int offset) {
        return AInt32SerializerDeserializer.getInt(byteArray, offset);
    }

}