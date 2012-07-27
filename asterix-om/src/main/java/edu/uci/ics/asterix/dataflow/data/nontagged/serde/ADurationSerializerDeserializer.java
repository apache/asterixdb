package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADuration;
import edu.uci.ics.asterix.om.base.AMutableDuration;
import edu.uci.ics.asterix.om.base.temporal.ADurationParser;
import edu.uci.ics.asterix.om.base.temporal.StringCharSequenceAccessor;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ADurationSerializerDeserializer implements ISerializerDeserializer<ADuration> {

    private static final long serialVersionUID = 1L;

    public static final ADurationSerializerDeserializer INSTANCE = new ADurationSerializerDeserializer();
    @SuppressWarnings("unchecked")
    private static final ISerializerDeserializer<ADuration> durationSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADURATION);

    private ADurationSerializerDeserializer() {
    }

    @Override
    public ADuration deserialize(DataInput in) throws HyracksDataException {
        try {
            return new ADuration(in.readInt(), in.readLong());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ADuration instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeInt(instance.getMonths());
            out.writeLong(instance.getMilliseconds());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void parse(String duration, DataOutput out) throws HyracksDataException {
        try {
            AMutableDuration aDuration = new AMutableDuration(0, 0);
            StringCharSequenceAccessor charAccessor = new StringCharSequenceAccessor();
            charAccessor.reset(duration, 0);
            ADurationParser.parse(charAccessor, aDuration);

            durationSerde.serialize(aDuration, out);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
