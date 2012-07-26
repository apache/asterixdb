package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADate;
import edu.uci.ics.asterix.om.base.AMutableDate;
import edu.uci.ics.asterix.om.base.temporal.ADateAndTimeParser;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.base.temporal.StringCharSequenceAccessor;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ADateSerializerDeserializer implements ISerializerDeserializer<ADate> {

    private static final long serialVersionUID = 1L;

    public static final ADateSerializerDeserializer INSTANCE = new ADateSerializerDeserializer();
    @SuppressWarnings("unchecked")
    private static final ISerializerDeserializer<ADate> dateSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADATE);

    private ADateSerializerDeserializer() {
    }

    @Override
    public ADate deserialize(DataInput in) throws HyracksDataException {
        try {
            return new ADate(in.readInt());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ADate instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeInt(instance.getChrononTimeInDays());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void parse(String date, DataOutput out) throws HyracksDataException {
        AMutableDate aDate = new AMutableDate(0);

        long chrononTimeInMs = 0;
        try {
            StringCharSequenceAccessor charAccessor = new StringCharSequenceAccessor();
            charAccessor.reset(date, 0);
            chrononTimeInMs = ADateAndTimeParser.parseDatePart(charAccessor, true);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        short temp = 0;
        if (chrononTimeInMs < 0 && chrononTimeInMs % GregorianCalendarSystem.CHRONON_OF_DAY != 0) {
            temp = 1;
        }
        aDate.setValue((int) (chrononTimeInMs / GregorianCalendarSystem.CHRONON_OF_DAY) - temp);

        dateSerde.serialize(aDate, out);
    }
}
