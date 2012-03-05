package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADate;
import edu.uci.ics.asterix.om.base.AMutableDate;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ADateSerializerDeserializer implements ISerializerDeserializer<ADate> {

    private static final long serialVersionUID = 1L;
    private short year;
    private byte monthAndDay;

    public static final ADateSerializerDeserializer INSTANCE = new ADateSerializerDeserializer();
    @SuppressWarnings("unchecked")
    private static ISerializerDeserializer<ADate> dateSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADATE);
    private static AMutableDate aDate = new AMutableDate(0, 0, 0, 0);
    private static String errorMessage = " can not be an instance of date";

    private ADateSerializerDeserializer() {
    }

    @Override
    public ADate deserialize(DataInput in) throws HyracksDataException {
        try {
            year = in.readShort();
            monthAndDay = in.readByte();
            return new ADate(year >> 1, (year & 0x0001) * 8 + ((monthAndDay >> 5) & 0x07), monthAndDay & 0x1f,
                    in.readByte());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ADate instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeByte((byte) ((instance.getYear() << 1) >> 8));
            out.writeByte((byte) ((byte) ((instance.getYear() << 1) & 0x00ff) + (byte) (instance.getMonth() >> 3)));
            out.writeByte((byte) (((byte) (instance.getMonth() << 5)) | ((byte) instance.getDay())));
            out.writeByte((byte) instance.getTimeZone());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void parse(String date, DataOutput out) throws HyracksDataException {
        int offset = 0;
        boolean positive = true;
        byte timezonePart = 0;
        if (date.charAt(0) == '-') {
            positive = false;
            offset++;
        }

        if (date.charAt(offset + 4) != '-' || date.charAt(offset + 7) != '-')
            throw new HyracksDataException(date + errorMessage);
        short year = Short.parseShort(date.substring(offset, offset + 4));
        short month = Short.parseShort(date.substring(offset + 5, offset + 7));
        short day = Short.parseShort(date.substring(offset + 8, offset + 10));
        short hour, minute;
        if (year < 0 || year > 9999 || month < 0 || month > 12 || day < 0 || day > 31)
            throw new HyracksDataException(date + errorMessage);

        offset += 10;

        if (date.length() > offset) {
            if (date.charAt(offset) == 'Z')
                timezonePart = 0;
            else {
                if ((date.charAt(offset) != '+' && date.charAt(offset) != '-') || (date.charAt(offset + 3) != ':'))
                    throw new HyracksDataException(date + errorMessage);

                hour = Short.parseShort(date.substring(offset + 1, offset + 3));
                minute = Short.parseShort(date.substring(offset + 4, offset + 6));

                if (hour < 0 || hour > 24 || (hour == 24 && minute != 0)
                        || (minute != 0 && minute != 15 && minute != 30 && minute != 45))
                    throw new HyracksDataException(date + errorMessage);

                if (date.charAt(offset) == '-')
                    timezonePart = (byte) -((hour * 4) + minute / 15);
                else
                    timezonePart = (byte) ((hour * 4) + minute / 15);
            }

        }
        if (!positive)
            year *= -1;
        aDate.setValue(year, month, day, timezonePart);
        dateSerde.serialize(aDate, out);
    }

}
