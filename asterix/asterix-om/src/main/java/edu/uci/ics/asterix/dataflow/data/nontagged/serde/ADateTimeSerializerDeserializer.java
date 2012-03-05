package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADateTime;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ADateTimeSerializerDeserializer implements ISerializerDeserializer<ADateTime> {

    private static final long serialVersionUID = 1L;

    public static final ADateTimeSerializerDeserializer INSTANCE = new ADateTimeSerializerDeserializer();
    @SuppressWarnings("unchecked")
    private static ISerializerDeserializer<ADateTime> datetimeSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADATETIME);
    private static AMutableDateTime aDateTime = new AMutableDateTime(0, 0, 0, 0, 0, 0, 0, 0, 0);
    private static String errorMessage = " can not be an instance of datetime";

    private int time;
    private byte timezone;
    private short year;
    private byte monthAndDay;

    private ADateTimeSerializerDeserializer() {
    }

    @Override
    public ADateTime deserialize(DataInput in) throws HyracksDataException {
        try {
            year = in.readShort();
            monthAndDay = in.readByte();
            timezone = in.readByte();
            time = in.readInt();

            return new ADateTime(year >> 1, (year & 0x0001) * 8 + ((monthAndDay >> 5) & 0x07), monthAndDay & 0x1f,
                    (short) ((time) * 20 % 216000000 / 3600000), (short) ((time) * 20 % 3600000 / 60000),
                    (short) ((time) * 20 % 60000 / 1000), (short) ((time) * 20 % 1000), (short) 0, timezone);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ADateTime instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeByte((byte) ((instance.getYear() << 1) >> 8));
            out.writeByte((byte) ((byte) ((instance.getYear() << 1) & 0x00ff) + (byte) (instance.getMonth() >> 3)));
            out.writeByte((byte) ((instance.getMonth() << 5) | (instance.getDay())));
            out.writeByte((byte) instance.getTimeZone());
            out.writeInt((((((instance.getHours() * 60) + instance.getMinutes()) * 60) + instance.getSeconds()) * 1000 + instance
                    .getMilliseconds()) / 20);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void parse(String datetime, DataOutput out) throws HyracksDataException {
        boolean positive = true;
        int offset = 0;
        byte timezonePart = 0;

        if (datetime.charAt(offset) == '-') {
            offset++;
            positive = false;
        }

        if (datetime.charAt(offset + 4) != '-' || datetime.charAt(offset + 7) != '-')
            throw new HyracksDataException(datetime + errorMessage);

        short year = Short.parseShort(datetime.substring(offset, offset + 4));
        short month = Short.parseShort(datetime.substring(offset + 5, offset + 7));
        short day = Short.parseShort(datetime.substring(offset + 8, offset + 10));

        if (year < 0 || year > 9999 || month < 0 || month > 12 || day < 0 || day > 31)
            throw new HyracksDataException(datetime + errorMessage);

        if (!positive)
            year *= -1;

        if (datetime.charAt(offset + 10) != 'T')
            throw new HyracksDataException(datetime + errorMessage);

        offset += 11;

        if (datetime.charAt(offset + 2) != ':' || datetime.charAt(offset + 5) != ':')
            throw new HyracksDataException(datetime + errorMessage);

        short hour = Short.parseShort(datetime.substring(offset, offset + 2));
        short minute = Short.parseShort(datetime.substring(offset + 3, offset + 5));
        short second = Short.parseShort(datetime.substring(offset + 6, offset + 8));
        short msecond = 0;

        if (datetime.length() > offset + 8 && datetime.charAt(offset + 8) == ':') {
            msecond = Short.parseShort(datetime.substring(offset + 9, offset + 12));
            if (hour < 0 || hour > 24 || minute < 0 || minute > 59 || second < 0 || second > 59 || msecond < 0
                    || msecond > 999 || (hour == 24 && (minute != 0 || second != 0 || msecond != 0)))
                throw new HyracksDataException(datetime + errorMessage);
            offset += 12;
        } else {
            if (hour < 0 || hour > 24 || minute < 0 || minute > 59 || second < 0 || second > 59
                    || (hour == 24 && (minute != 0 || second != 0)))
                throw new HyracksDataException(datetime + errorMessage);
            offset += 8;
        }

        short timezoneHour = 0, timezoneMinute = 0;
        if (datetime.length() > offset) {
            if (datetime.charAt(offset) == 'Z')
                timezonePart = 0;
            else {
                if ((datetime.charAt(offset) != '+' && datetime.charAt(offset) != '-')
                        || (datetime.charAt(offset + 3) != ':'))
                    throw new HyracksDataException(datetime + errorMessage);

                timezoneHour = Short.parseShort(datetime.substring(offset + 1, offset + 3));
                timezoneMinute = Short.parseShort(datetime.substring(offset + 4, offset + 6));

                if (timezoneHour < 0
                        || timezoneHour > 24
                        || (timezoneHour == 24 && timezoneMinute != 0)
                        || (timezoneMinute != 0 && timezoneMinute != 15 && timezoneMinute != 30 && timezoneMinute != 45))
                    throw new HyracksDataException(datetime + errorMessage);

                if (datetime.charAt(offset) == '-')
                    timezonePart = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                else
                    timezonePart = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
            }

        }
        aDateTime.setValue(year, month, day, hour, minute, second, msecond, 0, timezonePart);
        datetimeSerde.serialize(aDateTime, out);
    }
}
