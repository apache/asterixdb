package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableTime;
import edu.uci.ics.asterix.om.base.ATime;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ATimeSerializerDeserializer implements ISerializerDeserializer<ATime> {

    private static final long serialVersionUID = 1L;

    public static final ATimeSerializerDeserializer INSTANCE = new ATimeSerializerDeserializer();

    @SuppressWarnings("unchecked")
    private static ISerializerDeserializer<ATime> timeSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ATIME);
    private static AMutableTime aTime = new AMutableTime(0, 0, 0, 0, 0, 0);
    private static String errorMessage = " can not be an instance of time";

    private int time, timezone;

    private ATimeSerializerDeserializer() {
    }

    @Override
    public ATime deserialize(DataInput in) throws HyracksDataException {
        try {
            time = in.readInt();
            timezone = time >> 24;
            time -= (timezone << 24);
            return new ATime((short) ((time << 8 >> 8) * 20 % 216000000 / 3600000),
                    (short) ((time << 8 >> 8) * 20 % 3600000 / 60000), (short) ((time << 8 >> 8) * 20 % 60000 / 1000),
                    (short) ((time << 8 >> 8) * 20 % 1000), (short) 0, timezone);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ATime instance, DataOutput out) throws HyracksDataException {
        try {
            time = instance.getTimeZone();
            time = time << 24;
            time = time
                    + ((((((instance.getHours() * 60) + instance.getMinutes()) * 60) + instance.getSeconds()) * 1000 + instance
                            .getMilliseconds()) / 20);
            out.writeInt(time);

        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static final void serialize(short hour, short minute, short second, short msecond, byte timeZone,
            DataOutput out) throws HyracksDataException {
        try {
            int time = timeZone;
            time = time << 24;
            time = time + ((((((hour * 60) + minute) * 60) + second) * 1000 + msecond) / 20);
            out.writeInt(time);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void parse(String time, DataOutput out) throws HyracksDataException {
        try {
            byte timezonePart = 0;
            int offset = 0;
            if (time.charAt(offset + 2) != ':' || time.charAt(offset + 5) != ':')
                throw new HyracksDataException(time + errorMessage);

            short hour = Short.parseShort(time.substring(offset, offset + 2));
            short minute = Short.parseShort(time.substring(offset + 3, offset + 5));
            short second = Short.parseShort(time.substring(offset + 6, offset + 8));
            short msecond = 0;
            if (time.length() > offset + 8 && time.charAt(offset + 8) == ':') {
                msecond = Short.parseShort(time.substring(offset + 9, offset + 12));
                if (hour < 0 || hour > 24 || minute < 0 || minute > 59 || second < 0 || second > 59 || msecond < 0
                        || msecond > 999 || (hour == 24 && (minute != 0 || second != 0 || msecond != 0)))
                    throw new HyracksDataException(time + errorMessage);
                offset += 12;
            } else {
                if (hour < 0 || hour > 24 || minute < 0 || minute > 59 || second < 0 || second > 59
                        || (hour == 24 && (minute != 0 || second != 0)))
                    throw new HyracksDataException(time + errorMessage);
                offset += 8;
            }

            short timezoneHour = 0, timezoneMinute = 0;
            if (time.length() > offset) {
                if (time.charAt(offset) == 'Z')
                    timezonePart = 0;
                else {
                    if ((time.charAt(offset) != '+' && time.charAt(offset) != '-') || (time.charAt(offset + 3) != ':'))
                        throw new HyracksDataException(time + errorMessage);

                    timezoneHour = Short.parseShort(time.substring(offset + 1, offset + 3));
                    timezoneMinute = Short.parseShort(time.substring(offset + 4, offset + 6));

                    if (timezoneHour < 0
                            || timezoneHour > 24
                            || (timezoneHour == 24 && timezoneMinute != 0)
                            || (timezoneMinute != 0 && timezoneMinute != 15 && timezoneMinute != 30 && timezoneMinute != 45))
                        throw new HyracksDataException(time + errorMessage);

                    if (time.charAt(offset) == '-')
                        timezonePart = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                    else
                        timezonePart = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                }

            }

            aTime.setValue(hour, minute, second, msecond, 0, timezonePart);

            timeSerde.serialize(aTime, out);
        } catch (HyracksDataException e) {
            throw new HyracksDataException(time + errorMessage);
        }
    }
}