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
    private static String errorMessage = "This can not be an instance of time";

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
        parseString(time);
        timeSerde.serialize(aTime, out);
    }

    private static void parseString(String time) throws HyracksDataException {

        int hour = 0, min = 0, sec = 0, millis = 0;
        int timezone = 0;

        int offset = 0;

        boolean isExtendedForm = false;
        if (time.charAt(offset + 2) == ':') {
            isExtendedForm = true;
        }
        if (isExtendedForm) {
            // parse extended form
            if (time.charAt(offset) == '-' || time.charAt(offset + 5) != ':')
                throw new HyracksDataException(errorMessage);

            for (int i = 0; i < 2; i++) {
                if ((time.charAt(offset + i) >= '0' && time.charAt(offset + i) <= '9'))
                    hour = hour * 10 + time.charAt(offset + i) - '0';
                else
                    throw new HyracksDataException(errorMessage);

            }

            if (hour < 0 || hour > 23) {
                throw new HyracksDataException(errorMessage + ": hour " + hour);
            }

            offset += 3;

            for (int i = 0; i < 2; i++) {
                if ((time.charAt(offset + i) >= '0' && time.charAt(offset + i) <= '9'))
                    min = min * 10 + time.charAt(offset + i) - '0';
                else
                    throw new HyracksDataException(errorMessage);

            }

            if (min < 0 || min > 59) {
                throw new HyracksDataException(errorMessage + ": min " + min);
            }

            offset += 3;

            for (int i = 0; i < 2; i++) {
                if ((time.charAt(offset + i) >= '0' && time.charAt(offset + i) <= '9'))
                    sec = sec * 10 + time.charAt(offset + i) - '0';
                else
                    throw new HyracksDataException(errorMessage);

            }

            if (sec < 0 || sec > 59) {
                throw new HyracksDataException(errorMessage + ": sec " + sec);
            }

            offset += 2;

            if (time.length() > offset && time.charAt(offset) == '.') {

                offset++;
                int i = 0;
                for (; i < 3 && offset + i < time.length(); i++) {
                    if (time.charAt(offset + i) >= '0' && time.charAt(offset + i) <= '9') {
                        millis = millis * 10 + time.charAt(offset + i) - '0';
                    } else {
                        break;
                    }
                }

                offset += i;

                for (; i < 3; i++) {
                    millis = millis * 10;
                }

                for (; offset < time.length(); offset++) {
                    if (time.charAt(offset) < '0' || time.charAt(offset) > '9') {
                        break;
                    }
                }
            }

            if (time.length() > offset) {
                if (time.charAt(offset) != 'Z') {
                    if ((time.charAt(offset) != '+' && time.charAt(offset) != '-') || (time.charAt(offset + 3) != ':'))
                        throw new HyracksDataException(errorMessage);

                    short timezoneHour = 0;
                    short timezoneMinute = 0;

                    for (int i = 0; i < 2; i++) {
                        if ((time.charAt(offset + 1 + i) >= '0' && time.charAt(offset + 1 + i) <= '9'))
                            timezoneHour = (short) (timezoneHour * 10 + time.charAt(offset + 1 + i) - '0');
                        else
                            throw new HyracksDataException(errorMessage);

                    }

                    if (timezoneHour < GregorianCalendarSystem.TIMEZONE_HOUR_MIN
                            || timezoneHour > GregorianCalendarSystem.TIMEZONE_HOUR_MAX) {
                        throw new HyracksDataException(errorMessage + ": time zone hour " + timezoneHour);
                    }

                    for (int i = 0; i < 2; i++) {
                        if ((time.charAt(offset + 4 + i) >= '0' && time.charAt(offset + 4 + i) <= '9'))
                            timezoneMinute = (short) (timezoneMinute * 10 + time.charAt(offset + 4 + i) - '0');
                        else
                            throw new HyracksDataException(errorMessage);
                    }

                    if (timezoneMinute < GregorianCalendarSystem.TIMEZONE_MIN_MIN
                            || timezoneMinute > GregorianCalendarSystem.TIMEZONE_MIN_MAX) {
                        throw new HyracksDataException(errorMessage + ": time zone minute " + timezoneMinute);
                    }

                    if (time.charAt(offset) == '-')
                        timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                    else
                        timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                }
            }

        } else {
            // parse basic form

            if (time.charAt(offset) == '-')
                throw new HyracksDataException(errorMessage);

            for (int i = 0; i < 2; i++) {
                if ((time.charAt(offset + i) >= '0' && time.charAt(offset + i) <= '9'))
                    hour = hour * 10 + time.charAt(offset + i) - '0';
                else
                    throw new HyracksDataException(errorMessage);

            }

            if (hour < 0 || hour > 23) {
                throw new HyracksDataException(errorMessage + ": hour " + hour);
            }

            offset += 2;

            for (int i = 0; i < 2; i++) {
                if ((time.charAt(offset + i) >= '0' && time.charAt(offset + i) <= '9'))
                    min = min * 10 + time.charAt(offset + i) - '0';
                else
                    throw new HyracksDataException(errorMessage);

            }

            if (min < 0 || min > 59) {
                throw new HyracksDataException(errorMessage + ": min " + min);
            }

            offset += 2;

            for (int i = 0; i < 2; i++) {
                if ((time.charAt(offset + i) >= '0' && time.charAt(offset + i) <= '9'))
                    sec = sec * 10 + time.charAt(offset + i) - '0';
                else
                    throw new HyracksDataException(errorMessage);

            }

            if (sec < 0 || sec > 59) {
                throw new HyracksDataException(errorMessage + ": sec " + sec);
            }

            offset += 2;

            if (time.length() > offset) {
                int i = 0;
                for (; i < 3 && offset + i < time.length(); i++) {
                    if ((time.charAt(offset + i) >= '0' && time.charAt(offset + i) <= '9')) {
                        millis = millis * 10 + time.charAt(offset + i) - '0';
                    } else {
                        break;
                    }
                }

                offset += i;

                for (; i < 3; i++) {
                    millis = millis * 10;
                }

                for (; offset < time.length(); offset++) {
                    if (time.charAt(offset) < '0' || time.charAt(offset) > '9') {
                        break;
                    }
                }
            }

            if (time.length() > offset) {
                if (time.charAt(offset) != 'Z') {
                    if ((time.charAt(offset) != '+' && time.charAt(offset) != '-'))
                        throw new HyracksDataException(errorMessage);

                    short timezoneHour = 0;
                    short timezoneMinute = 0;

                    for (int i = 0; i < 2; i++) {
                        if ((time.charAt(offset + 1 + i) >= '0' && time.charAt(offset + 1 + i) <= '9'))
                            timezoneHour = (short) (timezoneHour * 10 + time.charAt(offset + 1 + i) - '0');
                        else
                            throw new HyracksDataException(errorMessage);

                    }

                    for (int i = 0; i < 2; i++) {
                        if ((time.charAt(offset + 3 + i) >= '0' && time.charAt(offset + 3 + i) <= '9'))
                            timezoneMinute = (short) (timezoneMinute * 10 + time.charAt(offset + 3 + i) - '0');
                        else
                            throw new HyracksDataException(errorMessage);

                    }

                    if (time.charAt(offset) == '-')
                        timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                    else
                        timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                }
            }
        }

        aTime.setValue(GregorianCalendarSystem.getInstance().getChronon(hour, min, sec, millis, timezone));

    }
}