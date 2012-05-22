package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADateTime;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ADateTimeSerializerDeserializer implements ISerializerDeserializer<ADateTime> {

    private static final long serialVersionUID = 1L;

    public static final ADateTimeSerializerDeserializer INSTANCE = new ADateTimeSerializerDeserializer();
    @SuppressWarnings("unchecked")
    private static ISerializerDeserializer<ADateTime> datetimeSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADATETIME);
    private static AMutableDateTime aDateTime = new AMutableDateTime(0L);
    private static final String errorMessage = "This can not be an instance of datetime";

    private long chrononTime;

    private ADateTimeSerializerDeserializer() {
    }

    @Override
    public ADateTime deserialize(DataInput in) throws HyracksDataException {
        try {
            chrononTime = in.readLong();

            return new ADateTime(chrononTime);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ADateTime instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeLong(instance.getChrnonoTime());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void parse(String datetime, DataOutput out) throws HyracksDataException {
        parseString(datetime);
        datetimeSerde.serialize(aDateTime, out);
    }

    private static void parseString(String datetime) throws HyracksDataException {
        int year = 0, month = 0, day = 0, hour = 0, min = 0, sec = 0, millis = 0;
        int timezone = 0;

        int offset = 0;

        boolean positive = true;
        boolean isExtendedForm = false;
        if (datetime.charAt(offset + 13) == ':' || datetime.charAt(offset + 14) == ':') {
            isExtendedForm = true;
        }
        if (isExtendedForm) {
            // parse extended form
            if (datetime.charAt(offset) == '-') {
                offset++;
                positive = false;
            }

            if (datetime.charAt(offset + 4) != '-' || datetime.charAt(offset + 7) != '-')
                throw new HyracksDataException(errorMessage);

            // year
            for (int i = 0; i < 4; i++) {
                if ((datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9')) {
                    year = year * 10 + datetime.charAt(offset + i) - '0';
                } else {
                    throw new HyracksDataException(errorMessage);
                }
            }

            if (year < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.YEAR]
                    || year > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.YEAR]) {
                throw new HyracksDataException(errorMessage + ": year " + year);
            }

            offset += 5;

            // month
            for (int i = 0; i < 2; i++) {
                if ((datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9')) {
                    month = month * 10 + datetime.charAt(offset + i) - '0';
                } else {
                    throw new HyracksDataException(errorMessage);
                }
            }

            if (month < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.MONTH]
                    || month > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.MONTH]) {
                throw new HyracksDataException(errorMessage + ": month " + month);
            }

            offset += 3;

            // day
            for (int i = 0; i < 2; i++) {
                if ((datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9')) {
                    day = day * 10 + datetime.charAt(offset + i) - '0';
                } else {
                    throw new HyracksDataException(errorMessage);
                }
            }

            if (day < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.DAY]
                    || day > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.DAY]) {
                throw new HyracksDataException(errorMessage + ": day " + day);
            }

            offset += 2;

            if (!positive)
                year *= -1;

            // skip the "T" separator
            offset += 1;

            if (datetime.charAt(offset + 2) != ':' || datetime.charAt(offset + 5) != ':')
                throw new HyracksDataException(errorMessage);

            // hour
            for (int i = 0; i < 2; i++) {
                if ((datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9')) {
                    hour = hour * 10 + datetime.charAt(offset + i) - '0';
                } else {
                    throw new HyracksDataException(errorMessage);
                }
            }

            if (hour < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.HOUR]
                    || hour > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.HOUR]) {
                throw new HyracksDataException(errorMessage + ": hour " + hour);
            }

            offset += 3;

            // minute
            for (int i = 0; i < 2; i++) {
                if ((datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9')) {
                    min = min * 10 + datetime.charAt(offset + i) - '0';
                } else {
                    throw new HyracksDataException(errorMessage);
                }
            }

            if (min < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.MINUTE]
                    || min > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.MINUTE]) {
                throw new HyracksDataException(errorMessage + ": min " + min);
            }

            offset += 3;

            // second
            for (int i = 0; i < 2; i++) {
                if ((datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9')) {
                    sec = sec * 10 + datetime.charAt(offset + i) - '0';
                } else {
                    throw new HyracksDataException(errorMessage);
                }
            }

            if (sec < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.SECOND]
                    || sec > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.SECOND]) {
                throw new HyracksDataException(errorMessage + ": sec " + sec);
            }

            offset += 2;

            if (datetime.length() > offset && datetime.charAt(offset) == '.') {

                offset++;
                int i = 0;
                for (; i < 3 && offset + i < datetime.length(); i++) {
                    if (datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9') {
                        millis = millis * 10 + datetime.charAt(offset + i) - '0';
                    } else {
                        break;
                    }
                }

                offset += i;

                for (; i < 3; i++) {
                    millis = millis * 10;
                }

                for (; offset < datetime.length(); offset++) {
                    if (datetime.charAt(offset) < '0' || datetime.charAt(offset) > '9') {
                        break;
                    }
                }
            }

            if (datetime.length() > offset) {
                if (datetime.charAt(offset) != 'Z') {
                    if ((datetime.charAt(offset) != '+' && datetime.charAt(offset) != '-')
                            || (datetime.charAt(offset + 3) != ':'))
                        throw new HyracksDataException(errorMessage);

                    short timezoneHour = 0;
                    short timezoneMinute = 0;

                    for (int i = 0; i < 2; i++) {
                        if ((datetime.charAt(offset + 1 + i) >= '0' && datetime.charAt(offset + 1 + i) <= '9'))
                            timezoneHour = (short) (timezoneHour * 10 + datetime.charAt(offset + 1 + i) - '0');
                        else
                            throw new HyracksDataException(errorMessage);

                    }

                    if (timezoneHour < GregorianCalendarSystem.TIMEZONE_HOUR_MIN
                            || timezoneHour > GregorianCalendarSystem.TIMEZONE_HOUR_MAX) {
                        throw new HyracksDataException(errorMessage + ": time zone hour " + timezoneHour);
                    }

                    for (int i = 0; i < 2; i++) {
                        if ((datetime.charAt(offset + 4 + i) >= '0' && datetime.charAt(offset + 4 + i) <= '9'))
                            timezoneMinute = (short) (timezoneMinute * 10 + datetime.charAt(offset + 4 + i) - '0');
                        else
                            throw new HyracksDataException(errorMessage);
                    }

                    if (timezoneMinute < GregorianCalendarSystem.TIMEZONE_MIN_MIN
                            || timezoneMinute > GregorianCalendarSystem.TIMEZONE_MIN_MAX) {
                        throw new HyracksDataException(errorMessage + ": time zone minute " + timezoneMinute);
                    }

                    if (datetime.charAt(offset) == '-')
                        timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                    else
                        timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                }
            }

        } else {
            // parse basic form
            if (datetime.charAt(offset) == '-') {
                offset++;
                positive = false;
            }

            // year
            for (int i = 0; i < 4; i++) {
                if ((datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9')) {
                    year = year * 10 + datetime.charAt(offset + i) - '0';
                } else {
                    throw new HyracksDataException(errorMessage);
                }
            }

            if (year < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.YEAR]
                    || year > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.YEAR]) {
                throw new HyracksDataException(errorMessage + ": year " + year);
            }

            offset += 4;

            // month
            for (int i = 0; i < 2; i++) {
                if ((datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9')) {
                    month = month * 10 + datetime.charAt(offset + i) - '0';
                } else {
                    throw new HyracksDataException(errorMessage);
                }
            }

            if (month < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.MONTH]
                    || month > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.MONTH]) {
                throw new HyracksDataException(errorMessage + ": month " + month);
            }

            offset += 2;

            // day
            for (int i = 0; i < 2; i++) {
                if ((datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9')) {
                    day = day * 10 + datetime.charAt(offset + i) - '0';
                } else {
                    throw new HyracksDataException(errorMessage);
                }
            }

            if (day < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.DAY]
                    || day > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.DAY]) {
                throw new HyracksDataException(errorMessage + ": day " + day);
            }

            offset += 2;

            if (!positive)
                year *= -1;

            // hour
            for (int i = 0; i < 2; i++) {
                if ((datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9')) {
                    hour = hour * 10 + datetime.charAt(offset + i) - '0';
                } else {
                    throw new HyracksDataException(errorMessage);
                }
            }

            if (hour < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.HOUR]
                    || hour > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.HOUR]) {
                throw new HyracksDataException(errorMessage + ": hour " + hour);
            }

            offset += 2;

            // minute
            for (int i = 0; i < 2; i++) {
                if ((datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9')) {
                    min = min * 10 + datetime.charAt(offset + i) - '0';
                } else {
                    throw new HyracksDataException(errorMessage);
                }
            }

            if (min < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.MINUTE]
                    || min > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.MINUTE]) {
                throw new HyracksDataException(errorMessage + ": min " + min);
            }

            offset += 2;

            // second
            for (int i = 0; i < 2; i++) {
                if ((datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9')) {
                    sec = sec * 10 + datetime.charAt(offset + i) - '0';
                } else {
                    throw new HyracksDataException(errorMessage);
                }
            }

            if (sec < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.SECOND]
                    || sec > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.SECOND]) {
                throw new HyracksDataException(errorMessage + ": sec " + sec);
            }

            offset += 2;

            if (datetime.length() > offset) {
                int i = 0;
                for (; i < 3 && offset + i < datetime.length(); i++) {
                    if (datetime.charAt(offset + i) >= '0' && datetime.charAt(offset + i) <= '9') {
                        millis = millis * 10 + datetime.charAt(offset + i) - '0';
                    } else {
                        break;
                    }
                }

                offset += i;

                for (; i < 3; i++) {
                    millis = millis * 10;
                }

                for (; offset < datetime.length(); offset++) {
                    if (datetime.charAt(offset) < '0' || datetime.charAt(offset) > '9') {
                        break;
                    }
                }
            }

            if (datetime.length() > offset) {
                if (datetime.charAt(offset) != 'Z') {
                    if ((datetime.charAt(offset) != '+' && datetime.charAt(offset) != '-'))
                        throw new HyracksDataException(errorMessage);

                    short timezoneHour = 0;
                    short timezoneMinute = 0;

                    for (int i = 0; i < 2; i++) {
                        if ((datetime.charAt(offset + 1 + i) >= '0' && datetime.charAt(offset + 1 + i) <= '9'))
                            timezoneHour = (short) (timezoneHour * 10 + datetime.charAt(offset + 1 + i) - '0');
                        else
                            throw new HyracksDataException(errorMessage);

                    }

                    if (timezoneHour < GregorianCalendarSystem.TIMEZONE_HOUR_MIN
                            || timezoneHour > GregorianCalendarSystem.TIMEZONE_HOUR_MAX) {
                        throw new HyracksDataException(errorMessage + ": time zone hour " + timezoneHour);
                    }

                    for (int i = 0; i < 2; i++) {
                        if ((datetime.charAt(offset + 3 + i) >= '0' && datetime.charAt(offset + 3 + i) <= '9'))
                            timezoneMinute = (short) (timezoneMinute * 10 + datetime.charAt(offset + 3 + i) - '0');
                        else
                            throw new HyracksDataException(errorMessage);
                    }

                    if (timezoneMinute < GregorianCalendarSystem.TIMEZONE_MIN_MIN
                            || timezoneMinute > GregorianCalendarSystem.TIMEZONE_MIN_MAX) {
                        throw new HyracksDataException(errorMessage + ": time zone minute " + timezoneMinute);
                    }

                    if (datetime.charAt(offset) == '-')
                        timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                    else
                        timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                }
            }
        }

        aDateTime.setValue(GregorianCalendarSystem.getInstance().getChronon(year, month, day, hour, min, sec,
                (int) millis, timezone));
    }
}
