package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADate;
import edu.uci.ics.asterix.om.base.AMutableDate;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ADateSerializerDeserializer implements ISerializerDeserializer<ADate> {

    private static final long serialVersionUID = 1L;

    private int chrononTimeInDays;

    public static final ADateSerializerDeserializer INSTANCE = new ADateSerializerDeserializer();
    @SuppressWarnings("unchecked")
    private static ISerializerDeserializer<ADate> dateSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADATE);
    private static AMutableDate aDate = new AMutableDate(0);
    private static final String errorMessage = "This can not be an instance of date";

    private ADateSerializerDeserializer() {
    }

    @Override
    public ADate deserialize(DataInput in) throws HyracksDataException {
        try {
            chrononTimeInDays = in.readInt();
            return new ADate(chrononTimeInDays);
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
        parseString(date);
        dateSerde.serialize(aDate, out);
    }

    private static void parseString(String date) throws HyracksDataException {
        int year = 0, month = 0, day = 0;
        int timezone = 0;
        int offset = 0;
        boolean positive = true;
        if (date.charAt(offset + 4) == '-' || date.charAt(offset + 5) == '-') {
            // parse extended form
            if (date.charAt(offset) == '-') {
                offset++;
                positive = false;
            }

            if (date.charAt(offset + 4) != '-' || date.charAt(offset + 7) != '-')
                throw new HyracksDataException(errorMessage);

            // year
            for (int i = 0; i < 4; i++) {
                if ((date.charAt(offset + i) >= '0' && date.charAt(offset + i) <= '9')) {
                    year = year * 10 + date.charAt(offset + i) - '0';
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
                if ((date.charAt(offset + i) >= '0' && date.charAt(offset + i) <= '9')) {
                    month = month * 10 + date.charAt(offset + i) - '0';
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
                if ((date.charAt(offset + i) >= '0' && date.charAt(offset + i) <= '9')) {
                    day = day * 10 + date.charAt(offset + i) - '0';
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

            if (date.length() > offset) {
                if (date.charAt(offset) != 'Z') {
                    if ((date.charAt(offset) != '+' && date.charAt(offset) != '-') || (date.charAt(offset + 3) != ':'))
                        throw new HyracksDataException(errorMessage);

                    short timezoneHour = 0;
                    short timezoneMinute = 0;

                    for (int i = 0; i < 2; i++) {
                        if ((date.charAt(offset + 1 + i) >= '0' && date.charAt(offset + 1 + i) <= '9'))
                            timezoneHour = (short) (timezoneHour * 10 + date.charAt(offset + 1 + i) - '0');
                        else
                            throw new HyracksDataException(errorMessage);

                    }

                    for (int i = 0; i < 2; i++) {
                        if ((date.charAt(offset + 4 + i) >= '0' && date.charAt(offset + 4 + i) <= '9'))
                            timezoneMinute = (short) (timezoneMinute * 10 + date.charAt(offset + 4 + i) - '0');
                        else
                            throw new HyracksDataException(errorMessage);

                        if (date.charAt(offset) == '-')
                            timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                        else
                            timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                    }
                }
            }

        } else {
            // parse basic form
            if (date.charAt(offset) == '-') {
                offset++;
                positive = false;
            }

            // year
            for (int i = 0; i < 4; i++) {
                if ((date.charAt(offset + i) >= '0' && date.charAt(offset + i) <= '9')) {
                    year = year * 10 + date.charAt(offset + i) - '0';
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
                if ((date.charAt(offset + i) >= '0' && date.charAt(offset + i) <= '9')) {
                    month = month * 10 + date.charAt(offset + i) - '0';
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
                if ((date.charAt(offset + i) >= '0' && date.charAt(offset + i) <= '9')) {
                    day = day * 10 + date.charAt(offset + i) - '0';
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

            if (date.length() > offset) {
                if (date.charAt(offset) != 'Z') {
                    if ((date.charAt(offset) != '+' && date.charAt(offset) != '-'))
                        throw new HyracksDataException(errorMessage);

                    short timezoneHour = 0;
                    short timezoneMinute = 0;

                    for (int i = 0; i < 2; i++) {
                        if (date.charAt(offset + 1 + i) >= '0' && date.charAt(offset + 1 + i) <= '9')
                            timezoneHour = (short) (timezoneHour * 10 + date.charAt(offset + 1 + i) - '0');
                        else
                            throw new HyracksDataException(errorMessage);

                    }

                    if (timezoneHour < GregorianCalendarSystem.TIMEZONE_HOUR_MIN
                            || timezoneHour > GregorianCalendarSystem.TIMEZONE_HOUR_MAX) {
                        throw new HyracksDataException(errorMessage + ": time zone hour " + timezoneHour);
                    }

                    for (int i = 0; i < 2; i++) {
                        if (date.charAt(offset + 3 + i) >= '0' && date.charAt(offset + 3 + i) <= '9')
                            timezoneMinute = (short) (timezoneMinute * 10 + date.charAt(offset + 3 + i) - '0');
                        else
                            throw new HyracksDataException(errorMessage);
                    }

                    if (timezoneMinute < GregorianCalendarSystem.TIMEZONE_MIN_MIN
                            || timezoneMinute > GregorianCalendarSystem.TIMEZONE_MIN_MAX) {
                        throw new HyracksDataException(errorMessage + ": time zone minute " + timezoneMinute);
                    }

                    if (date.charAt(offset) == '-')
                        timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
                    else
                        timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
                }
            }
        }

        long chrononTimeInMs = GregorianCalendarSystem.getInstance().getChronon(year, month, day, 0, 0, 0, 0, timezone);
        aDate.setValue((int) (chrononTimeInMs / GregorianCalendarSystem.CHRONON_OF_DAY));
        if (chrononTimeInMs < 0 && chrononTimeInMs % GregorianCalendarSystem.CHRONON_OF_DAY != 0)
            aDate.setValue((int) (chrononTimeInMs / GregorianCalendarSystem.CHRONON_OF_DAY) - 1);
    }
}
