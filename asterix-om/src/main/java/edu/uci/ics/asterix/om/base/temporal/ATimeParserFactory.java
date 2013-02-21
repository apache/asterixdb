/*
 * Copyright 2009-2011 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.om.base.temporal;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class ATimeParserFactory implements IValueParserFactory {

    public static final IValueParserFactory INSTANCE = new ATimeParserFactory();

    private static final long serialVersionUID = 1L;

    private static final String timeErrorMessage = "Wrong Input Format for a Time Value";

    private ATimeParserFactory() {

    }

    @Override
    public IValueParser createValueParser() {

        final CharArrayCharSequenceAccessor charArrayAccessor = new CharArrayCharSequenceAccessor();

        return new IValueParser() {

            @Override
            public void parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
                charArrayAccessor.reset(buffer, start, length);
                try {
                    out.writeInt(parseTimePart(charArrayAccessor));
                } catch (IOException ex) {
                    throw new HyracksDataException(ex);
                }
            }
        };
    }

    /**
     * Parse the given char sequence as a time string, and return the milliseconds represented by the time.
     * 
     * @param charAccessor
     * @return
     * @throws Exception
     */
    public static <T> int parseTimePart(ICharSequenceAccessor<T> charAccessor) throws HyracksDataException {

        int length = charAccessor.getLength();
        int offset = 0;

        int hour = 0, min = 0, sec = 0, millis = 0;
        int timezone = 0;

        boolean isExtendedForm = false;
        if (charAccessor.getCharAt(offset + 2) == ':') {
            isExtendedForm = true;
        }

        if (isExtendedForm && (charAccessor.getCharAt(offset + 2) != ':' || charAccessor.getCharAt(offset + 5) != ':')) {
            throw new HyracksDataException(timeErrorMessage + ": Missing colon in an extended time format.");
        }
        // hour
        for (int i = 0; i < 2; i++) {
            if ((charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9')) {
                hour = hour * 10 + charAccessor.getCharAt(offset + i) - '0';
            } else {
                throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in hour field");
            }
        }

        if (hour < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.HOUR.ordinal()]
                || hour > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.HOUR.ordinal()]) {
            throw new HyracksDataException(timeErrorMessage + ": hour " + hour);
        }

        offset += (isExtendedForm) ? 3 : 2;

        // minute
        for (int i = 0; i < 2; i++) {
            if ((charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9')) {
                min = min * 10 + charAccessor.getCharAt(offset + i) - '0';
            } else {
                throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in minute field");
            }
        }

        if (min < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.MINUTE.ordinal()]
                || min > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.MINUTE.ordinal()]) {
            throw new HyracksDataException(timeErrorMessage + ": min " + min);
        }

        offset += (isExtendedForm) ? 3 : 2;

        // second
        for (int i = 0; i < 2; i++) {
            if ((charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9')) {
                sec = sec * 10 + charAccessor.getCharAt(offset + i) - '0';
            } else {
                throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in second field");
            }
        }

        if (sec < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.SECOND.ordinal()]
                || sec > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.SECOND.ordinal()]) {
            throw new HyracksDataException(timeErrorMessage + ": sec " + sec);
        }

        offset += 2;

        if ((isExtendedForm && length > offset && charAccessor.getCharAt(offset) == '.')
                || (!isExtendedForm && length > offset)) {

            offset += (isExtendedForm) ? 1 : 0;
            int i = 0;
            for (; i < 3 && offset + i < length; i++) {
                if (charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9') {
                    millis = millis * 10 + charAccessor.getCharAt(offset + i) - '0';
                } else {
                    break;
                }
            }

            offset += i;

            for (; i < 3; i++) {
                millis = millis * 10;
            }

            // error is thrown if more than three digits are seen for the millisecond part
            if (charAccessor.getLength() > offset && charAccessor.getCharAt(offset) >= '0'
                    && charAccessor.getCharAt(offset) <= '9') {
                throw new HyracksDataException(timeErrorMessage + ": too many fields for millisecond.");
            }
        }

        if (length > offset) {
            timezone = parseTimezonePart(charAccessor, offset);
        }

        return GregorianCalendarSystem.getInstance().getChronon(hour, min, sec, millis, timezone);
    }

    /**
     * Parse the given char sequence as a time string, and return the milliseconds represented by the time.
     * 
     * @param charAccessor
     * @return
     * @throws Exception
     */
    public static <T> int parseTimezonePart(ICharSequenceAccessor<T> charAccessor, int offset)
            throws HyracksDataException {
        int timezone = 0;

        if (charAccessor.getCharAt(offset) != 'Z') {
            if ((charAccessor.getCharAt(offset) != '+' && charAccessor.getCharAt(offset) != '-')) {
                throw new HyracksDataException("Wrong timezone format: missing sign or missing colon for a time zone");
            }

            short timezoneHour = 0;
            short timezoneMinute = 0;

            for (int i = 0; i < 2; i++) {
                if ((charAccessor.getCharAt(offset + 1 + i) >= '0' && charAccessor.getCharAt(offset + 1 + i) <= '9')) {
                    timezoneHour = (short) (timezoneHour * 10 + charAccessor.getCharAt(offset + 1 + i) - '0');
                } else {
                    throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in timezone hour field");
                }
            }

            if (timezoneHour < GregorianCalendarSystem.TIMEZONE_HOUR_MIN
                    || timezoneHour > GregorianCalendarSystem.TIMEZONE_HOUR_MAX) {
                throw new HyracksDataException(timeErrorMessage + ": time zone hour " + timezoneHour);
            }

            int temp_offset = (charAccessor.getCharAt(offset + 3) == ':') ? 1 : 0;

            for (int i = 0; i < 2; i++) {
                if ((charAccessor.getCharAt(offset + temp_offset + 3 + i) >= '0' && charAccessor.getCharAt(offset
                        + temp_offset + 3 + i) <= '9')) {
                    timezoneMinute = (short) (timezoneMinute * 10
                            + charAccessor.getCharAt(offset + temp_offset + 3 + i) - '0');
                } else {
                    throw new HyracksDataException(timeErrorMessage + ": Non-numeric value in timezone minute field");
                }
            }

            if (timezoneMinute < GregorianCalendarSystem.TIMEZONE_MIN_MIN
                    || timezoneMinute > GregorianCalendarSystem.TIMEZONE_MIN_MAX) {
                throw new HyracksDataException(timeErrorMessage + ": time zone minute " + timezoneMinute);
            }

            if (charAccessor.getCharAt(offset) == '-') {
                timezone = (byte) -((timezoneHour * 4) + timezoneMinute / 15);
            } else {
                timezone = (byte) ((timezoneHour * 4) + timezoneMinute / 15);
            }
        }
        return timezone;
    }

}
