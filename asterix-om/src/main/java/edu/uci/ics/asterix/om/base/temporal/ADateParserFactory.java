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

public class ADateParserFactory implements IValueParserFactory {

    public static final IValueParserFactory INSTANCE = new ADateParserFactory();

    private static final long serialVersionUID = 1L;

    private static final String dateErrorMessage = "Wrong input format for a date value";

    private ADateParserFactory() {

    }

    @Override
    public IValueParser createValueParser() {

        final CharArrayCharSequenceAccessor charArrayAccessor = new CharArrayCharSequenceAccessor();

        return new IValueParser() {

            @Override
            public void parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
                charArrayAccessor.reset(buffer, start, length);
                try {
                    out.writeInt((int) (parseDatePart(charArrayAccessor, true) / GregorianCalendarSystem.CHRONON_OF_DAY));
                } catch (IOException ex) {
                    throw new HyracksDataException(ex);
                }
            }
        };
    }

    /**
     * Parse the given char sequence as a date string, and return the milliseconds represented by the date.
     * 
     * @param charAccessor
     *            accessor for the char sequence
     * @param isDateOnly
     *            indicating whether it is a single date string, or it is the date part of a datetime string
     * @param errorMessage
     * @return
     * @throws Exception
     */
    public static <T> long parseDatePart(ICharSequenceAccessor<T> charAccessor, boolean isDateOnly)
            throws HyracksDataException {

        int length = charAccessor.getLength();
        int offset = 0;

        int year = 0, month = 0, day = 0;
        boolean positive = true;

        boolean isExtendedForm = false;

        if (charAccessor.getCharAt(offset) == '-') {
            offset++;
            positive = false;
        }

        if ((isDateOnly) && charAccessor.getCharAt(offset + 4) == '-' || (!isDateOnly)
                && charAccessor.getCharAt(offset + 13) == ':') {
            isExtendedForm = true;
        }

        if (isExtendedForm) {
            if (charAccessor.getCharAt(offset + 4) != '-' || charAccessor.getCharAt(offset + 7) != '-') {
                throw new HyracksDataException("Missing dash in the date string as an extended form");
            }
        }

        // year
        for (int i = 0; i < 4; i++) {
            if (charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9') {
                year = year * 10 + charAccessor.getCharAt(offset + i) - '0';
            } else {
                throw new HyracksDataException("Non-numeric value in year field");
            }
        }

        if (year < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.YEAR.ordinal()]
                || year > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.YEAR.ordinal()]) {
            throw new HyracksDataException(dateErrorMessage + ": year " + year);
        }

        offset += (isExtendedForm) ? 5 : 4;

        // month
        for (int i = 0; i < 2; i++) {
            if ((charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9')) {
                month = month * 10 + charAccessor.getCharAt(offset + i) - '0';
            } else {
                throw new HyracksDataException("Non-numeric value in month field");
            }
        }

        if (month < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.MONTH.ordinal()]
                || month > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.MONTH.ordinal()]) {
            throw new HyracksDataException(dateErrorMessage + ": month " + month);
        }
        offset += (isExtendedForm) ? 3 : 2;

        // day
        for (int i = 0; i < 2; i++) {
            if ((charAccessor.getCharAt(offset + i) >= '0' && charAccessor.getCharAt(offset + i) <= '9')) {
                day = day * 10 + charAccessor.getCharAt(offset + i) - '0';
            } else {
                throw new HyracksDataException("Non-numeric value in day field");
            }
        }

        if (day < GregorianCalendarSystem.FIELD_MINS[GregorianCalendarSystem.Fields.DAY.ordinal()]
                || day > GregorianCalendarSystem.FIELD_MAXS[GregorianCalendarSystem.Fields.DAY.ordinal()]) {
            throw new HyracksDataException(dateErrorMessage + ": day " + day);
        }

        offset += 2;

        if (!positive) {
            year *= -1;
        }

        if (isDateOnly && length > offset) {
            throw new HyracksDataException("Too many chars for a date only value");
        }
        return GregorianCalendarSystem.getInstance().getChronon(year, month, day, 0, 0, 0, 0, 0);
    }

}
