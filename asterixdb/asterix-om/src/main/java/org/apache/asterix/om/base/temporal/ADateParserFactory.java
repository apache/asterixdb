/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.om.base.temporal;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class ADateParserFactory implements IValueParserFactory {

    public static final IValueParserFactory INSTANCE = new ADateParserFactory();

    private static final long serialVersionUID = 1L;

    private static final String dateErrorMessage = "Wrong input format for a date value";

    private ADateParserFactory() {

    }

    @Override
    public IValueParser createValueParser() {

        return new IValueParser() {

            @Override
            public void parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
                try {
                    out.writeInt((int) (parseDatePart(buffer, start, length) / GregorianCalendarSystem.CHRONON_OF_DAY));
                } catch (IOException ex) {
                    throw HyracksDataException.create(ex);
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
    public static long parseDatePart(String dateString, int start, int length) throws HyracksDataException {

        int offset = 0;

        int year = 0, month = 0, day = 0;
        boolean positive = true;

        boolean isExtendedForm = false;

        if (dateString.charAt(start + offset) == '-') {
            offset++;
            positive = false;
        }

        if (dateString.charAt(start + offset + 4) == '-') {
            isExtendedForm = true;
        }

        if (isExtendedForm) {
            if (dateString.charAt(start + offset + 4) != '-' || dateString.charAt(start + offset + 7) != '-') {
                throw new HyracksDataException("Missing dash in the date string as an extended form");
            }
        }

        // year
        for (int i = 0; i < 4; i++) {
            if (dateString.charAt(start + offset + i) >= '0' && dateString.charAt(start + offset + i) <= '9') {
                year = year * 10 + dateString.charAt(start + offset + i) - '0';
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
            if ((dateString.charAt(start + offset + i) >= '0' && dateString.charAt(start + offset + i) <= '9')) {
                month = month * 10 + dateString.charAt(start + offset + i) - '0';
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
            if ((dateString.charAt(start + offset + i) >= '0' && dateString.charAt(start + offset + i) <= '9')) {
                day = day * 10 + dateString.charAt(start + offset + i) - '0';
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

        if (length > offset) {
            throw new HyracksDataException("Too many chars for a date only value");
        }

        if (!GregorianCalendarSystem.getInstance().validate(year, month, day, 0, 0, 0, 0)) {
            throw new HyracksDataException(dateErrorMessage);
        }

        return GregorianCalendarSystem.getInstance().getChronon(year, month, day, 0, 0, 0, 0, 0);
    }

    /**
     * A copy-and-paste of {@link #parseDatePart(String, int, int)} but for a char array, in order
     * to avoid object creation.
     *
     * @param dateString
     * @param start
     * @param length
     * @return
     * @throws HyracksDataException
     */
    public static long parseDatePart(char[] dateString, int start, int length) throws HyracksDataException {

        int offset = 0;

        int year = 0, month = 0, day = 0;
        boolean positive = true;

        boolean isExtendedForm = false;

        if (dateString[start + offset] == '-') {
            offset++;
            positive = false;
        }

        if (dateString[start + offset + 4] == '-') {
            isExtendedForm = true;
        }

        if (isExtendedForm) {
            if (dateString[start + offset + 4] != '-' || dateString[start + offset + 7] != '-') {
                throw new HyracksDataException("Missing dash in the date string as an extended form");
            }
        }

        // year
        for (int i = 0; i < 4; i++) {
            if (dateString[start + offset + i] >= '0' && dateString[start + offset + i] <= '9') {
                year = year * 10 + dateString[start + offset + i] - '0';
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
            if ((dateString[start + offset + i] >= '0' && dateString[start + offset + i] <= '9')) {
                month = month * 10 + dateString[start + offset + i] - '0';
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
            if ((dateString[start + offset + i] >= '0' && dateString[start + offset + i] <= '9')) {
                day = day * 10 + dateString[start + offset + i] - '0';
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

        if (length > offset) {
            throw new HyracksDataException("Too many chars for a date only value");
        }

        if (!GregorianCalendarSystem.getInstance().validate(year, month, day, 0, 0, 0, 0)) {
            throw new HyracksDataException(dateErrorMessage);
        }

        return GregorianCalendarSystem.getInstance().getChronon(year, month, day, 0, 0, 0, 0, 0);
    }

    /**
     * A copy-and-paste of {@link #parseDatePart(String, int, int)} but for a byte array, in order
     * to avoid object creation.
     *
     * @param dateString
     * @param start
     * @param length
     * @return
     * @throws HyracksDataException
     */
    public static long parseDatePart(byte[] dateString, int start, int length) throws HyracksDataException {

        int offset = 0;

        int year = 0, month = 0, day = 0;
        boolean positive = true;

        boolean isExtendedForm = false;

        if (dateString[start + offset] == '-') {
            offset++;
            positive = false;
        }

        if (dateString[start + offset + 4] == '-') {
            isExtendedForm = true;
        }

        if (isExtendedForm) {
            if (dateString[start + offset + 4] != '-' || dateString[start + offset + 7] != '-') {
                throw new HyracksDataException("Missing dash in the date string as an extended form");
            }
        }

        // year
        for (int i = 0; i < 4; i++) {
            if (dateString[start + offset + i] >= '0' && dateString[start + offset + i] <= '9') {
                year = year * 10 + dateString[start + offset + i] - '0';
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
            if ((dateString[start + offset + i] >= '0' && dateString[start + offset + i] <= '9')) {
                month = month * 10 + dateString[start + offset + i] - '0';
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
            if ((dateString[start + offset + i] >= '0' && dateString[start + offset + i] <= '9')) {
                day = day * 10 + dateString[start + offset + i] - '0';
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

        if (length > offset) {
            throw new HyracksDataException("Too many chars for a date only value");
        }

        if (!GregorianCalendarSystem.getInstance().validate(year, month, day, 0, 0, 0, 0)) {
            throw new HyracksDataException(dateErrorMessage);
        }

        return GregorianCalendarSystem.getInstance().getChronon(year, month, day, 0, 0, 0, 0, 0);
    }
}
