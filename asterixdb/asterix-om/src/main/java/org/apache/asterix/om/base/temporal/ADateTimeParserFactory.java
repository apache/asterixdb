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

public class ADateTimeParserFactory implements IValueParserFactory {

    public static final IValueParserFactory INSTANCE = new ADateTimeParserFactory();

    private static final long serialVersionUID = 1L;

    private static final String dateTimeErrorMessage = "Wrong Input Format for a DateTime Value";

    private ADateTimeParserFactory() {

    }

    @Override
    public IValueParser createValueParser() {

        return new IValueParser() {

            @Override
            public boolean parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
                long parsedDateTime = parseDateTimePart(buffer, start, length);
                try {
                    out.writeLong(parsedDateTime);
                    return true;
                } catch (IOException ex) {
                    throw HyracksDataException.create(ex);
                }
            }
        };
    }

    /**
     * Parse the given char sequence as a datetime string, and return the milliseconds represented by the date and time.
     *
     * @param dateTimeString
     * @param start
     * @param length
     * @return
     * @throws Exception
     */
    public static long parseDateTimePart(String dateTimeString, int start, int length) throws HyracksDataException {
        long chrononTimeInMs = 0;

        short timeOffset = (short) ((dateTimeString.charAt(start) == '-') ? 1 : 0);

        timeOffset += 8;

        if (dateTimeString.charAt(start + timeOffset) != 'T') {
            timeOffset += 2;
            if (dateTimeString.charAt(start + timeOffset) != 'T') {
                throw new HyracksDataException(dateTimeErrorMessage + ": missing T");
            }
        }

        chrononTimeInMs = ADateParserFactory.parseDatePart(dateTimeString, start, timeOffset);

        return chrononTimeInMs
                + ATimeParserFactory.parseTimePart(dateTimeString, start + timeOffset + 1, length - timeOffset - 1);
    }

    /**
     * A copy-and-paste of {@link #parseDateTimePart(String, int, int)} but for a char array, in order
     * to avoid object creation.
     *
     * @param dateTimeString
     * @param start
     * @param length
     * @return
     * @throws HyracksDataException
     */
    public static long parseDateTimePart(char[] dateTimeString, int start, int length) throws HyracksDataException {
        long chrononTimeInMs = 0;

        short timeOffset = (short) ((dateTimeString[start] == '-') ? 1 : 0);

        timeOffset += 8;

        if (dateTimeString[start + timeOffset] != 'T') {
            timeOffset += 2;
            if (dateTimeString[start + timeOffset] != 'T') {
                throw new HyracksDataException(dateTimeErrorMessage + ": missing T");
            }
        }

        chrononTimeInMs = ADateParserFactory.parseDatePart(dateTimeString, start, timeOffset);

        return chrononTimeInMs
                + ATimeParserFactory.parseTimePart(dateTimeString, start + timeOffset + 1, length - timeOffset - 1);
    }
}
