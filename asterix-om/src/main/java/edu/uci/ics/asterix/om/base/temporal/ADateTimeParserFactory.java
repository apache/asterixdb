/*
 * Copyright 2009-2013 by The Regents of the University of California
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
            public void parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
                long chrononTimeInMs = 0;

                short timeOffset = (short) ((buffer[start] == '-') ? 1 : 0);

                timeOffset += 8;

                if (buffer[start + timeOffset] != 'T') {
                    timeOffset += 2;
                    if (buffer[start + timeOffset] != 'T') {
                        throw new HyracksDataException(dateTimeErrorMessage + ": missing T");
                    }
                }

                chrononTimeInMs = ADateParserFactory.parseDatePart(buffer, start, timeOffset);

                chrononTimeInMs += ATimeParserFactory.parseTimePart(buffer, start + timeOffset + 1, length - timeOffset
                        - 1);

                try {
                    out.writeLong(chrononTimeInMs);
                } catch (IOException ex) {
                    throw new HyracksDataException(ex);
                }
            }
        };
    }

}
