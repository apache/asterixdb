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

public class ADateTimeParserFactory implements IValueParserFactory {

    public static final IValueParserFactory INSTANCE = new ADateTimeParserFactory();

    private static final long serialVersionUID = 1L;

    private static final String dateTimeErrorMessage = "Wrong Input Format for a DateTime Value";

    private ADateTimeParserFactory() {

    }

    @Override
    public IValueParser createValueParser() {

        final CharArrayCharSequenceAccessor charArrayAccessor = new CharArrayCharSequenceAccessor();

        return new IValueParser() {

            @Override
            public void parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
                long chrononTimeInMs = 0;

                charArrayAccessor.reset(buffer, start, length);

                short timeOffset = (short) ((charArrayAccessor.getCharAt(0) == '-') ? 1 : 0);

                if (charArrayAccessor.getCharAt(timeOffset + 10) != 'T'
                        && charArrayAccessor.getCharAt(timeOffset + 8) != 'T') {
                    throw new HyracksDataException(dateTimeErrorMessage + ": missing T");
                }

                // if extended form 11, else 9
                timeOffset += (charArrayAccessor.getCharAt(timeOffset + 13) == ':') ? (short) (11) : (short) (9);

                chrononTimeInMs = ADateParserFactory.parseDatePart(charArrayAccessor, false);

                charArrayAccessor.reset(buffer, start + timeOffset, length - timeOffset);

                chrononTimeInMs += ATimeParserFactory.parseTimePart(charArrayAccessor);

                try {
                    out.writeLong(chrononTimeInMs);
                } catch (IOException ex) {
                    throw new HyracksDataException(ex);
                }
            }
        };
    }

}
