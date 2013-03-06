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
package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInterval;
import edu.uci.ics.asterix.om.base.AMutableInterval;
import edu.uci.ics.asterix.om.base.temporal.ADateParserFactory;
import edu.uci.ics.asterix.om.base.temporal.ATimeParserFactory;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.base.temporal.StringCharSequenceAccessor;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class AIntervalSerializerDeserializer implements ISerializerDeserializer<AInterval> {

    private static final long serialVersionUID = 1L;

    public static final AIntervalSerializerDeserializer INSTANCE = new AIntervalSerializerDeserializer();
    @SuppressWarnings("unchecked")
    private static final ISerializerDeserializer<AInterval> intervalSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINTERVAL);

    private static final String errorMessage = "This can not be an instance of interval";

    private AIntervalSerializerDeserializer() {
    }

    @Override
    public AInterval deserialize(DataInput in) throws HyracksDataException {
        try {
            return new AInterval(in.readLong(), in.readLong(), in.readByte());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }

    }

    @Override
    public void serialize(AInterval instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeLong(instance.getIntervalStart());
            out.writeLong(instance.getIntervalEnd());
            out.writeByte(instance.getIntervalType());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }

    }

    public static long getIntervalStart(byte[] data, int offset) {
        return AInt64SerializerDeserializer.getLong(data, offset);
    }

    public static long getIntervalEnd(byte[] data, int offset) {
        return AInt64SerializerDeserializer.getLong(data, offset + 8);
    }

    public static byte getIntervalTimeType(byte[] data, int offset) {
        return data[offset + 8 * 2];
    }

    /**
     * create an interval value from two given datetime instance.
     * 
     * @param interval
     * @param out
     * @throws HyracksDataException
     */
    public static void parseDatetime(String interval, DataOutput out) throws HyracksDataException {
        AMutableInterval aInterval = new AMutableInterval(0l, 0l, (byte) 0);

        long chrononTimeInMsStart = 0;
        long chrononTimeInMsEnd = 0;
        try {

            StringCharSequenceAccessor charAccessor = new StringCharSequenceAccessor();

            // the starting point for parsing (so for the accessor)
            int startOffset = 0;
            int endOffset, timeSeperatorOffsetInDatetimeString;

            // Get the index for the comma
            int commaIndex = interval.indexOf(',');
            if (commaIndex < 1) {
                throw new AlgebricksException("comma is missing for a string of interval");
            }

            endOffset = commaIndex - 1;

            while (interval.charAt(endOffset) == ' ') {
                endOffset--;
            }

            while (interval.charAt(startOffset) == '"' || interval.charAt(startOffset) == ' ') {
                startOffset++;
            }

            charAccessor.reset(interval, startOffset, endOffset - startOffset + 1);

            // +1 if it is negative (-)
            timeSeperatorOffsetInDatetimeString = ((charAccessor.getCharAt(0) == '-') ? 1 : 0);

            timeSeperatorOffsetInDatetimeString += 8;

            if (charAccessor.getCharAt(timeSeperatorOffsetInDatetimeString) != 'T') {
                timeSeperatorOffsetInDatetimeString += 2;
                if (charAccessor.getCharAt(timeSeperatorOffsetInDatetimeString) != 'T') {
                    throw new AlgebricksException(errorMessage + ": missing T for a datetime value.");
                }
            }
            charAccessor.reset(interval, startOffset, timeSeperatorOffsetInDatetimeString);
            chrononTimeInMsStart = ADateParserFactory.parseDatePart(charAccessor);
            charAccessor.reset(interval, startOffset + timeSeperatorOffsetInDatetimeString + 1, endOffset
                    - (startOffset + timeSeperatorOffsetInDatetimeString + 1) + 1);
            chrononTimeInMsStart += ATimeParserFactory.parseTimePart(charAccessor);

            // Interval End
            startOffset = commaIndex + 1;
            endOffset = interval.length() - 1;
            while (interval.charAt(endOffset) == '"' || interval.charAt(endOffset) == ' ') {
                endOffset--;
            }

            while (interval.charAt(startOffset) == ' ') {
                startOffset++;
            }

            charAccessor.reset(interval, startOffset, endOffset - startOffset + 1);

            // +1 if it is negative (-)
            timeSeperatorOffsetInDatetimeString = ((charAccessor.getCharAt(0) == '-') ? 1 : 0);

            timeSeperatorOffsetInDatetimeString += 8;

            if (charAccessor.getCharAt(timeSeperatorOffsetInDatetimeString) != 'T') {
                timeSeperatorOffsetInDatetimeString += 2;
                if (charAccessor.getCharAt(timeSeperatorOffsetInDatetimeString) != 'T') {
                    throw new AlgebricksException(errorMessage + ": missing T for a datetime value.");
                }
            }
            charAccessor.reset(interval, startOffset, timeSeperatorOffsetInDatetimeString);
            chrononTimeInMsEnd = ADateParserFactory.parseDatePart(charAccessor);
            charAccessor.reset(interval, startOffset + timeSeperatorOffsetInDatetimeString + 1, endOffset
                    - (startOffset + timeSeperatorOffsetInDatetimeString + 1) + 1);
            chrononTimeInMsEnd += ATimeParserFactory.parseTimePart(charAccessor);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        aInterval.setValue(chrononTimeInMsStart, chrononTimeInMsEnd, ATypeTag.DATETIME.serialize());

        intervalSerde.serialize(aInterval, out);
    }

    public static void parseTime(String interval, DataOutput out) throws HyracksDataException {
        AMutableInterval aInterval = new AMutableInterval(0l, 0l, (byte) 0);

        long chrononTimeInMsStart = 0;
        long chrononTimeInMsEnd = 0;
        try {

            StringCharSequenceAccessor charAccessor = new StringCharSequenceAccessor();

            int startOffset = 0;
            int endOffset;

            // Get the index for the comma
            int commaIndex = interval.indexOf(',');
            if (commaIndex < 0) {
                throw new AlgebricksException("comma is missing for a string of interval");
            }

            endOffset = commaIndex - 1;

            while (interval.charAt(endOffset) == ' ') {
                endOffset--;
            }

            while (interval.charAt(startOffset) == '"' || interval.charAt(startOffset) == ' ') {
                startOffset++;
            }

            // Interval Start
            charAccessor.reset(interval, startOffset, endOffset - startOffset + 1);
            chrononTimeInMsStart = ATimeParserFactory.parseTimePart(charAccessor);

            if (chrononTimeInMsStart < 0) {
                chrononTimeInMsStart += GregorianCalendarSystem.CHRONON_OF_DAY;
            }

            // Interval End
            startOffset = commaIndex + 1;
            while (interval.charAt(startOffset) == ' ') {
                startOffset++;
            }

            endOffset = interval.length() - 1;

            while (interval.charAt(endOffset) == '"' || interval.charAt(endOffset) == ' ') {
                endOffset--;
            }

            charAccessor.reset(interval, startOffset, endOffset - startOffset + 1);
            chrononTimeInMsEnd = ATimeParserFactory.parseTimePart(charAccessor);

            if (chrononTimeInMsEnd < 0) {
                chrononTimeInMsEnd += GregorianCalendarSystem.CHRONON_OF_DAY;
            }

        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        aInterval.setValue(chrononTimeInMsStart, chrononTimeInMsEnd, ATypeTag.TIME.serialize());
        intervalSerde.serialize(aInterval, out);
    }

    public static void parseDate(String interval, DataOutput out) throws HyracksDataException {
        AMutableInterval aInterval = new AMutableInterval(0l, 0l, (byte) 0);

        long chrononTimeInMsStart = 0;
        long chrononTimeInMsEnd = 0;
        short tempStart = 0;
        short tempEnd = 0;
        try {
            StringCharSequenceAccessor charAccessor = new StringCharSequenceAccessor();

            // the starting point for parsing (so for the accessor)
            int startOffset = 0;
            int endOffset;

            // Get the index for the comma
            int commaIndex = interval.indexOf(',');
            if (commaIndex < 1) {
                throw new AlgebricksException("comma is missing for a string of interval");
            }

            endOffset = commaIndex - 1;

            while (interval.charAt(endOffset) == ' ') {
                endOffset--;
            }

            while (interval.charAt(startOffset) == '"' || interval.charAt(startOffset) == ' ') {
                startOffset++;
            }

            charAccessor.reset(interval, startOffset, endOffset - startOffset + 1);
            chrononTimeInMsStart = ADateParserFactory.parseDatePart(charAccessor);

            // Interval End
            startOffset = commaIndex + 1;
            endOffset = interval.length() - 1;
            while (interval.charAt(endOffset) == '"' || interval.charAt(endOffset) == ' ') {
                endOffset--;
            }

            while (interval.charAt(startOffset) == ' ') {
                startOffset++;
            }

            charAccessor.reset(interval, startOffset, endOffset - startOffset + 1);
            chrononTimeInMsEnd = ADateParserFactory.parseDatePart(charAccessor);

        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        aInterval.setValue((chrononTimeInMsStart / GregorianCalendarSystem.CHRONON_OF_DAY) - tempStart,
                (chrononTimeInMsEnd / GregorianCalendarSystem.CHRONON_OF_DAY) - tempEnd, ATypeTag.DATE.serialize());

        intervalSerde.serialize(aInterval, out);
    }

}
