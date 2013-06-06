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
    private static final AMutableInterval aInterval = new AMutableInterval(0L, 0L, (byte) 0);
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
        return AInt64SerializerDeserializer.getLong(data, offset + getIntervalStartOffset());
    }

    public static long getIntervalEnd(byte[] data, int offset) {
        return AInt64SerializerDeserializer.getLong(data, offset + getIntervalEndOffset());
    }

    public static int getIntervalStartOffset() {
        return 0;
    }

    public static int getIntervalEndOffset() {
        return 8;
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

        long chrononTimeInMsStart = 0;
        long chrononTimeInMsEnd = 0;
        try {

            // the starting point for parsing (so for the accessor)
            int startOffset = 0;
            int endOffset, timeSeperatorOffsetInDatetimeString;

            // Get the index for the comma
            int commaIndex = interval.indexOf(',');
            if (commaIndex < 1) {
                throw new AlgebricksException("comma is missing for a string of interval");
            }

            endOffset = commaIndex - 1;

            timeSeperatorOffsetInDatetimeString = interval.indexOf('T');

            if (timeSeperatorOffsetInDatetimeString < 0) {
                throw new AlgebricksException(errorMessage + ": missing T for a datetime value.");
            }

            chrononTimeInMsStart = parseDatePart(interval, startOffset, timeSeperatorOffsetInDatetimeString - 1);

            chrononTimeInMsStart += parseTimePart(interval, timeSeperatorOffsetInDatetimeString + 1, endOffset);

            // Interval End
            startOffset = commaIndex + 1;
            endOffset = interval.length() - 1;

            timeSeperatorOffsetInDatetimeString = interval.indexOf('T', startOffset);

            if (timeSeperatorOffsetInDatetimeString < 0) {
                throw new AlgebricksException(errorMessage + ": missing T for a datetime value.");
            }

            chrononTimeInMsEnd = parseDatePart(interval, startOffset, timeSeperatorOffsetInDatetimeString - 1);

            chrononTimeInMsEnd += parseTimePart(interval, timeSeperatorOffsetInDatetimeString + 1, endOffset);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        aInterval.setValue(chrononTimeInMsStart, chrononTimeInMsEnd, ATypeTag.DATETIME.serialize());

        intervalSerde.serialize(aInterval, out);
    }

    public static void parseTime(String interval, DataOutput out) throws HyracksDataException {

        long chrononTimeInMsStart = 0;
        long chrononTimeInMsEnd = 0;
        try {

            int startOffset = 0;
            int endOffset;

            // Get the index for the comma
            int commaIndex = interval.indexOf(',');
            if (commaIndex < 0) {
                throw new AlgebricksException("comma is missing for a string of interval");
            }

            endOffset = commaIndex - 1;

            // Interval Start
            chrononTimeInMsStart = parseTimePart(interval, startOffset, endOffset);

            if (chrononTimeInMsStart < 0) {
                chrononTimeInMsStart += GregorianCalendarSystem.CHRONON_OF_DAY;
            }

            // Interval End
            startOffset = commaIndex + 1;
            endOffset = interval.length() - 1;

            chrononTimeInMsEnd = parseTimePart(interval, startOffset, endOffset);

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

        long chrononTimeInMsStart = 0;
        long chrononTimeInMsEnd = 0;
        try {

            // the starting point for parsing (so for the accessor)
            int startOffset = 0;
            int endOffset;

            // Get the index for the comma
            int commaIndex = interval.indexOf(',');
            if (commaIndex < 1) {
                throw new AlgebricksException("comma is missing for a string of interval");
            }

            endOffset = commaIndex - 1;

            chrononTimeInMsStart = parseDatePart(interval, startOffset, endOffset);

            // Interval End
            startOffset = commaIndex + 1;
            endOffset = interval.length() - 1;

            chrononTimeInMsEnd = parseDatePart(interval, startOffset, endOffset);

        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        aInterval.setValue((chrononTimeInMsStart / GregorianCalendarSystem.CHRONON_OF_DAY),
                (chrononTimeInMsEnd / GregorianCalendarSystem.CHRONON_OF_DAY), ATypeTag.DATE.serialize());

        intervalSerde.serialize(aInterval, out);
    }

    private static long parseDatePart(String interval, int startOffset, int endOffset) throws AlgebricksException,
            HyracksDataException {

        while (interval.charAt(endOffset) == '"' || interval.charAt(endOffset) == ' ') {
            endOffset--;
        }

        while (interval.charAt(startOffset) == '"' || interval.charAt(startOffset) == ' ') {
            startOffset++;
        }

        return ADateParserFactory.parseDatePart(interval, startOffset, endOffset - startOffset + 1);
    }

    private static int parseTimePart(String interval, int startOffset, int endOffset) throws AlgebricksException,
            HyracksDataException {

        while (interval.charAt(endOffset) == '"' || interval.charAt(endOffset) == ' ') {
            endOffset--;
        }

        while (interval.charAt(startOffset) == '"' || interval.charAt(startOffset) == ' ') {
            startOffset++;
        }

        return ATimeParserFactory.parseTimePart(interval, startOffset, endOffset - startOffset + 1);
    }

}
