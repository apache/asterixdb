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
package org.apache.asterix.external.parser;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.external.api.IDataParser;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ACircle;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADayTimeDuration;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.ADuration;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AGeometry;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.ALine;
import org.apache.asterix.om.base.AMutableBinary;
import org.apache.asterix.om.base.AMutableCircle;
import org.apache.asterix.om.base.AMutableDate;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.base.AMutableDayTimeDuration;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableDuration;
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.base.AMutableGeometry;
import org.apache.asterix.om.base.AMutableInt16;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableInt8;
import org.apache.asterix.om.base.AMutableInterval;
import org.apache.asterix.om.base.AMutableLine;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.AMutablePoint3D;
import org.apache.asterix.om.base.AMutableRectangle;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AMutableTime;
import org.apache.asterix.om.base.AMutableUUID;
import org.apache.asterix.om.base.AMutableYearMonthDuration;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.APoint3D;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.ATime;
import org.apache.asterix.om.base.AUUID;
import org.apache.asterix.om.base.AYearMonthDuration;
import org.apache.asterix.om.base.temporal.ADateParserFactory;
import org.apache.asterix.om.base.temporal.ADurationParserFactory;
import org.apache.asterix.om.base.temporal.ADurationParserFactory.ADurationParseOption;
import org.apache.asterix.om.base.temporal.ATimeParserFactory;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.bytes.Base64Parser;
import org.apache.hyracks.util.bytes.HexParser;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

/**
 * Base class for data parsers. Includes the common set of definitions for
 * serializers/deserializers for built-in ADM types.
 */
public abstract class AbstractDataParser implements IDataParser {

    protected AMutableInt8 aInt8 = new AMutableInt8((byte) 0);
    protected AMutableInt16 aInt16 = new AMutableInt16((short) 0);
    protected AMutableInt32 aInt32 = new AMutableInt32(0);
    protected AMutableInt64 aInt64 = new AMutableInt64(0);
    protected AMutableDouble aDouble = new AMutableDouble(0);
    protected AMutableFloat aFloat = new AMutableFloat(0);
    protected AMutableString aString = new AMutableString("");
    protected AMutableBinary aBinary = new AMutableBinary(null, 0, 0);
    protected AMutableString aStringFieldName = new AMutableString("");
    protected AMutableUUID aUUID = new AMutableUUID();
    protected AMutableGeometry aGeomtry = new AMutableGeometry(null);
    // For temporal and spatial data types
    protected AMutableTime aTime = new AMutableTime(0);
    protected AMutableDateTime aDateTime = new AMutableDateTime(0L);
    protected AMutableDuration aDuration = new AMutableDuration(0, 0);
    protected AMutableDayTimeDuration aDayTimeDuration = new AMutableDayTimeDuration(0);
    protected AMutableYearMonthDuration aYearMonthDuration = new AMutableYearMonthDuration(0);
    protected AMutablePoint aPoint = new AMutablePoint(0, 0);
    protected AMutablePoint3D aPoint3D = new AMutablePoint3D(0, 0, 0);
    protected AMutableCircle aCircle = new AMutableCircle(null, 0);
    protected AMutableRectangle aRectangle = new AMutableRectangle(null, null);
    protected AMutablePoint aPoint2 = new AMutablePoint(0, 0);
    protected AMutableLine aLine = new AMutableLine(null, null);
    protected AMutableDate aDate = new AMutableDate(0);
    protected AMutableInterval aInterval = new AMutableInterval(0L, 0L, (byte) 0);

    // Serializers
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AString> stringSerde =
            SerializerDeserializerProvider.INSTANCE.getAStringSerializerDeserializer();
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ABinary> binarySerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABINARY);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AFloat> floatSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AFLOAT);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AInt8> int8Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AInt16> int16Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT16);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AInt32> int32Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ABoolean> booleanSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ANull> nullSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);

    protected final AStringSerializerDeserializer untaggedStringSerde =
            new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader());

    protected final HexParser hexParser = new HexParser();
    protected final Base64Parser base64Parser = new Base64Parser();

    // For UUID, we assume that the format is the string representation of UUID
    // (xxxxxxxx-xxxx-xxxx-xxxxxxxxxxxx) when parsing the data.
    // Thus, we need to call UUID.fromStringToAMutableUUID() to convert it to the internal representation (byte []).
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AUUID> uuidSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AUUID);

    protected ISerializerDeserializer<AGeometry> geomSerde =
            SerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(BuiltinType.AGEOMETRY);

    // To avoid race conditions, the serdes for temporal and spatial data types needs to be one per parser
    // ^^^^^^^^^^^^^^^^^^^^^^^^ ??? then why all these serdes are static?
    @SuppressWarnings("unchecked")
    protected static final ISerializerDeserializer<ATime> timeSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ATIME);
    @SuppressWarnings("unchecked")
    protected static final ISerializerDeserializer<ADate> dateSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATE);
    @SuppressWarnings("unchecked")
    protected static final ISerializerDeserializer<ADateTime> datetimeSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATETIME);
    @SuppressWarnings("unchecked")
    protected static final ISerializerDeserializer<ADuration> durationSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADURATION);
    @SuppressWarnings("unchecked")
    protected static final ISerializerDeserializer<ADayTimeDuration> dayTimeDurationSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADAYTIMEDURATION);
    @SuppressWarnings("unchecked")
    protected static final ISerializerDeserializer<AYearMonthDuration> yearMonthDurationSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AYEARMONTHDURATION);
    @SuppressWarnings("unchecked")
    protected final static ISerializerDeserializer<APoint> pointSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.APOINT);
    @SuppressWarnings("unchecked")
    protected final static ISerializerDeserializer<APoint3D> point3DSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.APOINT3D);
    @SuppressWarnings("unchecked")
    protected final static ISerializerDeserializer<ACircle> circleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ACIRCLE);
    @SuppressWarnings("unchecked")
    protected final static ISerializerDeserializer<ARectangle> rectangleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ARECTANGLE);
    @SuppressWarnings("unchecked")
    protected final static ISerializerDeserializer<ALine> lineSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ALINE);
    @SuppressWarnings("unchecked")
    protected static final ISerializerDeserializer<AInterval> intervalSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINTERVAL);

    protected String filename;

    void setFilename(String filename) {
        this.filename = filename;
    }

    protected void parseTime(char[] buffer, int begin, int len, DataOutput out) throws HyracksDataException {
        int chrononTimeInMs = ATimeParserFactory.parseTimePart(buffer, begin, len);
        aTime.setValue(chrononTimeInMs);
        timeSerde.serialize(aTime, out);
    }

    protected void parseDate(char[] buffer, int begin, int len, DataOutput out) throws HyracksDataException {
        long chrononTimeInMs = ADateParserFactory.parseDatePart(buffer, begin, len);
        short temp = 0;
        if (chrononTimeInMs < 0 && chrononTimeInMs % GregorianCalendarSystem.CHRONON_OF_DAY != 0) {
            temp = 1;
        }
        aDate.setValue((int) (chrononTimeInMs / GregorianCalendarSystem.CHRONON_OF_DAY) - temp);
        dateSerde.serialize(aDate, out);
    }

    protected void parseDateTime(char[] buffer, int begin, int len, DataOutput out) throws HyracksDataException {
        // +1 if it is negative (-)

        int timeOffset = (buffer[begin] == '-') ? 1 : 0;

        timeOffset = timeOffset + 8 + begin;

        if (buffer[timeOffset] != 'T') {
            timeOffset += 2;
            if (buffer[timeOffset] != 'T') {
                throw new ParseException(ErrorCode.PARSER_ADM_DATA_PARSER_INTERVAL_INVALID_DATETIME);
            }
        }
        long chrononTimeInMs = ADateParserFactory.parseDatePart(buffer, begin, timeOffset - begin);
        chrononTimeInMs += ATimeParserFactory.parseTimePart(buffer, timeOffset + 1, begin + len - timeOffset - 1);
        aDateTime.setValue(chrononTimeInMs);
        datetimeSerde.serialize(aDateTime, out);
    }

    protected void parseDuration(char[] buffer, int begin, int len, DataOutput out) throws HyracksDataException {
        ADurationParserFactory.parseDuration(buffer, begin, len, aDuration, ADurationParseOption.All);
        durationSerde.serialize(aDuration, out);
    }

    protected void parseDateTimeDuration(char[] buffer, int begin, int len, DataOutput out)
            throws HyracksDataException {
        ADurationParserFactory.parseDuration(buffer, begin, len, aDayTimeDuration, ADurationParseOption.All);
        dayTimeDurationSerde.serialize(aDayTimeDuration, out);
    }

    protected void parseYearMonthDuration(char[] buffer, int begin, int len, DataOutput out)
            throws HyracksDataException {
        ADurationParserFactory.parseDuration(buffer, begin, len, aYearMonthDuration, ADurationParseOption.All);
        yearMonthDurationSerde.serialize(aYearMonthDuration, out);
    }

    protected void parsePoint(char[] buffer, int begin, int len, DataOutput out) throws HyracksDataException {
        try {
            int commaIndex = indexOf(buffer, begin, len, ',');
            aPoint.setValue(parseDouble(buffer, begin, commaIndex - begin),
                    parseDouble(buffer, commaIndex + 1, begin + len - commaIndex - 1));
            pointSerde.serialize(aPoint, out);
        } catch (Exception e) {
            throw new ParseException(ErrorCode.PARSER_ADM_DATA_PARSER_WRONG_INSTANCE, e, new String(buffer, begin, len),
                    "point");
        }
    }

    protected void parse3DPoint(char[] buffer, int begin, int len, DataOutput out) throws HyracksDataException {
        try {
            int firstCommaIndex = indexOf(buffer, begin, len, ',');
            int secondCommaIndex = indexOf(buffer, firstCommaIndex + 1, begin + len - firstCommaIndex - 1, ',');
            aPoint3D.setValue(parseDouble(buffer, begin, firstCommaIndex - begin),
                    parseDouble(buffer, firstCommaIndex + 1, secondCommaIndex - firstCommaIndex - 1),
                    parseDouble(buffer, secondCommaIndex + 1, begin + len - secondCommaIndex - 1));
            point3DSerde.serialize(aPoint3D, out);
        } catch (Exception e) {
            throw new ParseException(ErrorCode.PARSER_ADM_DATA_PARSER_WRONG_INSTANCE, e, new String(buffer, begin, len),
                    "point3d");
        }
    }

    protected void parseCircle(char[] buffer, int begin, int len, DataOutput out) throws HyracksDataException {
        try {
            int firstCommaIndex = indexOf(buffer, begin, len, ',');
            int spaceIndex = indexOf(buffer, firstCommaIndex + 1, begin + len - firstCommaIndex - 1, ' ');
            aPoint.setValue(parseDouble(buffer, begin, firstCommaIndex - begin),
                    parseDouble(buffer, firstCommaIndex + 1, spaceIndex - firstCommaIndex - 1));
            aCircle.setValue(aPoint, parseDouble(buffer, spaceIndex + 1, begin + len - spaceIndex - 1));
            circleSerde.serialize(aCircle, out);
        } catch (Exception e) {
            throw new ParseException(ErrorCode.PARSER_ADM_DATA_PARSER_WRONG_INSTANCE, e, new String(buffer, begin, len),
                    "circle");
        }
    }

    protected void parseRectangle(char[] buffer, int begin, int len, DataOutput out) throws HyracksDataException {
        try {
            int spaceIndex = indexOf(buffer, begin, len, ' ');

            int firstCommaIndex = indexOf(buffer, begin, len, ',');
            aPoint.setValue(parseDouble(buffer, begin, firstCommaIndex - begin),
                    parseDouble(buffer, firstCommaIndex + 1, spaceIndex - firstCommaIndex - 1));

            int secondCommaIndex = indexOf(buffer, spaceIndex + 1, begin + len - spaceIndex - 1, ',');
            aPoint2.setValue(parseDouble(buffer, spaceIndex + 1, secondCommaIndex - spaceIndex - 1),
                    parseDouble(buffer, secondCommaIndex + 1, begin + len - secondCommaIndex - 1));
            if (aPoint.getX() > aPoint2.getX() && aPoint.getY() > aPoint2.getY()) {
                aRectangle.setValue(aPoint2, aPoint);
            } else if (aPoint.getX() < aPoint2.getX() && aPoint.getY() < aPoint2.getY()) {
                aRectangle.setValue(aPoint, aPoint2);
            } else {
                throw new IllegalArgumentException(
                        "Rectangle arugment must be either (bottom left point, top right point) or (top right point, bottom left point)");
            }
            rectangleSerde.serialize(aRectangle, out);
        } catch (Exception e) {
            throw new ParseException(ErrorCode.PARSER_ADM_DATA_PARSER_WRONG_INSTANCE, e, new String(buffer, begin, len),
                    "rectangle");
        }
    }

    protected void parseLine(char[] buffer, int begin, int len, DataOutput out) throws HyracksDataException {
        try {
            int spaceIndex = indexOf(buffer, begin, len, ' ');
            int firstCommaIndex = indexOf(buffer, begin, len, ',');
            aPoint.setValue(parseDouble(buffer, begin, firstCommaIndex - begin),
                    parseDouble(buffer, firstCommaIndex + 1, spaceIndex - firstCommaIndex - 1));
            int secondCommaIndex = indexOf(buffer, spaceIndex + 1, begin + len - spaceIndex - 1, ',');
            aPoint2.setValue(parseDouble(buffer, spaceIndex + 1, secondCommaIndex - spaceIndex - 1),
                    parseDouble(buffer, secondCommaIndex + 1, begin + len - secondCommaIndex - 1));
            aLine.setValue(aPoint, aPoint2);
            lineSerde.serialize(aLine, out);
        } catch (Exception e) {
            throw new ParseException(ErrorCode.PARSER_ADM_DATA_PARSER_WRONG_INSTANCE, e, new String(buffer, begin, len),
                    "line");
        }
    }

    protected void parseHexBinaryString(char[] input, int start, int length, DataOutput out)
            throws HyracksDataException {
        hexParser.generateByteArrayFromHexString(input, start, length);
        aBinary.setValue(hexParser.getByteArray(), 0, hexParser.getLength());
        binarySerde.serialize(aBinary, out);
    }

    protected void parseBase64BinaryString(char[] input, int start, int length, DataOutput out)
            throws HyracksDataException {
        base64Parser.generatePureByteArrayFromBase64String(input, start, length);
        aBinary.setValue(base64Parser.getByteArray(), 0, base64Parser.getLength());
        binarySerde.serialize(aBinary, out);
    }

    protected long parseDatePart(String interval, int startOffset, int endOffset) throws HyracksDataException {

        while (interval.charAt(endOffset) == '"' || interval.charAt(endOffset) == ' ') {
            endOffset--;
        }

        while (interval.charAt(startOffset) == '"' || interval.charAt(startOffset) == ' ') {
            startOffset++;
        }

        return ADateParserFactory.parseDatePart(interval, startOffset, endOffset - startOffset + 1);
    }

    protected int parseTimePart(String interval, int startOffset, int endOffset) throws HyracksDataException {

        while (interval.charAt(endOffset) == '"' || interval.charAt(endOffset) == ' ') {
            endOffset--;
        }

        while (interval.charAt(startOffset) == '"' || interval.charAt(startOffset) == ' ') {
            startOffset++;
        }

        return ATimeParserFactory.parseTimePart(interval, startOffset, endOffset - startOffset + 1);
    }

    protected double parseDouble(char[] buffer, int begin, int len) {
        // TODO: parse double directly from char[]
        String str = new String(buffer, begin, len);
        return Double.valueOf(str);
    }

    protected float parseFloat(char[] buffer, int begin, int len) {
        //TODO: pares float directly from char[]
        String str = new String(buffer, begin, len);
        return Float.valueOf(str);
    }

    protected int indexOf(char[] buffer, int begin, int len, char target) {
        for (int i = begin; i < begin + len; i++) {
            if (buffer[i] == target) {
                return i;
            }
        }
        throw new IllegalArgumentException("Cannot find " + target + " in " + new String(buffer, begin, len));
    }

    protected void parseString(char[] buffer, int begin, int length, DataOutput out) throws HyracksDataException {
        try {
            out.writeByte(ATypeTag.STRING.serialize());
            untaggedStringSerde.serialize(buffer, begin, length, out);
        } catch (IOException e) {
            throw new ParseException(e);
        }
    }
}
