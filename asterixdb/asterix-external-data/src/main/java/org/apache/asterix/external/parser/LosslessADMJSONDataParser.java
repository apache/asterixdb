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
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ATaggedValuePrinter;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.util.bytes.HexParser;

import com.fasterxml.jackson.core.JsonFactory;

public final class LosslessADMJSONDataParser extends JSONDataParser {

    private AMutablePoint[] polygonPoints;

    public LosslessADMJSONDataParser(JsonFactory jsonFactory) {
        super(RecordUtil.FULLY_OPEN_RECORD_TYPE, jsonFactory);
    }

    @Override
    protected void serializeNumeric(ATypeTag numericType, DataOutput out) throws IOException {
        super.serializeNumeric(ATypeTag.BIGINT, out);
    }

    @Override
    protected void serializeString(ATypeTag stringVariantType, DataOutput out) throws IOException {
        char[] textChars = jsonParser.getTextCharacters();
        int textOffset = jsonParser.getTextOffset();
        int textLength = jsonParser.getTextLength();

        ATypeTag typeToUse = parseTypeTag(textChars, textOffset, textLength, aInt32);
        if (typeToUse == null) {
            throw new RuntimeDataException(ErrorCode.RECORD_READER_MALFORMED_INPUT_STREAM);
        }
        int parsedLength = aInt32.getIntegerValue();
        int nonTaggedTextOffset = textOffset + parsedLength;
        int nonTaggedTextLength = textLength - parsedLength;
        switch (typeToUse) {
            case MISSING:
                missingSerde.serialize(AMissing.MISSING, out);
                break;
            case NULL:
                nullSerde.serialize(ANull.NULL, out);
                break;
            case BOOLEAN:
                parseInt64(textChars, nonTaggedTextOffset, nonTaggedTextLength, aInt64);
                booleanSerde.serialize(ABoolean.valueOf(aInt64.getLongValue() != 0), out);
                break;
            case TINYINT:
                parseInt64(textChars, nonTaggedTextOffset, nonTaggedTextLength, aInt64);
                aInt8.setValue((byte) aInt64.getLongValue());
                int8Serde.serialize(aInt8, out);
                break;
            case SMALLINT:
                parseInt64(textChars, nonTaggedTextOffset, nonTaggedTextLength, aInt64);
                aInt16.setValue((short) aInt64.getLongValue());
                int16Serde.serialize(aInt16, out);
                break;
            case INTEGER:
                parseInt64(textChars, nonTaggedTextOffset, nonTaggedTextLength, aInt64);
                aInt32.setValue((int) aInt64.getLongValue());
                int32Serde.serialize(aInt32, out);
                break;
            case BIGINT:
                parseInt64(textChars, nonTaggedTextOffset, nonTaggedTextLength, aInt64);
                int64Serde.serialize(aInt64, out);
                break;
            case FLOAT:
                parseInt64(textChars, nonTaggedTextOffset, nonTaggedTextLength, aInt64);
                aFloat.setValue(Float.intBitsToFloat((int) aInt64.getLongValue()));
                floatSerde.serialize(aFloat, out);
                break;
            case DOUBLE:
                parseInt64(textChars, nonTaggedTextOffset, nonTaggedTextLength, aInt64);
                aDouble.setValue(Double.longBitsToDouble(aInt64.getLongValue()));
                doubleSerde.serialize(aDouble, out);
                break;
            case TIME:
                parseInt64(textChars, nonTaggedTextOffset, nonTaggedTextLength, aInt64);
                aTime.setValue((int) aInt64.getLongValue());
                timeSerde.serialize(aTime, out);
                break;
            case DATE:
                parseInt64(textChars, nonTaggedTextOffset, nonTaggedTextLength, aInt64);
                aDate.setValue((int) aInt64.getLongValue());
                dateSerde.serialize(aDate, out);
                break;
            case DATETIME:
                parseInt64(textChars, nonTaggedTextOffset, nonTaggedTextLength, aInt64);
                aDateTime.setValue(aInt64.getLongValue());
                datetimeSerde.serialize(aDateTime, out);
                break;
            case YEARMONTHDURATION:
                parseInt64(textChars, nonTaggedTextOffset, nonTaggedTextLength, aInt64);
                aYearMonthDuration.setMonths((int) aInt64.getLongValue());
                yearMonthDurationSerde.serialize(aYearMonthDuration, out);
                break;
            case DAYTIMEDURATION:
                parseInt64(textChars, nonTaggedTextOffset, nonTaggedTextLength, aInt64);
                aDayTimeDuration.setMilliseconds(aInt64.getLongValue());
                dayTimeDurationSerde.serialize(aDayTimeDuration, out);
                break;
            case DURATION:
                int delimIdx = findDelim(textChars, nonTaggedTextOffset, nonTaggedTextLength, BuiltinType.ADURATION);
                parseInt64(textChars, nonTaggedTextOffset, delimIdx - nonTaggedTextOffset, aInt64);
                int months = (int) aInt64.getLongValue();
                parseInt64(textChars, delimIdx + 1, textLength - delimIdx - 1, aInt64);
                long millis = aInt64.getLongValue();
                aDuration.setValue(months, millis);
                durationSerde.serialize(aDuration, out);
                break;
            case UUID:
                aUUID.parseUUIDString(textChars, nonTaggedTextOffset, nonTaggedTextLength);
                uuidSerde.serialize(aUUID, out);
                break;
            case STRING:
                parseString(textChars, nonTaggedTextOffset, nonTaggedTextLength, out);
                break;
            case BINARY:
                parseBase64BinaryString(textChars, nonTaggedTextOffset, nonTaggedTextLength, out);
                break;
            case POINT:
                delimIdx = findDelim(textChars, nonTaggedTextOffset, nonTaggedTextLength, BuiltinType.APOINT);
                parseInt64(textChars, nonTaggedTextOffset, delimIdx - nonTaggedTextOffset, aInt64);
                double x = Double.longBitsToDouble(aInt64.getLongValue());
                parseInt64(textChars, delimIdx + 1, textLength - delimIdx - 1, aInt64);
                double y = Double.longBitsToDouble(aInt64.getLongValue());
                aPoint.setValue(x, y);
                pointSerde.serialize(aPoint, out);
                break;
            case POINT3D:
                delimIdx = findDelim(textChars, nonTaggedTextOffset, nonTaggedTextLength, BuiltinType.APOINT3D);
                parseInt64(textChars, nonTaggedTextOffset, delimIdx - nonTaggedTextOffset, aInt64);
                x = Double.longBitsToDouble(aInt64.getLongValue());
                int delimIdx2 = findDelim(textChars, delimIdx + 1, textLength - delimIdx - 1, BuiltinType.APOINT3D);
                parseInt64(textChars, delimIdx + 1, delimIdx2 - delimIdx - 1, aInt64);
                y = Double.longBitsToDouble(aInt64.getLongValue());
                parseInt64(textChars, delimIdx2 + 1, textLength - delimIdx2 - 1, aInt64);
                double z = Double.longBitsToDouble(aInt64.getLongValue());
                aPoint3D.setValue(x, y, z);
                point3DSerde.serialize(aPoint3D, out);
                break;
            case CIRCLE:
                delimIdx = findDelim(textChars, nonTaggedTextOffset, nonTaggedTextLength, BuiltinType.ACIRCLE);
                parseInt64(textChars, nonTaggedTextOffset, delimIdx - nonTaggedTextOffset, aInt64);
                x = Double.longBitsToDouble(aInt64.getLongValue());
                delimIdx2 = findDelim(textChars, delimIdx + 1, textLength - delimIdx - 1, BuiltinType.ACIRCLE);
                parseInt64(textChars, delimIdx + 1, delimIdx2 - delimIdx - 1, aInt64);
                y = Double.longBitsToDouble(aInt64.getLongValue());
                parseInt64(textChars, delimIdx2 + 1, textLength - delimIdx2 - 1, aInt64);
                z = Double.longBitsToDouble(aInt64.getLongValue());
                aPoint.setValue(x, y);
                aCircle.setValue(aPoint, z);
                circleSerde.serialize(aCircle, out);
                break;
            case LINE:
                delimIdx = findDelim(textChars, nonTaggedTextOffset, nonTaggedTextLength, BuiltinType.ALINE);
                parseInt64(textChars, nonTaggedTextOffset, delimIdx - nonTaggedTextOffset, aInt64);
                x = Double.longBitsToDouble(aInt64.getLongValue());
                delimIdx2 = findDelim(textChars, delimIdx + 1, textLength - delimIdx - 1, BuiltinType.ALINE);
                parseInt64(textChars, delimIdx + 1, delimIdx2 - delimIdx - 1, aInt64);
                y = Double.longBitsToDouble(aInt64.getLongValue());
                int delimIdx3 = findDelim(textChars, delimIdx2 + 1, textLength - delimIdx2 - 1, BuiltinType.ALINE);
                parseInt64(textChars, delimIdx2 + 1, delimIdx3 - delimIdx2 - 1, aInt64);
                double x2 = Double.longBitsToDouble(aInt64.getLongValue());
                parseInt64(textChars, delimIdx3 + 1, textLength - delimIdx3 - 1, aInt64);
                double y2 = Double.longBitsToDouble(aInt64.getLongValue());
                aPoint.setValue(x, y);
                aPoint2.setValue(x2, y2);
                aLine.setValue(aPoint, aPoint2);
                lineSerde.serialize(aLine, out);
                break;
            case RECTANGLE:
                delimIdx = findDelim(textChars, nonTaggedTextOffset, nonTaggedTextLength, BuiltinType.ARECTANGLE);
                parseInt64(textChars, nonTaggedTextOffset, delimIdx - nonTaggedTextOffset, aInt64);
                x = Double.longBitsToDouble(aInt64.getLongValue());
                delimIdx2 = findDelim(textChars, delimIdx + 1, textLength - delimIdx - 1, BuiltinType.ARECTANGLE);
                parseInt64(textChars, delimIdx + 1, delimIdx2 - delimIdx - 1, aInt64);
                y = Double.longBitsToDouble(aInt64.getLongValue());
                delimIdx3 = findDelim(textChars, delimIdx2 + 1, textLength - delimIdx2 - 1, BuiltinType.ARECTANGLE);
                parseInt64(textChars, delimIdx2 + 1, delimIdx3 - delimIdx2 - 1, aInt64);
                x2 = Double.longBitsToDouble(aInt64.getLongValue());
                parseInt64(textChars, delimIdx3 + 1, textLength - delimIdx3 - 1, aInt64);
                y2 = Double.longBitsToDouble(aInt64.getLongValue());
                aPoint.setValue(x, y);
                aPoint2.setValue(x2, y2);
                aRectangle.setValue(aPoint, aPoint2);
                rectangleSerde.serialize(aRectangle, out);
                break;
            case POLYGON:
                delimIdx = findDelim(textChars, nonTaggedTextOffset, nonTaggedTextLength, BuiltinType.APOLYGON);
                parseInt64(textChars, nonTaggedTextOffset, delimIdx - nonTaggedTextOffset, aInt64);
                int numPoints = (int) aInt64.getLongValue();
                if (polygonPoints == null || polygonPoints.length != numPoints) {
                    polygonPoints = new AMutablePoint[numPoints];
                    for (int i = 0; i < numPoints; i++) {
                        polygonPoints[i] = new AMutablePoint(0, 0);
                    }
                }
                for (int i = 0; i < numPoints; i++) {
                    delimIdx2 = findDelim(textChars, delimIdx + 1, textLength - delimIdx - 1, BuiltinType.APOLYGON);
                    parseInt64(textChars, delimIdx + 1, delimIdx2 - delimIdx - 1, aInt64);
                    x = Double.longBitsToDouble(aInt64.getLongValue());
                    if (i < numPoints - 1) {
                        delimIdx3 =
                                findDelim(textChars, delimIdx2 + 1, textLength - delimIdx2 - 1, BuiltinType.APOLYGON);
                        parseInt64(textChars, delimIdx2 + 1, delimIdx3 - delimIdx2 - 1, aInt64);
                        delimIdx = delimIdx3;
                    } else {
                        parseInt64(textChars, delimIdx2 + 1, textLength - delimIdx2 - 1, aInt64);
                    }
                    y = Double.longBitsToDouble(aInt64.getLongValue());
                    polygonPoints[i].setValue(x, y);
                }
                aPolygon.setValue(polygonPoints);
                polygonSerde.serialize(aPolygon, out);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, "", typeToUse.toString());
        }
    }

    private static ATypeTag parseTypeTag(char[] textChars, int textOffset, int textLength,
            AMutableInt32 outParsedLength) {
        if (textLength == 0) {
            // empty string
            outParsedLength.setValue(0);
            return ATypeTag.STRING;
        }
        if (textChars[textOffset] == ATaggedValuePrinter.DELIMITER) {
            // any string
            outParsedLength.setValue(1);
            return ATypeTag.STRING;
        }
        // any type
        int typeTagLength = 2;
        if (textLength < typeTagLength) {
            return null;
        }
        byte typeTagByte;
        try {
            typeTagByte = HexParser.getByteFromValidHexChars(textChars[textOffset], textChars[textOffset + 1]);
        } catch (IllegalArgumentException e) {
            return null;
        }
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(typeTagByte);
        if (typeTag == null) {
            return null;
        }
        if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
            outParsedLength.setValue(typeTagLength);
            return typeTag;
        }
        int delimiterLength = 1;
        if (textLength < typeTagLength + delimiterLength) {
            return null;
        }
        if (textChars[textOffset + typeTagLength] != ATaggedValuePrinter.DELIMITER) {
            return null;
        }
        outParsedLength.setValue(typeTagLength + delimiterLength);
        return typeTag;
    }

    private int findDelim(char[] text, int offset, int len, BuiltinType type) throws ParseException {
        try {
            return indexOf(text, offset, len, ATaggedValuePrinter.DELIMITER);
        } catch (IllegalArgumentException e) {
            throw new ParseException(ErrorCode.PARSER_ADM_DATA_PARSER_WRONG_INSTANCE, new String(text, offset, len),
                    type.getTypeName());
        }
    }
}
