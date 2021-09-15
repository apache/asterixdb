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

package org.apache.asterix.runtime.evaluators.constructors;

import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.ABinarySerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import org.apache.asterix.om.base.AUUID;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.common.NumberUtils;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.util.bytes.Base64Printer;

public abstract class AbstractStringConstructorEvaluator extends AbstractConstructorEvaluator {

    protected final UTF8StringBuilder builder = new UTF8StringBuilder();
    protected final GrowableArray baaos = new GrowableArray();
    protected final StringBuilder sb = new StringBuilder(32);

    protected AbstractStringConstructorEvaluator(IEvaluatorContext ctx, IScalarEvaluator inputEval,
            SourceLocation sourceLoc) {
        super(ctx, inputEval, sourceLoc);
    }

    @Override
    protected void evaluateImpl(IPointable result) throws HyracksDataException {
        byte[] bytes = inputArg.getByteArray();
        int startOffset = inputArg.getStartOffset();
        int len = inputArg.getLength();
        ATypeTag inputType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[startOffset]);
        if (inputType == ATypeTag.STRING) {
            result.set(inputArg);
            return;
        }
        try {
            baaos.reset();
            builder.reset(baaos, len);
            sb.setLength(0);
            switch (inputType) {
                case TINYINT:
                    int i = AInt8SerializerDeserializer.getByte(bytes, startOffset + 1);
                    sb.append(i);
                    builder.appendString(sb);
                    break;
                case SMALLINT:
                    i = AInt16SerializerDeserializer.getShort(bytes, startOffset + 1);
                    sb.append(i);
                    builder.appendString(sb);
                    break;
                case INTEGER:
                    i = AInt32SerializerDeserializer.getInt(bytes, startOffset + 1);
                    sb.append(i);
                    builder.appendString(sb);
                    break;
                case BIGINT:
                    long l = AInt64SerializerDeserializer.getLong(bytes, startOffset + 1);
                    sb.append(l);
                    builder.appendString(sb);
                    break;
                case DOUBLE:
                    double d = ADoubleSerializerDeserializer.getDouble(bytes, startOffset + 1);
                    if (Double.isNaN(d)) {
                        builder.appendUtf8StringPointable(NumberUtils.NAN);
                    } else if (d == Double.POSITIVE_INFINITY) { // NOSONAR
                        builder.appendUtf8StringPointable(NumberUtils.POSITIVE_INF);
                    } else if (d == Double.NEGATIVE_INFINITY) { // NOSONAR
                        builder.appendUtf8StringPointable(NumberUtils.NEGATIVE_INF);
                    } else {
                        sb.append(d);
                        builder.appendString(sb);
                    }
                    break;
                case FLOAT:
                    float f = AFloatSerializerDeserializer.getFloat(bytes, startOffset + 1);
                    if (Float.isNaN(f)) {
                        builder.appendUtf8StringPointable(NumberUtils.NAN);
                    } else if (f == Float.POSITIVE_INFINITY) { // NOSONAR
                        builder.appendUtf8StringPointable(NumberUtils.POSITIVE_INF);
                    } else if (f == Float.NEGATIVE_INFINITY) { // NOSONAR
                        builder.appendUtf8StringPointable(NumberUtils.NEGATIVE_INF);
                    } else {
                        sb.append(f);
                        builder.appendString(sb);
                    }
                    break;
                case BOOLEAN:
                    boolean b = ABooleanSerializerDeserializer.getBoolean(bytes, startOffset + 1);
                    builder.appendString(String.valueOf(b));
                    break;
                case DATE:
                    l = ADateSerializerDeserializer.getChronon(bytes, startOffset + 1)
                            * GregorianCalendarSystem.CHRONON_OF_DAY;
                    GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(l, sb,
                            GregorianCalendarSystem.Fields.YEAR, GregorianCalendarSystem.Fields.DAY);
                    builder.appendString(sb);
                    break;
                case TIME:
                    i = ATimeSerializerDeserializer.getChronon(bytes, startOffset + 1);
                    GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(i, sb,
                            GregorianCalendarSystem.Fields.HOUR, GregorianCalendarSystem.Fields.MILLISECOND);
                    builder.appendString(sb);
                    break;
                case DATETIME:
                    l = ADateTimeSerializerDeserializer.getChronon(bytes, startOffset + 1);
                    GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(l, sb,
                            GregorianCalendarSystem.Fields.YEAR, GregorianCalendarSystem.Fields.MILLISECOND);
                    builder.appendString(sb);
                    break;
                case YEARMONTHDURATION:
                    i = AYearMonthDurationSerializerDeserializer.getYearMonth(bytes, startOffset + 1);
                    GregorianCalendarSystem.getInstance().getDurationExtendStringRepWithTimezoneUntilField(0, i, sb);
                    builder.appendString(sb);
                    break;
                case DAYTIMEDURATION:
                    l = ADayTimeDurationSerializerDeserializer.getDayTime(bytes, startOffset + 1);
                    GregorianCalendarSystem.getInstance().getDurationExtendStringRepWithTimezoneUntilField(l, 0, sb);
                    builder.appendString(sb);
                    break;
                case DURATION:
                    i = ADurationSerializerDeserializer.getYearMonth(bytes, startOffset + 1);
                    l = ADurationSerializerDeserializer.getDayTime(bytes, startOffset + 1);
                    GregorianCalendarSystem.getInstance().getDurationExtendStringRepWithTimezoneUntilField(l, i, sb);
                    builder.appendString(sb);
                    break;
                case UUID:
                    AUUID.appendLiteralOnly(bytes, startOffset + 1, sb);
                    builder.appendString(sb);
                    break;
                case BINARY:
                    int contentLength = ABinarySerializerDeserializer.getContentLength(bytes, startOffset + 1);
                    int metaLength = ABinarySerializerDeserializer.getMetaLength(contentLength);
                    Base64Printer.printBase64Binary(bytes, startOffset + 1 + metaLength, contentLength, sb);
                    builder.appendString(sb);
                    break;
                default:
                    handleUnsupportedType(inputType, result);
                    return;
            }
            builder.finish();
            resultStorage.reset();
            out.write(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            out.write(baaos.getByteArray(), 0, baaos.getLength());
            result.set(resultStorage);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    protected BuiltinType getTargetType() {
        return BuiltinType.ASTRING;
    }
}
