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

import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.base.temporal.ADateParserFactory;
import org.apache.asterix.om.base.temporal.ATimeParserFactory;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public abstract class AbstractDateTimeConstructorEvaluator extends AbstractConstructorEvaluator {

    private final AMutableDateTime aDateTime = new AMutableDateTime(0L);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADateTime> datetimeSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATETIME);
    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

    protected AbstractDateTimeConstructorEvaluator(IEvaluatorContext ctx, IScalarEvaluator inputEval,
            SourceLocation sourceLoc) {
        super(ctx, inputEval, sourceLoc);
    }

    @Override
    protected void evaluateImpl(IPointable result) throws HyracksDataException {
        byte[] bytes = inputArg.getByteArray();
        int startOffset = inputArg.getStartOffset();
        int len = inputArg.getLength();
        ATypeTag inputType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[startOffset]);
        switch (inputType) {
            case DATETIME:
                result.set(inputArg);
                break;
            case DATE:
                int chrononInDays = ADateSerializerDeserializer.getChronon(bytes, startOffset + 1);
                aDateTime.setValue(chrononInDays * GregorianCalendarSystem.CHRONON_OF_DAY);
                resultStorage.reset();
                datetimeSerde.serialize(aDateTime, out);
                result.set(resultStorage);
                break;
            case TIME:
                int chronon = ATimeSerializerDeserializer.getChronon(bytes, startOffset + 1);
                aDateTime.setValue(chronon);
                resultStorage.reset();
                datetimeSerde.serialize(aDateTime, out);
                result.set(resultStorage);
                break;
            case STRING:
                utf8Ptr.set(bytes, startOffset + 1, len - 1);
                if (parseDateTime(utf8Ptr, aDateTime)) {
                    resultStorage.reset();
                    datetimeSerde.serialize(aDateTime, out);
                    result.set(resultStorage);
                } else {
                    handleParseError(utf8Ptr, result);
                }
                break;
            default:
                handleUnsupportedType(inputType, result);
                break;
        }
    }

    protected boolean parseDateTime(UTF8StringPointable textPtr, AMutableDateTime result) {
        int stringLength = textPtr.getUTF8Length();
        // the string to be parsed should be at least 14 characters: YYYYMMDDhhmmss
        if (stringLength < 14) {
            return false;
        }
        byte[] bytes = textPtr.getByteArray();
        int charStartOffset = textPtr.getCharStartOffset();
        // +1 if it is negative (-)
        short timeOffset = (short) ((bytes[charStartOffset] == '-') ? 1 : 0);
        timeOffset += 8;
        if (bytes[charStartOffset + timeOffset] != 'T') {
            timeOffset += 2;
            if (bytes[charStartOffset + timeOffset] != 'T') {
                return false;
            }
        }
        try {
            long chronon = ADateParserFactory.parseDatePart(bytes, charStartOffset, timeOffset);
            int timeInMs = ATimeParserFactory.parseTimePart(bytes, charStartOffset + timeOffset + 1,
                    stringLength - timeOffset - 1);
            chronon += timeInMs;
            result.setValue(chronon);
            return true;
        } catch (HyracksDataException e) {
            return false;
        }
    }

    @Override
    protected final BuiltinType getTargetType() {
        return BuiltinType.ADATETIME;
    }
}
