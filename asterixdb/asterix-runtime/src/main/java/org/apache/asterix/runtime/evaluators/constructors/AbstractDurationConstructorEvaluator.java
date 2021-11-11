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

import org.apache.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADuration;
import org.apache.asterix.om.base.AMutableDuration;
import org.apache.asterix.om.base.temporal.ADurationParserFactory;
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

public abstract class AbstractDurationConstructorEvaluator extends AbstractConstructorEvaluator {

    private final AMutableDuration aDuration = new AMutableDuration(0, 0);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADuration> durationSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADURATION);
    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

    protected AbstractDurationConstructorEvaluator(IEvaluatorContext ctx, IScalarEvaluator inputEval,
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
            case DURATION:
                result.set(inputArg);
                break;
            case YEARMONTHDURATION:
                int months = AYearMonthDurationSerializerDeserializer.getYearMonth(bytes, startOffset + 1);
                aDuration.setValue(months, 0);
                resultStorage.reset();
                durationSerde.serialize(aDuration, out);
                result.set(resultStorage);
                break;
            case DAYTIMEDURATION:
                long millis = ADayTimeDurationSerializerDeserializer.getDayTime(bytes, startOffset + 1);
                aDuration.setValue(0, millis);
                resultStorage.reset();
                durationSerde.serialize(aDuration, out);
                result.set(resultStorage);
                break;
            case STRING:
                utf8Ptr.set(bytes, startOffset + 1, len - 1);
                if (parseDuration(utf8Ptr, aDuration)) {
                    resultStorage.reset();
                    durationSerde.serialize(aDuration, out);
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

    @Override
    protected final BuiltinType getTargetType() {
        return BuiltinType.ADURATION;
    }

    private static boolean parseDuration(UTF8StringPointable textPtr, ADuration result) {
        try {
            ADurationParserFactory.parseDuration(textPtr.getByteArray(), textPtr.getCharStartOffset(),
                    textPtr.getUTF8Length(), result, ADurationParserFactory.ADurationParseOption.All);
            return true;
        } catch (HyracksDataException e) {
            return false;
        }
    }
}
