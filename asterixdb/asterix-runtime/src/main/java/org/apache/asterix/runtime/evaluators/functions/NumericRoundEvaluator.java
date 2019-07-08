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

package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * This function rounds the numeric values. It receives 2 arguments, first is the numeric value to be rounded, second
 * is an optional parameter to specify what digit to round to. If the provided rounding digit is positive, then the
 * rounding should be at digits to the right of the decimal point, and if the rounding digit is negative, then the
 * rounding should be at digits to the left of the decimal point.
 *
 * The return type behavior is as follows:
 * int8, int16, int32 and int64 -> int64 is returned
 * float -> float is returned
 * double -> double is returned
 * others -> null is returned
 *
 * Below is the formula used for rounding in the possible cases:
 *
 * Case 1: rounding digit is not provided or 0
 *  - Normal Math.round(123.5) -> 124
 *
 * Case 2: rounding digit is positive
 *  - multiplier = 10 ^ digit
 *  - Math.round(value * multiplier) / multiplier
 *  Example:
 *      - round(1.456, 2)
 *      - multiplier = 10 ^ 2 = 100
 *      - value = value * multiplier = 1.456 * 100 = 145.6
 *      - value = Math.round(value) = Math.round(145.6) = 146
 *      - value = value / multiplier = 146 / 100 = 1.46 <--- final result
 *
 * Case 3: rounding digit is negative
 *  - multiplier = 10 ^ Math.abs(digit)
 *  - Math.round(value / multiplier) * multiplier
 *  Example:
 *      - round(1255, -2)
 *      - multiplier = 10 ^ Math.abs(-2) = 100
 *      - value = value / multiplier = 1255 / 100 = 12.55
 *      - value = Math.round(value) = Math.around(12.55) = 13
 *      - value = value * multiplier = 13 * 100 = 1300 <--- final result
 *
 * Important:
 * When dealing with big round decimal points, there is a potential of precision loss.
 */

class NumericRoundEvaluator extends AbstractScalarEval {

    // Result members
    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private AMutableInt64 aInt64 = new AMutableInt64(0);
    private AMutableFloat aFloat = new AMutableFloat(0);
    private AMutableDouble aDouble = new AMutableDouble(0);

    // Evaluators and Pointables
    private final IScalarEvaluator valueEvaluator;
    private IScalarEvaluator roundingDigitEvaluator;
    private final IPointable valuePointable;
    private IPointable roundingDigitPointable;

    // Serializers/Deserializers
    @SuppressWarnings("rawtypes")
    protected ISerializerDeserializer int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("rawtypes")
    protected ISerializerDeserializer floatSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AFLOAT);
    @SuppressWarnings("rawtypes")
    protected ISerializerDeserializer doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    NumericRoundEvaluator(IEvaluatorContext context, IScalarEvaluatorFactory[] argEvaluatorFactories,
            FunctionIdentifier functionIdentifier, SourceLocation sourceLocation) throws HyracksDataException {
        super(sourceLocation, functionIdentifier);

        valueEvaluator = argEvaluatorFactories[0].createScalarEvaluator(context);
        valuePointable = new VoidPointable();

        // 2nd argument if it exists
        if (argEvaluatorFactories.length > 1) {
            roundingDigitEvaluator = argEvaluatorFactories[1].createScalarEvaluator(context);
            roundingDigitPointable = new VoidPointable();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        valueEvaluator.evaluate(tuple, valuePointable);

        if (roundingDigitEvaluator != null) {
            roundingDigitEvaluator.evaluate(tuple, roundingDigitPointable);
        }

        if (PointableHelper.checkAndSetMissingOrNull(result, valuePointable, roundingDigitPointable)) {
            return;
        }

        byte[] valueBytes = valuePointable.getByteArray();
        int valueOffset = valuePointable.getStartOffset();
        ATypeTag valueTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(valueBytes[valueOffset]);

        byte[] roundingDigitBytes = null;
        int roundingDigitOffset = 0;

        if (roundingDigitEvaluator != null) {
            roundingDigitBytes = roundingDigitPointable.getByteArray();
            roundingDigitOffset = roundingDigitPointable.getStartOffset();
        }

        // Validity of arguments
        if (!ATypeHierarchy.canPromote(valueTypeTag, ATypeTag.DOUBLE)) {
            PointableHelper.setNull(result);
            return;
        }

        // Validity of arguments
        if (roundingDigitEvaluator != null
                && !PointableHelper.isValidLongValue(roundingDigitBytes, roundingDigitOffset, true)) {
            PointableHelper.setNull(result);
            return;
        }

        // If we don't have the second argument, then rounding digit is 0, otherwise, read it from argument
        long roundingDigit = roundingDigitEvaluator == null ? 0
                : ATypeHierarchy.getLongValue(functionIdentifier.getName(), 2, roundingDigitBytes, roundingDigitOffset);

        // Right of decimal
        if (roundingDigit >= 0) {

            // Multiplier based on round digit
            double multiplier = Math.pow(10, roundingDigit);

            switch (valueTypeTag) {
                // For zero and positive digit rounding, no need to do anything for integers
                case TINYINT:
                    aInt64.setValue(AInt8SerializerDeserializer.getByte(valueBytes, valueOffset + 1));
                    int64Serde.serialize(aInt64, resultStorage.getDataOutput());
                    break;
                case SMALLINT:
                    aInt64.setValue(AInt16SerializerDeserializer.getShort(valueBytes, valueOffset + 1));
                    int64Serde.serialize(aInt64, resultStorage.getDataOutput());
                    break;
                case INTEGER:
                    aInt64.setValue(AInt32SerializerDeserializer.getInt(valueBytes, valueOffset + 1));
                    int64Serde.serialize(aInt64, resultStorage.getDataOutput());
                    break;
                case BIGINT:
                    resultStorage.set(valuePointable);
                    break;
                case FLOAT:
                    float floatValue = AFloatSerializerDeserializer.getFloat(valueBytes, valueOffset + 1);
                    // Maintain float precision in next operation
                    aFloat.setValue(Math.round(floatValue * (float) multiplier) / (float) multiplier);
                    floatSerde.serialize(aFloat, resultStorage.getDataOutput());
                    break;
                case DOUBLE:
                    double doubleValue = ADoubleSerializerDeserializer.getDouble(valueBytes, valueOffset + 1);
                    aDouble.setValue(Math.round(doubleValue * multiplier) / multiplier);
                    doubleSerde.serialize(aDouble, resultStorage.getDataOutput());
                    break;
                default:
                    PointableHelper.setNull(result);
                    return;
            }
        }
        // Left of decimal (negative roundingDigit value)
        else {
            // Multiplier based on round digit (convert to positive digit)
            double multiplier = Math.pow(10, -roundingDigit);

            switch (valueTypeTag) {
                case TINYINT:
                    byte byteValue = AInt8SerializerDeserializer.getByte(valueBytes, valueOffset + 1);
                    aInt64.setValue((long) (Math.round(byteValue / multiplier) * multiplier));
                    int64Serde.serialize(aInt64, resultStorage.getDataOutput());
                    break;
                case SMALLINT:
                    short shortValue = AInt16SerializerDeserializer.getShort(valueBytes, valueOffset + 1);
                    aInt64.setValue((long) (Math.round(shortValue / multiplier) * multiplier));
                    int64Serde.serialize(aInt64, resultStorage.getDataOutput());
                    break;
                case INTEGER:
                    int intValue = AInt32SerializerDeserializer.getInt(valueBytes, valueOffset + 1);
                    aInt64.setValue((long) (Math.round(intValue / multiplier) * multiplier));
                    int64Serde.serialize(aInt64, resultStorage.getDataOutput());
                    break;
                case BIGINT:
                    long longValue = AInt64SerializerDeserializer.getLong(valueBytes, valueOffset + 1);
                    aInt64.setValue((long) (Math.round(longValue / multiplier) * multiplier));
                    int64Serde.serialize(aInt64, resultStorage.getDataOutput());
                    break;
                case FLOAT:
                    float floatValue = AFloatSerializerDeserializer.getFloat(valueBytes, valueOffset + 1);
                    // Maintain float precision in next operation
                    aFloat.setValue(Math.round(floatValue / (float) multiplier) * (float) multiplier);
                    floatSerde.serialize(aFloat, resultStorage.getDataOutput());
                    break;
                case DOUBLE:
                    double doubleValue = ADoubleSerializerDeserializer.getDouble(valueBytes, valueOffset + 1);
                    aDouble.setValue(Math.round(doubleValue / multiplier) * multiplier);
                    doubleSerde.serialize(aDouble, resultStorage.getDataOutput());
                    break;
                default:
                    PointableHelper.setNull(result);
                    return;
            }
        }

        result.set(resultStorage);
    }
}
