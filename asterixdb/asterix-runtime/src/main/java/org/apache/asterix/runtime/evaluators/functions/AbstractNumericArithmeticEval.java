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

import java.io.DataOutput;

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
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDate;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableDuration;
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.base.AMutableInt16;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableInt8;
import org.apache.asterix.om.base.AMutableTime;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.IncompatibleTypeException;
import org.apache.asterix.runtime.exceptions.OverflowException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.asterix.runtime.exceptions.UnderflowException;
import org.apache.asterix.runtime.exceptions.UnsupportedTypeException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractNumericArithmeticEval extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 527445160961348706L;

    /**
     * abstract method for arithmetic operation between two integer values
     *
     * @param lhs first operand
     * @param rhs second operand
     * @param result result holder
     * @return {@code false} if the result is {@code NULL}, otherwise {@code true}
     */
    abstract protected boolean evaluateInteger(long lhs, long rhs, AMutableInt64 result) throws HyracksDataException;

    /**
     * abstract method for arithmetic operation between two floating point values
     *
     * @param lhs first operand
     * @param rhs second operand
     * @param result result holder
     * @return {@code false} if the result is {@code NULL}, otherwise {@code true}
     */
    abstract protected boolean evaluateDouble(double lhs, double rhs, AMutableDouble result)
            throws HyracksDataException;

    /**
     * abstract method for arithmetic operation between a time instance (date/time/datetime)
     * and a duration (duration/year-month-duration/day-time-duration)
     *
     * @param chronon first operand
     * @param yearMonth year-month component of the second operand
     * @param dayTime day-time component of the second operand
     * @param result result holder
     * @return {@code false} if the result is {@code NULL}, otherwise {@code true}
     */
    abstract protected boolean evaluateTimeDurationArithmetic(long chronon, int yearMonth, long dayTime,
            boolean isTimeOnly, AMutableInt64 result) throws HyracksDataException;

    /**
     * abstract method for arithmetic operation between two time instances (date/time/datetime)
     *
     * @param chronon0 first operand
     * @param chronon1 second operand
     * @param result result holder
     * @return {@code false} if the result is {@code NULL}, otherwise {@code true}
     */
    abstract protected boolean evaluateTimeInstanceArithmetic(long chronon0, long chronon1, AMutableInt64 result)
            throws HyracksDataException;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {

                return new IScalarEvaluator() {
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IPointable argPtr0 = new VoidPointable();
                    private final IPointable argPtr1 = new VoidPointable();
                    private final IScalarEvaluator evalLeft = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalRight = args[1].createScalarEvaluator(ctx);
                    private final double[] operandsFloating = new double[args.length];
                    private final long[] operandsInteger = new long[args.length];

                    private final AMutableFloat aFloat = new AMutableFloat(0);
                    private final AMutableDouble aDouble = new AMutableDouble(0);
                    private final AMutableInt64 aInt64 = new AMutableInt64(0);
                    private final AMutableInt32 aInt32 = new AMutableInt32(0);
                    private final AMutableInt16 aInt16 = new AMutableInt16((short) 0);
                    private final AMutableInt8 aInt8 = new AMutableInt8((byte) 0);

                    private final AMutableDuration aDuration = new AMutableDuration(0, 0);
                    private final AMutableDate aDate = new AMutableDate(0);
                    private final AMutableTime aTime = new AMutableTime(0);
                    private final AMutableDateTime aDatetime = new AMutableDateTime(0);

                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer int8Serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);
                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer int16Serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT16);
                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer int32Serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer int64Serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer floatSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AFLOAT);
                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer doubleSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer dateSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATE);
                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer timeSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ATIME);
                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer dateTimeSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATETIME);
                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer durationSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADURATION);
                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer nullSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    @SuppressWarnings("unchecked")
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        evalLeft.evaluate(tuple, argPtr0);
                        evalRight.evaluate(tuple, argPtr1);

                        resultStorage.reset();

                        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr0, argPtr1)) {
                            return;
                        }

                        ATypeTag argTypeMax = null;

                        for (int i = 0; i < 2; i++) {
                            IPointable argPtr = i == 0 ? argPtr0 : argPtr1;
                            byte[] bytes = argPtr.getByteArray();
                            int offset = argPtr.getStartOffset();

                            ATypeTag currentType;
                            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
                            switch (typeTag) {
                                case TINYINT:
                                    currentType = ATypeTag.TINYINT;
                                    operandsInteger[i] = AInt8SerializerDeserializer.getByte(bytes, offset + 1);
                                    operandsFloating[i] = operandsInteger[i];
                                    break;
                                case SMALLINT:
                                    currentType = ATypeTag.SMALLINT;
                                    operandsInteger[i] = AInt16SerializerDeserializer.getShort(bytes, offset + 1);
                                    operandsFloating[i] = operandsInteger[i];
                                    break;
                                case INTEGER:
                                    currentType = ATypeTag.INTEGER;
                                    operandsInteger[i] = AInt32SerializerDeserializer.getInt(bytes, offset + 1);
                                    operandsFloating[i] = operandsInteger[i];
                                    break;
                                case BIGINT:
                                    currentType = ATypeTag.BIGINT;
                                    operandsInteger[i] = AInt64SerializerDeserializer.getLong(bytes, offset + 1);
                                    operandsFloating[i] = operandsInteger[i];
                                    break;
                                case FLOAT:
                                    currentType = ATypeTag.FLOAT;
                                    operandsFloating[i] = AFloatSerializerDeserializer.getFloat(bytes, offset + 1);
                                    break;
                                case DOUBLE:
                                    currentType = ATypeTag.DOUBLE;
                                    operandsFloating[i] = ADoubleSerializerDeserializer.getDouble(bytes, offset + 1);
                                    break;
                                case DATE:
                                case TIME:
                                case DATETIME:
                                case DURATION:
                                case YEARMONTHDURATION:
                                case DAYTIMEDURATION:
                                    evaluateTemporalArithmeticOperation(typeTag);
                                    result.set(resultStorage);
                                    return;
                                default:
                                    throw new TypeMismatchException(sourceLoc, getIdentifier(), i, bytes[offset],
                                            ATypeTag.SERIALIZED_INT8_TYPE_TAG, ATypeTag.SERIALIZED_INT16_TYPE_TAG,
                                            ATypeTag.SERIALIZED_INT32_TYPE_TAG, ATypeTag.SERIALIZED_INT64_TYPE_TAG,
                                            ATypeTag.SERIALIZED_FLOAT_TYPE_TAG, ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG,
                                            ATypeTag.SERIALIZED_DATE_TYPE_TAG, ATypeTag.SERIALIZED_TIME_TYPE_TAG,
                                            ATypeTag.SERIALIZED_DATETIME_TYPE_TAG,
                                            ATypeTag.SERIALIZED_DURATION_TYPE_TAG,
                                            ATypeTag.SERIALIZED_YEAR_MONTH_DURATION_TYPE_TAG,
                                            ATypeTag.SERIALIZED_DAY_TIME_DURATION_TYPE_TAG);
                            }

                            if (i == 0 || currentType.ordinal() > argTypeMax.ordinal()) {
                                argTypeMax = currentType;
                            }
                        }

                        ATypeTag resultType = getNumericResultType(argTypeMax);

                        long lres;
                        double dres;
                        switch (resultType) {
                            case TINYINT:
                                if (evaluateInteger(operandsInteger[0], operandsInteger[1], aInt64)) {
                                    lres = aInt64.getLongValue();
                                    if (lres > Byte.MAX_VALUE) {
                                        throw new OverflowException(sourceLoc, getIdentifier());
                                    }
                                    if (lres < Byte.MIN_VALUE) {
                                        throw new UnderflowException(sourceLoc, getIdentifier());
                                    }
                                    aInt8.setValue((byte) lres);
                                    int8Serde.serialize(aInt8, out);
                                } else {
                                    nullSerde.serialize(ANull.NULL, out);
                                }
                                break;
                            case SMALLINT:
                                if (evaluateInteger(operandsInteger[0], operandsInteger[1], aInt64)) {
                                    lres = aInt64.getLongValue();
                                    if (lres > Short.MAX_VALUE) {
                                        throw new OverflowException(sourceLoc, getIdentifier());
                                    }
                                    if (lres < Short.MIN_VALUE) {
                                        throw new UnderflowException(sourceLoc, getIdentifier());
                                    }
                                    aInt16.setValue((short) lres);
                                    int16Serde.serialize(aInt16, out);
                                } else {
                                    nullSerde.serialize(ANull.NULL, out);
                                }
                                break;
                            case INTEGER:
                                if (evaluateInteger(operandsInteger[0], operandsInteger[1], aInt64)) {
                                    lres = aInt64.getLongValue();
                                    if (lres > Integer.MAX_VALUE) {
                                        throw new OverflowException(sourceLoc, getIdentifier());
                                    }
                                    if (lres < Integer.MIN_VALUE) {
                                        throw new UnderflowException(sourceLoc, getIdentifier());
                                    }
                                    aInt32.setValue((int) lres);
                                    int32Serde.serialize(aInt32, out);
                                } else {
                                    nullSerde.serialize(ANull.NULL, out);
                                }
                                break;
                            case BIGINT:
                                if (evaluateInteger(operandsInteger[0], operandsInteger[1], aInt64)) {
                                    int64Serde.serialize(aInt64, out);
                                } else {
                                    nullSerde.serialize(ANull.NULL, out);
                                }
                                break;
                            case FLOAT:
                                if (evaluateDouble(operandsFloating[0], operandsFloating[1], aDouble)) {
                                    dres = aDouble.getDoubleValue();
                                    if (Double.isFinite(dres)) {
                                        if (dres > Float.MAX_VALUE) {
                                            throw new OverflowException(sourceLoc, getIdentifier());
                                        }
                                        if (dres < -Float.MAX_VALUE) {
                                            throw new UnderflowException(sourceLoc, getIdentifier());
                                        }
                                    }
                                    aFloat.setValue((float) dres);
                                    floatSerde.serialize(aFloat, out);
                                } else {
                                    nullSerde.serialize(ANull.NULL, out);
                                }
                                break;
                            case DOUBLE:
                                if (evaluateDouble(operandsFloating[0], operandsFloating[1], aDouble)) {
                                    doubleSerde.serialize(aDouble, out);
                                } else {
                                    nullSerde.serialize(ANull.NULL, out);
                                }
                                break;
                        }
                        result.set(resultStorage);
                    }

                    @SuppressWarnings("unchecked")
                    private void evaluateTemporalArithmeticOperation(ATypeTag leftType) throws HyracksDataException {
                        byte[] bytes1 = argPtr1.getByteArray();
                        int offset1 = argPtr1.getStartOffset();
                        ATypeTag rightType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]);
                        byte[] bytes0 = argPtr0.getByteArray();
                        int offset0 = argPtr0.getStartOffset();

                        if (rightType == leftType) {
                            long leftChronon = 0, rightChronon = 0, dayTime = 0;
                            int yearMonth = 0;
                            boolean yearMonthIsNull = false, dayTimeIsNull = false;

                            switch (leftType) {
                                case DATE:
                                    leftChronon = ADateSerializerDeserializer.getChronon(bytes0, offset0 + 1)
                                            * GregorianCalendarSystem.CHRONON_OF_DAY;
                                    rightChronon = ADateSerializerDeserializer.getChronon(bytes1, offset1 + 1)
                                            * GregorianCalendarSystem.CHRONON_OF_DAY;
                                    break;
                                case TIME:
                                    leftChronon = ATimeSerializerDeserializer.getChronon(bytes0, offset0 + 1);
                                    rightChronon = ATimeSerializerDeserializer.getChronon(bytes1, offset1 + 1);
                                    break;
                                case DATETIME:
                                    leftChronon = ADateTimeSerializerDeserializer.getChronon(bytes0, offset0 + 1);
                                    rightChronon = ADateTimeSerializerDeserializer.getChronon(bytes1, offset1 + 1);
                                    break;
                                case YEARMONTHDURATION:
                                    if (evaluateTimeInstanceArithmetic(
                                            AYearMonthDurationSerializerDeserializer.getYearMonth(bytes0, offset0 + 1),
                                            AYearMonthDurationSerializerDeserializer.getYearMonth(bytes1, offset1 + 1),
                                            aInt64)) {
                                        yearMonth = (int) aInt64.getLongValue();
                                    } else {
                                        yearMonthIsNull = true;
                                    }
                                    break;
                                case DAYTIMEDURATION:
                                    leftChronon =
                                            ADayTimeDurationSerializerDeserializer.getDayTime(bytes0, offset0 + 1);
                                    rightChronon =
                                            ADayTimeDurationSerializerDeserializer.getDayTime(bytes1, offset1 + 1);
                                    break;
                                default:
                                    throw new UnsupportedTypeException(sourceLoc, getIdentifier(), bytes1[offset1]);
                            }

                            if (evaluateTimeInstanceArithmetic(leftChronon, rightChronon, aInt64)) {
                                dayTime = aInt64.getLongValue();
                            } else {
                                dayTimeIsNull = true;
                            }

                            if (yearMonthIsNull || dayTimeIsNull) {
                                nullSerde.serialize(ANull.NULL, out);
                            } else {
                                aDuration.setValue(yearMonth, dayTime);
                                durationSerde.serialize(aDuration, out);
                            }

                        } else {
                            long chronon = 0, dayTime = 0;
                            int yearMonth = 0;
                            ATypeTag resultType = null;
                            ISerializerDeserializer serde = null;

                            boolean isTimeOnly = false;

                            switch (leftType) {
                                case TIME:
                                    serde = timeSerde;
                                    chronon = ATimeSerializerDeserializer.getChronon(bytes0, offset0 + 1);
                                    isTimeOnly = true;
                                    resultType = ATypeTag.TIME;
                                    switch (rightType) {
                                        case DAYTIMEDURATION:
                                            dayTime = ADayTimeDurationSerializerDeserializer.getDayTime(bytes1,
                                                    offset1 + 1);
                                            break;
                                        case DURATION:
                                            dayTime = ADurationSerializerDeserializer.getDayTime(bytes1, offset1 + 1);
                                            yearMonth =
                                                    ADurationSerializerDeserializer.getYearMonth(bytes1, offset1 + 1);
                                            break;
                                        default:
                                            throw new IncompatibleTypeException(sourceLoc, getIdentifier(),
                                                    bytes0[offset0], bytes1[offset1]);
                                    }
                                    break;
                                case DATE:
                                    serde = dateSerde;
                                    resultType = ATypeTag.DATE;
                                    chronon = ADateSerializerDeserializer.getChronon(bytes0, offset0 + 1)
                                            * GregorianCalendarSystem.CHRONON_OF_DAY;
                                case DATETIME:
                                    if (leftType == ATypeTag.DATETIME) {
                                        serde = dateTimeSerde;
                                        resultType = ATypeTag.DATETIME;
                                        chronon = ADateTimeSerializerDeserializer.getChronon(bytes0, offset0 + 1);
                                    }
                                    switch (rightType) {
                                        case DURATION:
                                            yearMonth =
                                                    ADurationSerializerDeserializer.getYearMonth(bytes1, offset1 + 1);
                                            dayTime = ADurationSerializerDeserializer.getDayTime(bytes1, offset1 + 1);
                                            break;
                                        case YEARMONTHDURATION:
                                            yearMonth = AYearMonthDurationSerializerDeserializer.getYearMonth(bytes1,
                                                    offset1 + 1);
                                            break;
                                        case DAYTIMEDURATION:
                                            dayTime = ADayTimeDurationSerializerDeserializer.getDayTime(bytes1,
                                                    offset1 + 1);
                                            break;
                                        default:
                                            throw new IncompatibleTypeException(sourceLoc, getIdentifier(),
                                                    bytes0[offset0], bytes1[offset1]);
                                    }
                                    break;
                                case YEARMONTHDURATION:
                                    yearMonth =
                                            AYearMonthDurationSerializerDeserializer.getYearMonth(bytes0, offset0 + 1);
                                    switch (rightType) {
                                        case DATETIME:
                                            serde = dateTimeSerde;
                                            resultType = ATypeTag.DATETIME;
                                            chronon = ADateTimeSerializerDeserializer.getChronon(bytes1, offset1 + 1);
                                            break;
                                        case DATE:
                                            serde = dateSerde;
                                            resultType = ATypeTag.DATE;
                                            chronon = ADateSerializerDeserializer.getChronon(bytes1, offset1 + 1)
                                                    * GregorianCalendarSystem.CHRONON_OF_DAY;
                                            break;
                                        default:
                                            throw new IncompatibleTypeException(sourceLoc, getIdentifier(),
                                                    bytes0[offset0], bytes1[offset1]);
                                    }
                                    break;
                                case DURATION:
                                    yearMonth = ADurationSerializerDeserializer.getYearMonth(bytes0, offset0 + 1);
                                    dayTime = ADurationSerializerDeserializer.getDayTime(bytes0, offset0 + 1);
                                case DAYTIMEDURATION:
                                    if (leftType == ATypeTag.DAYTIMEDURATION) {
                                        dayTime =
                                                ADayTimeDurationSerializerDeserializer.getDayTime(bytes0, offset0 + 1);
                                    }
                                    switch (rightType) {
                                        case DATETIME:
                                            serde = dateTimeSerde;
                                            resultType = ATypeTag.DATETIME;
                                            chronon = ADateTimeSerializerDeserializer.getChronon(bytes1, offset1 + 1);
                                            break;
                                        case DATE:
                                            serde = dateSerde;
                                            resultType = ATypeTag.DATE;
                                            chronon = ADateSerializerDeserializer.getChronon(bytes1, offset1 + 1)
                                                    * GregorianCalendarSystem.CHRONON_OF_DAY;
                                            break;
                                        case TIME:
                                            if (yearMonth == 0) {
                                                serde = timeSerde;
                                                resultType = ATypeTag.TIME;
                                                chronon = ATimeSerializerDeserializer.getChronon(bytes1, offset1 + 1);
                                                isTimeOnly = true;
                                                break;
                                            }
                                        default:
                                            throw new IncompatibleTypeException(sourceLoc, getIdentifier(),
                                                    bytes0[offset0], bytes1[offset1]);
                                    }
                                    break;
                                default:
                                    throw new IncompatibleTypeException(sourceLoc, getIdentifier(), bytes0[offset0],
                                            bytes1[offset1]);
                            }

                            if (evaluateTimeDurationArithmetic(chronon, yearMonth, dayTime, isTimeOnly, aInt64)) {
                                chronon = aInt64.getLongValue();
                                switch (resultType) {
                                    case DATE:
                                        if (chronon < 0 && chronon % GregorianCalendarSystem.CHRONON_OF_DAY != 0) {
                                            chronon = chronon / GregorianCalendarSystem.CHRONON_OF_DAY - 1;
                                        } else {
                                            chronon = chronon / GregorianCalendarSystem.CHRONON_OF_DAY;
                                        }
                                        aDate.setValue((int) chronon);
                                        serde.serialize(aDate, out);
                                        break;
                                    case TIME:
                                        aTime.setValue((int) chronon);
                                        serde.serialize(aTime, out);
                                        break;
                                    case DATETIME:
                                        aDatetime.setValue(chronon);
                                        serde.serialize(aDatetime, out);
                                        break;
                                    default:
                                        throw new IncompatibleTypeException(sourceLoc, getIdentifier(), bytes0[offset0],
                                                bytes1[offset1]);
                                }
                            } else {
                                nullSerde.serialize(ANull.NULL, out);
                            }
                        }
                    }
                };
            }
        };
    }

    protected ATypeTag getNumericResultType(ATypeTag argTypeMax) {
        return argTypeMax;
    }
}
