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
package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableDate;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableDuration;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.base.AMutableInt16;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.AMutableInt8;
import edu.uci.ics.asterix.om.base.AMutableTime;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@SuppressWarnings("serial")
public abstract class AbstractNumericArithmeticEval extends AbstractScalarFunctionDynamicDescriptor {

    abstract protected long evaluateInteger(long lhs, long rhs) throws HyracksDataException;

    abstract protected double evaluateDouble(double lhs, double rhs) throws HyracksDataException;

    /**
     * abstract method for arithmetic operation between a time instance (date/time/datetime)
     * and a duration (duration/year-month-duration/day-time-duration)
     * 
     * @param chronon
     * @param yearMonth
     * @param dayTime
     * @return
     * @throws HyracksDataException
     */
    abstract protected long evaluateTimeDurationArithmetic(long chronon, int yearMonth, long dayTime, boolean isTimeOnly)
            throws HyracksDataException;

    /**
     * abstract method for arithmetic operation between two time instances (date/time/datetime)
     * 
     * @param chronon0
     * @param chronon1
     * @return
     * @throws HyracksDataException
     */
    abstract protected long evaluateTimeInstanceArithmetic(long chronon0, long chronon1) throws HyracksDataException;

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {

                return new ICopyEvaluator() {
                    private DataOutput out = output.getDataOutput();
                    // one temp. buffer re-used by both children
                    private ArrayBackedValueStorage argOut0 = new ArrayBackedValueStorage();
                    private ArrayBackedValueStorage argOut1 = new ArrayBackedValueStorage();
                    private ICopyEvaluator evalLeft = args[0].createEvaluator(argOut0);
                    private ICopyEvaluator evalRight = args[1].createEvaluator(argOut1);
                    private double[] operandsFloating = new double[args.length];
                    private long[] operandsInteger = new long[args.length];
                    private int resultType;
                    static protected final int typeInt8 = 1;
                    static protected final int typeInt16 = 2;
                    static protected final int typeInt32 = 3;
                    static protected final int typeInt64 = 4;
                    static protected final int typeFloat = 5;
                    static protected final int typeDouble = 6;

                    protected AMutableFloat aFloat = new AMutableFloat(0);
                    protected AMutableDouble aDouble = new AMutableDouble(0);
                    protected AMutableInt64 aInt64 = new AMutableInt64(0);
                    protected AMutableInt32 aInt32 = new AMutableInt32(0);
                    protected AMutableInt16 aInt16 = new AMutableInt16((short) 0);
                    protected AMutableInt8 aInt8 = new AMutableInt8((byte) 0);

                    protected AMutableDuration aDuration = new AMutableDuration(0, 0);
                    protected AMutableDate aDate = new AMutableDate(0);
                    protected AMutableTime aTime = new AMutableTime(0);
                    protected AMutableDateTime aDatetime = new AMutableDateTime(0);

                    private ATypeTag typeTag;
                    @SuppressWarnings("rawtypes")
                    private ISerializerDeserializer serde;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                        try {
                            resultType = 0;
                            int currentType = 0;
                            for (int i = 0; i < args.length; i++) {
                                ArrayBackedValueStorage argOut;
                                if (i == 0) {
                                    argOut0.reset();
                                    evalLeft.evaluate(tuple);
                                    argOut = argOut0;
                                } else {
                                    argOut1.reset();
                                    evalRight.evaluate(tuple);
                                    argOut = argOut1;
                                }
                                typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getByteArray()[0]);
                                switch (typeTag) {
                                    case INT8: {
                                        currentType = typeInt8;
                                        operandsInteger[i] = AInt8SerializerDeserializer.getByte(argOut.getByteArray(),
                                                1);
                                        operandsFloating[i] = AInt8SerializerDeserializer.getByte(
                                                argOut.getByteArray(), 1);
                                        break;
                                    }
                                    case INT16: {
                                        currentType = typeInt16;
                                        operandsInteger[i] = AInt16SerializerDeserializer.getShort(
                                                argOut.getByteArray(), 1);
                                        operandsFloating[i] = AInt16SerializerDeserializer.getShort(
                                                argOut.getByteArray(), 1);
                                        break;
                                    }
                                    case INT32: {
                                        currentType = typeInt32;
                                        operandsInteger[i] = AInt32SerializerDeserializer.getInt(argOut.getByteArray(),
                                                1);
                                        operandsFloating[i] = AInt32SerializerDeserializer.getInt(
                                                argOut.getByteArray(), 1);
                                        break;
                                    }
                                    case INT64: {
                                        currentType = typeInt64;
                                        operandsInteger[i] = AInt64SerializerDeserializer.getLong(
                                                argOut.getByteArray(), 1);
                                        operandsFloating[i] = AInt64SerializerDeserializer.getLong(
                                                argOut.getByteArray(), 1);
                                        break;
                                    }
                                    case FLOAT: {
                                        currentType = typeFloat;
                                        operandsFloating[i] = AFloatSerializerDeserializer.getFloat(
                                                argOut.getByteArray(), 1);
                                        break;
                                    }
                                    case DOUBLE: {
                                        currentType = typeDouble;
                                        operandsFloating[i] = ADoubleSerializerDeserializer.getDouble(
                                                argOut.getByteArray(), 1);
                                        break;
                                    }
                                    case DATE:
                                    case TIME:
                                    case DATETIME:
                                    case DURATION:
                                    case YEARMONTHDURATION:
                                    case DAYTIMEDURATION:
                                        evaluateTemporalArthmeticOperation(typeTag, tuple);
                                        return;
                                    case NULL: {
                                        serde = AqlSerializerDeserializerProvider.INSTANCE
                                                .getSerializerDeserializer(BuiltinType.ANULL);
                                        serde.serialize(ANull.NULL, out);
                                        return;
                                    }
                                    default: {
                                        throw new NotImplementedException(getIdentifier().getName()
                                                + (i == 0 ? ": Left" : ": Right")
                                                + " operand expects INT8/INT16/INT32/INT64/FLOAT/DOUBLE/NULL, but got "
                                                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut
                                                        .getByteArray()[0]));
                                    }
                                }

                                if (resultType < currentType) {
                                    resultType = currentType;
                                }
                            }

                            long lres = 0;
                            double dres = 0;
                            switch (resultType) {
                                case typeInt8:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                            .getSerializerDeserializer(BuiltinType.AINT8);
                                    lres = evaluateInteger(operandsInteger[0], operandsInteger[1]);
                                    if (lres > Byte.MAX_VALUE) {
                                        throw new AlgebricksException("Overflow happened.");
                                    }
                                    if (lres < Byte.MIN_VALUE) {
                                        throw new AlgebricksException("Underflow happened.");
                                    }
                                    aInt8.setValue((byte) lres);
                                    serde.serialize(aInt8, out);
                                    break;
                                case typeInt16:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                            .getSerializerDeserializer(BuiltinType.AINT16);
                                    lres = evaluateInteger(operandsInteger[0], operandsInteger[1]);
                                    if (lres > Short.MAX_VALUE) {
                                        throw new AlgebricksException("Overflow happened.");
                                    }
                                    if (lres < Short.MIN_VALUE) {
                                        throw new AlgebricksException("Underflow happened.");
                                    }
                                    aInt16.setValue((short) lres);
                                    serde.serialize(aInt16, out);
                                    break;
                                case typeInt32:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                            .getSerializerDeserializer(BuiltinType.AINT32);
                                    lres = evaluateInteger(operandsInteger[0], operandsInteger[1]);
                                    if (lres > Integer.MAX_VALUE) {
                                        throw new AlgebricksException("Overflow happened.");
                                    }
                                    if (lres < Integer.MIN_VALUE) {
                                        throw new AlgebricksException("Underflow happened.");
                                    }
                                    aInt32.setValue((int) lres);
                                    serde.serialize(aInt32, out);
                                    break;
                                case typeInt64:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                            .getSerializerDeserializer(BuiltinType.AINT64);
                                    lres = evaluateInteger(operandsInteger[0], operandsInteger[1]);
                                    aInt64.setValue(lres);
                                    serde.serialize(aInt64, out);
                                    break;
                                case typeFloat:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                            .getSerializerDeserializer(BuiltinType.AFLOAT);
                                    dres = evaluateDouble(operandsFloating[0], operandsFloating[1]);
                                    if (dres > Float.MAX_VALUE) {
                                        throw new AlgebricksException("Overflow happened.");
                                    }
                                    if (dres < -Float.MAX_VALUE) {
                                        throw new AlgebricksException("Underflow happened.");
                                    }
                                    aFloat.setValue((float) dres);
                                    serde.serialize(aFloat, out);
                                    break;
                                case typeDouble:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                            .getSerializerDeserializer(BuiltinType.ADOUBLE);
                                    aDouble.setValue(evaluateDouble(operandsFloating[0], operandsFloating[1]));
                                    serde.serialize(aDouble, out);
                                    break;
                            }
                        } catch (HyracksDataException hde) {
                            throw new AlgebricksException(hde);
                        }
                    }

                    @SuppressWarnings("unchecked")
                    private void evaluateTemporalArthmeticOperation(ATypeTag leftType, IFrameTupleReference tuple)
                            throws HyracksDataException, AlgebricksException {
                        argOut1.reset();
                        evalRight.evaluate(tuple);
                        ATypeTag rightType = EnumDeserializer.ATYPETAGDESERIALIZER
                                .deserialize(argOut1.getByteArray()[0]);

                        if (leftType == ATypeTag.NULL || rightType == ATypeTag.NULL) {
                            serde = AqlSerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.ANULL);
                            serde.serialize(ANull.NULL, out);
                            return;
                        }

                        if (rightType == leftType) {

                            serde = AqlSerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.ADURATION);

                            long leftChronon = 0, rightChronon = 0, dayTime = 0;

                            int yearMonth = 0;

                            switch (leftType) {
                                case DATE:
                                    leftChronon = ADateSerializerDeserializer.getChronon(argOut0.getByteArray(), 1)
                                            * GregorianCalendarSystem.CHRONON_OF_DAY;
                                    rightChronon = ADateSerializerDeserializer.getChronon(argOut1.getByteArray(), 1)
                                            * GregorianCalendarSystem.CHRONON_OF_DAY;

                                    break;
                                case TIME:
                                    leftChronon = ATimeSerializerDeserializer.getChronon(argOut0.getByteArray(), 1);
                                    rightChronon = ATimeSerializerDeserializer.getChronon(argOut1.getByteArray(), 1);
                                    break;
                                case DATETIME:
                                    leftChronon = ADateTimeSerializerDeserializer.getChronon(argOut0.getByteArray(), 1);
                                    rightChronon = ADateTimeSerializerDeserializer
                                            .getChronon(argOut1.getByteArray(), 1);
                                    break;
                                case YEARMONTHDURATION:
                                    yearMonth = (int) evaluateTimeInstanceArithmetic(
                                            AYearMonthDurationSerializerDeserializer.getYearMonth(
                                                    argOut0.getByteArray(), 1),
                                            AYearMonthDurationSerializerDeserializer.getYearMonth(
                                                    argOut1.getByteArray(), 1));
                                    break;
                                case DAYTIMEDURATION:
                                    leftChronon = ADayTimeDurationSerializerDeserializer.getDayTime(
                                            argOut0.getByteArray(), 1);
                                    rightChronon = ADayTimeDurationSerializerDeserializer.getDayTime(
                                            argOut1.getByteArray(), 1);
                                    break;
                                default:
                                    throw new NotImplementedException();
                            }

                            dayTime = evaluateTimeInstanceArithmetic(leftChronon, rightChronon);

                            aDuration.setValue(yearMonth, dayTime);

                            serde.serialize(aDuration, out);

                        } else {
                            long chronon = 0, dayTime = 0;
                            int yearMonth = 0;
                            ATypeTag resultType = null;

                            boolean isTimeOnly = false;

                            switch (leftType) {
                                case TIME:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                            .getSerializerDeserializer(BuiltinType.ATIME);
                                    chronon = ATimeSerializerDeserializer.getChronon(argOut0.getByteArray(), 1);
                                    isTimeOnly = true;
                                    resultType = ATypeTag.TIME;
                                    switch (rightType) {
                                        case DAYTIMEDURATION:
                                            dayTime = ADayTimeDurationSerializerDeserializer.getDayTime(
                                                    argOut1.getByteArray(), 1);
                                            break;
                                        case DURATION:
                                            dayTime = ADurationSerializerDeserializer.getDayTime(
                                                    argOut1.getByteArray(), 1);
                                            yearMonth = ADurationSerializerDeserializer.getYearMonth(
                                                    argOut1.getByteArray(), 1);
                                            break;
                                        default:
                                            throw new NotImplementedException(getIdentifier().getName()
                                                    + ": arithmetic operation between " + leftType + " and a "
                                                    + rightType + " value is not supported.");
                                    }
                                    break;
                                case DATE:
                                    serde = AqlSerializerDeserializerProvider.INSTANCE
                                            .getSerializerDeserializer(BuiltinType.ADATE);
                                    resultType = ATypeTag.DATE;
                                    chronon = ADateSerializerDeserializer.getChronon(argOut0.getByteArray(), 1)
                                            * GregorianCalendarSystem.CHRONON_OF_DAY;
                                case DATETIME:
                                    if (leftType == ATypeTag.DATETIME) {
                                        serde = AqlSerializerDeserializerProvider.INSTANCE
                                                .getSerializerDeserializer(BuiltinType.ADATETIME);
                                        resultType = ATypeTag.DATETIME;
                                        chronon = ADateTimeSerializerDeserializer.getChronon(argOut0.getByteArray(), 1);
                                    }
                                    switch (rightType) {
                                        case DURATION:
                                            yearMonth = ADurationSerializerDeserializer.getYearMonth(
                                                    argOut1.getByteArray(), 1);
                                            dayTime = ADurationSerializerDeserializer.getDayTime(
                                                    argOut1.getByteArray(), 1);
                                            break;
                                        case YEARMONTHDURATION:
                                            yearMonth = AYearMonthDurationSerializerDeserializer.getYearMonth(
                                                    argOut1.getByteArray(), 1);
                                            break;
                                        case DAYTIMEDURATION:
                                            dayTime = ADayTimeDurationSerializerDeserializer.getDayTime(
                                                    argOut1.getByteArray(), 1);
                                            break;
                                        default:
                                            throw new NotImplementedException(getIdentifier().getName()
                                                    + ": arithmetic operation between " + leftType + " and a "
                                                    + rightType + " value is not supported.");
                                    }
                                    break;
                                case YEARMONTHDURATION:
                                    yearMonth = AYearMonthDurationSerializerDeserializer.getYearMonth(
                                            argOut0.getByteArray(), 1);
                                    switch (rightType) {
                                        case DATETIME:
                                            serde = AqlSerializerDeserializerProvider.INSTANCE
                                                    .getSerializerDeserializer(BuiltinType.ADATETIME);
                                            resultType = ATypeTag.DATETIME;
                                            chronon = ADateTimeSerializerDeserializer.getChronon(
                                                    argOut1.getByteArray(), 1);
                                            break;
                                        case DATE:
                                            serde = AqlSerializerDeserializerProvider.INSTANCE
                                                    .getSerializerDeserializer(BuiltinType.ADATE);
                                            resultType = ATypeTag.DATE;
                                            chronon = ADateSerializerDeserializer.getChronon(argOut1.getByteArray(), 1)
                                                    * GregorianCalendarSystem.CHRONON_OF_DAY;
                                            break;
                                        default:
                                            throw new NotImplementedException(getIdentifier().getName()
                                                    + ": arithmetic operation between " + leftType + " and a "
                                                    + rightType + " value is not supported.");
                                    }
                                    break;
                                case DURATION:
                                    yearMonth = ADurationSerializerDeserializer.getYearMonth(argOut0.getByteArray(), 1);
                                    dayTime = ADurationSerializerDeserializer.getDayTime(argOut0.getByteArray(), 1);
                                case DAYTIMEDURATION:
                                    if (leftType == ATypeTag.DAYTIMEDURATION) {
                                        dayTime = ADayTimeDurationSerializerDeserializer.getDayTime(
                                                argOut0.getByteArray(), 1);
                                    }
                                    switch (rightType) {
                                        case DATETIME:
                                            serde = AqlSerializerDeserializerProvider.INSTANCE
                                                    .getSerializerDeserializer(BuiltinType.ADATETIME);
                                            resultType = ATypeTag.DATETIME;
                                            chronon = ADateTimeSerializerDeserializer.getChronon(
                                                    argOut1.getByteArray(), 1);
                                            break;
                                        case DATE:
                                            serde = AqlSerializerDeserializerProvider.INSTANCE
                                                    .getSerializerDeserializer(BuiltinType.ADATE);
                                            resultType = ATypeTag.DATE;
                                            chronon = ADateSerializerDeserializer.getChronon(argOut1.getByteArray(), 1)
                                                    * GregorianCalendarSystem.CHRONON_OF_DAY;
                                            break;
                                        case TIME:
                                            if (yearMonth == 0) {
                                                serde = AqlSerializerDeserializerProvider.INSTANCE
                                                        .getSerializerDeserializer(BuiltinType.ATIME);
                                                resultType = ATypeTag.TIME;
                                                chronon = ATimeSerializerDeserializer.getChronon(
                                                        argOut1.getByteArray(), 1);
                                                isTimeOnly = true;
                                                break;
                                            }
                                        default:
                                            throw new NotImplementedException(getIdentifier().getName()
                                                    + ": arithmetic operation between " + leftType + " and a "
                                                    + rightType + " value is not supported.");
                                    }
                                    break;
                                default:
                                    throw new NotImplementedException(getIdentifier().getName()
                                            + ": arithmetic operation between " + leftType + " and a " + rightType
                                            + " value is not supported.");
                            }

                            chronon = evaluateTimeDurationArithmetic(chronon, yearMonth, dayTime, isTimeOnly);

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
                                    throw new NotImplementedException(getIdentifier().getName()
                                            + ": arithmetic operation between " + leftType + " and a " + rightType
                                            + " value is not supported.");

                            }
                        }
                    }
                };
            }
        };
    }
}
