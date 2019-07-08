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
package org.apache.asterix.runtime.evaluators.functions.temporal;

import java.io.DataOutput;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADuration;
import org.apache.asterix.om.base.AMutableDuration;
import org.apache.asterix.om.base.temporal.DurationArithmeticOperations;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * This function converts a given duration into a "human-readable" duration containing both year-month and day-time
 * duration parts, by re-organizing values between the duration fields from the given reference time point.
 * <p/>
 * The basic algorithm for this convert is simple: <br/>
 * 1. Calculate the time point by adding the given duration to the given time point;<br/>
 * 2. Calculate the differences by fields between two different time points;<br/>
 * 3. Re-format the duration into a human-readable one.
 * <p/>
 * Here "human-readable" means the value of each field of the duration is within the value range of the field in the
 * calendar system. For example, month would be in [0, 12), and hour would be in [0, 24).
 * <p/>
 * The result can be considered as a "field-based" difference between the two datetime value, but all negative values
 * would be converted to be non-negative.
 * <p/>
 * In the implementation, we always do the subtraction from the later time point, resulting a positive duration always.
 * <p/>
 */

@MissingNullInOutFunction
public class CalendarDurationFromDateTimeDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final FunctionIdentifier FID = BuiltinFunctions.CALENDAR_DURATION_FROM_DATETIME;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CalendarDurationFromDateTimeDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable argPtr0 = new VoidPointable();
                    private IPointable argPtr1 = new VoidPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADuration> durationSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADURATION);

                    private AMutableDuration aDuration = new AMutableDuration(0, 0);

                    private GregorianCalendarSystem calInstanct = GregorianCalendarSystem.getInstance();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, argPtr0);
                        eval1.evaluate(tuple, argPtr1);

                        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr0, argPtr1)) {
                            return;
                        }

                        byte[] bytes0 = argPtr0.getByteArray();
                        int offset0 = argPtr0.getStartOffset();
                        byte[] bytes1 = argPtr1.getByteArray();
                        int offset1 = argPtr1.getStartOffset();

                        if (bytes0[offset0] != ATypeTag.SERIALIZED_DATETIME_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                                    ATypeTag.SERIALIZED_DATETIME_TYPE_TAG);
                        }

                        if (bytes1[offset1] != ATypeTag.SERIALIZED_DURATION_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                                    ATypeTag.SERIALIZED_DURATION_TYPE_TAG);
                        }

                        int yearMonthDurationInMonths =
                                ADurationSerializerDeserializer.getYearMonth(bytes1, offset1 + 1);
                        long dayTimeDurationInMs = ADurationSerializerDeserializer.getDayTime(bytes1, offset1 + 1);

                        long startingTimePoint = ADateTimeSerializerDeserializer.getChronon(bytes0, offset0 + 1);

                        long endingTimePoint = DurationArithmeticOperations.addDuration(startingTimePoint,
                                yearMonthDurationInMonths, dayTimeDurationInMs, false);

                        if (startingTimePoint == endingTimePoint) {
                            aDuration.setValue(0, 0);
                        } else {
                            boolean negative = false;
                            if (endingTimePoint < startingTimePoint) {
                                negative = true;
                                // swap the starting and ending time, so that ending time is always larger than the
                                // starting time.
                                long tmpTime = endingTimePoint;
                                endingTimePoint = startingTimePoint;
                                startingTimePoint = tmpTime;
                            }

                            int year0 = calInstanct.getYear(startingTimePoint);
                            int month0 = calInstanct.getMonthOfYear(startingTimePoint, year0);

                            int year1 = calInstanct.getYear(endingTimePoint);
                            int month1 = calInstanct.getMonthOfYear(endingTimePoint, year1);

                            int year = year1 - year0;
                            int month = month1 - month0;
                            int day = calInstanct.getDayOfMonthYear(endingTimePoint, year1, month1)
                                    - calInstanct.getDayOfMonthYear(startingTimePoint, year0, month0);
                            int hour = calInstanct.getHourOfDay(endingTimePoint)
                                    - calInstanct.getHourOfDay(startingTimePoint);
                            int min = calInstanct.getMinOfHour(endingTimePoint)
                                    - calInstanct.getMinOfHour(startingTimePoint);
                            int sec = calInstanct.getSecOfMin(endingTimePoint)
                                    - calInstanct.getSecOfMin(startingTimePoint);
                            int ms = calInstanct.getMillisOfSec(endingTimePoint)
                                    - calInstanct.getMillisOfSec(startingTimePoint);

                            if (ms < 0) {
                                ms += GregorianCalendarSystem.CHRONON_OF_SECOND;
                                sec -= 1;
                            }

                            if (sec < 0) {
                                sec += GregorianCalendarSystem.CHRONON_OF_MINUTE
                                        / GregorianCalendarSystem.CHRONON_OF_SECOND;
                                min -= 1;
                            }

                            if (min < 0) {
                                min += GregorianCalendarSystem.CHRONON_OF_HOUR
                                        / GregorianCalendarSystem.CHRONON_OF_MINUTE;
                                hour -= 1;
                            }

                            if (hour < 0) {
                                hour += GregorianCalendarSystem.CHRONON_OF_DAY
                                        / GregorianCalendarSystem.CHRONON_OF_HOUR;
                                day -= 1;
                            }

                            if (day < 0) {
                                boolean isLeapYear = calInstanct.isLeapYear(year1);
                                // need to "borrow" the days in previous month to make the day positive; when month is
                                // 1 (Jan), Dec will be borrowed
                                day += isLeapYear ? (GregorianCalendarSystem.DAYS_OF_MONTH_LEAP[(12 + month1 - 2) % 12])
                                        : (GregorianCalendarSystem.DAYS_OF_MONTH_ORDI[(12 + month1 - 2) % 12]);
                                month -= 1;
                            }

                            if (month < 0) {
                                month += GregorianCalendarSystem.MONTHS_IN_A_YEAR;
                                year -= 1;
                            }

                            if (negative) {
                                aDuration.setValue(-1 * (year * GregorianCalendarSystem.MONTHS_IN_A_YEAR + month),
                                        -1 * (day * GregorianCalendarSystem.CHRONON_OF_DAY
                                                + hour * GregorianCalendarSystem.CHRONON_OF_HOUR
                                                + min * GregorianCalendarSystem.CHRONON_OF_MINUTE
                                                + sec * GregorianCalendarSystem.CHRONON_OF_SECOND + ms));
                            } else {
                                aDuration.setValue(year * GregorianCalendarSystem.MONTHS_IN_A_YEAR + month,
                                        day * GregorianCalendarSystem.CHRONON_OF_DAY
                                                + hour * GregorianCalendarSystem.CHRONON_OF_HOUR
                                                + min * GregorianCalendarSystem.CHRONON_OF_MINUTE
                                                + sec * GregorianCalendarSystem.CHRONON_OF_SECOND + ms);
                            }
                        }
                        durationSerde.serialize(aDuration, out);
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    /* (non-Javadoc)
    * @see org.apache.asterix.om.functions.IFunctionDescriptor#getIdentifier()
    */
    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}
