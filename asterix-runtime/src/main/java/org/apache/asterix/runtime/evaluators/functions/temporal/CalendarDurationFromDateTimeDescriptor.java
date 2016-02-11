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

import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ADuration;
import org.apache.asterix.om.base.AMutableDuration;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.temporal.DurationArithmeticOperations;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
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
 * Here "human-readable" means the value of each field of the duration is within the value range of the field in the calendar system. For example, month would be in [0, 12), and hour would be in [0, 24).
 * <p/>
 * The result can be considered as a "field-based" difference between the two datetime value, but all negative values would be converted to be non-negative.
 * <p/>
 * In the implementation, we always do the subtraction from the later time point, resulting a positive duration always.
 * <p/>
 */
public class CalendarDurationFromDateTimeDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private final static long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = AsterixBuiltinFunctions.CALENDAR_DURATION_FROM_DATETIME;
    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CalendarDurationFromDateTimeDescriptor();
        }
    };

    /* (non-Javadoc)
     * @see org.apache.asterix.runtime.base.IScalarFunctionDynamicDescriptor#createEvaluatorFactory(org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory[])
     */
    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();
                    private ArrayBackedValueStorage argOut0 = new ArrayBackedValueStorage();
                    private ArrayBackedValueStorage argOut1 = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval0 = args[0].createEvaluator(argOut0);
                    private ICopyEvaluator eval1 = args[1].createEvaluator(argOut1);

                    // possible output types
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADuration> durationSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADURATION);

                    private AMutableDuration aDuration = new AMutableDuration(0, 0);

                    private GregorianCalendarSystem calInstanct = GregorianCalendarSystem.getInstance();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut0.reset();
                        eval0.evaluate(tuple);
                        argOut1.reset();
                        eval1.evaluate(tuple);

                        try {
                            if (argOut0.getByteArray()[0] == ATypeTag.SERIALIZED_NULL_TYPE_TAG
                                    || argOut1.getByteArray()[0] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                                return;
                            }

                            if (argOut0.getByteArray()[0] != ATypeTag.SERIALIZED_DATETIME_TYPE_TAG) {
                                throw new AlgebricksException(FID.getName()
                                        + ": expects type DATETIME/NULL for parameter 0 but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut0.getByteArray()[0]));
                            }

                            if (argOut1.getByteArray()[0] != ATypeTag.SERIALIZED_DURATION_TYPE_TAG) {
                                throw new AlgebricksException(FID.getName()
                                        + ": expects type DURATION/NULL for parameter 1 but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut1.getByteArray()[0]));
                            }

                            int yearMonthDurationInMonths = ADurationSerializerDeserializer
                                    .getYearMonth(argOut1.getByteArray(), 1);
                            long dayTimeDurationInMs = ADurationSerializerDeserializer
                                    .getDayTime(argOut1.getByteArray(), 1);

                            long startingTimePoint = ADateTimeSerializerDeserializer.getChronon(argOut0.getByteArray(),
                                    1);

                            long endingTimePoint = DurationArithmeticOperations.addDuration(startingTimePoint,
                                    yearMonthDurationInMonths, dayTimeDurationInMs, false);

                            if (startingTimePoint == endingTimePoint) {
                                aDuration.setValue(0, 0);
                            } else {

                                boolean negative = false;

                                if (endingTimePoint < startingTimePoint) {
                                    negative = true;
                                    // swap the starting and ending time, so that ending time is always larger than the starting time.
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
                                    // need to "borrow" the days in previous month to make the day positive; when month is 1 (Jan), Dec will be borrowed
                                    day += (isLeapYear)
                                            ? (GregorianCalendarSystem.DAYS_OF_MONTH_LEAP[(12 + month1 - 2) % 12])
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

                        } catch (HyracksDataException hex) {
                            throw new AlgebricksException(hex);
                        }
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
