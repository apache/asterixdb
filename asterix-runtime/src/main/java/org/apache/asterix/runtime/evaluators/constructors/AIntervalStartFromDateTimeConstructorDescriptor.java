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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.AMutableDuration;
import org.apache.asterix.om.base.AMutableInterval;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.temporal.ADateParserFactory;
import org.apache.asterix.om.base.temporal.ADurationParserFactory;
import org.apache.asterix.om.base.temporal.ADurationParserFactory.ADurationParseOption;
import org.apache.asterix.om.base.temporal.ATimeParserFactory;
import org.apache.asterix.om.base.temporal.DurationArithmeticOperations;
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
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class AIntervalStartFromDateTimeConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = AsterixBuiltinFunctions.INTERVAL_CONSTRUCTOR_START_FROM_DATETIME;
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte SER_DATETIME_TYPE_TAG = ATypeTag.DATETIME.serialize();
    private final static byte SER_DURATION_TYPE_TAG = ATypeTag.DURATION.serialize();
    private final static byte SER_DAY_TIME_DURATION_TYPE_TAG = ATypeTag.DAYTIMEDURATION.serialize();
    private final static byte SER_YEAR_MONTH_DURATION_TYPE_TAG = ATypeTag.YEARMONTHDURATION.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AIntervalStartFromDateTimeConstructorDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
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
                    private String errorMessage = "This can not be an instance of interval (from Date)";

                    private AMutableInterval aInterval = new AMutableInterval(0L, 0L, (byte) 0);
                    private AMutableDuration aDuration = new AMutableDuration(0, 0L);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInterval> intervalSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINTERVAL);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                        argOut0.reset();
                        argOut1.reset();
                        eval0.evaluate(tuple);
                        eval1.evaluate(tuple);

                        try {

                            if (argOut0.getByteArray()[0] == SER_NULL_TYPE_TAG
                                    || argOut1.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                                return;
                            }

                            long intervalStart = 0, intervalEnd = 0;

                            if (argOut0.getByteArray()[0] == SER_DATETIME_TYPE_TAG) {
                                intervalStart = ADateTimeSerializerDeserializer.getChronon(argOut0.getByteArray(), 1);
                            } else if (argOut0.getByteArray()[0] == SER_STRING_TYPE_TAG) {
                                utf8Ptr.set(argOut0.getByteArray(), 1, argOut0.getLength() - 1);

                                int stringLength = utf8Ptr.getUTF8Length();
                                int startOffset = utf8Ptr.getCharStartOffset();

                                // get offset for time part: +1 if it is negative (-)
                                short timeOffset = (short) ((argOut0.getByteArray()[startOffset] == '-') ? 1 : 0);

                                timeOffset += 8;

                                if (argOut0.getByteArray()[startOffset + timeOffset] != 'T') {
                                    timeOffset += 2;
                                    if (argOut0.getByteArray()[startOffset + timeOffset] != 'T') {
                                        throw new AlgebricksException(errorMessage + ": missing T");
                                    }
                                }

                                intervalStart = ADateParserFactory
                                        .parseDatePart(argOut0.getByteArray(), startOffset, timeOffset);
                                intervalStart += ATimeParserFactory.parseTimePart(argOut0.getByteArray(),
                                        startOffset + timeOffset + 1, stringLength - timeOffset - 1);
                            } else {
                                throw new AlgebricksException(FID.getName()
                                        + ": expects NULL/STRING/DATETIME for the first argument but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut0.getByteArray()[0]));
                            }

                            if (argOut1.getByteArray()[0] == SER_DURATION_TYPE_TAG) {
                                intervalEnd = DurationArithmeticOperations.addDuration(intervalStart,
                                        ADurationSerializerDeserializer.getYearMonth(argOut1.getByteArray(), 1),
                                        ADurationSerializerDeserializer.getDayTime(argOut1.getByteArray(), 1), false);
                            } else if (argOut1.getByteArray()[0] == SER_YEAR_MONTH_DURATION_TYPE_TAG) {
                                intervalEnd = DurationArithmeticOperations
                                        .addDuration(
                                                intervalStart,
                                                AYearMonthDurationSerializerDeserializer.getYearMonth(
                                                        argOut1.getByteArray(), 1), 0, false);
                            } else if (argOut1.getByteArray()[0] == SER_DAY_TIME_DURATION_TYPE_TAG) {
                                intervalEnd = DurationArithmeticOperations.addDuration(intervalStart, 0,
                                        ADayTimeDurationSerializerDeserializer.getDayTime(argOut1.getByteArray(), 1),
                                        false);
                            } else if (argOut1.getByteArray()[0] == SER_STRING_TYPE_TAG) {

                                // duration
                                utf8Ptr.set(argOut1.getByteArray(), 1, argOut1.getLength() - 1);
                                int stringLength = utf8Ptr.getUTF8Length();

                                ADurationParserFactory
                                        .parseDuration(argOut1.getByteArray(), utf8Ptr.getCharStartOffset(),
                                                stringLength, aDuration, ADurationParseOption.All);

                                intervalEnd = DurationArithmeticOperations.addDuration(intervalStart,
                                        aDuration.getMonths(), aDuration.getMilliseconds(), false);
                            } else {
                                throw new AlgebricksException(FID.getName()
                                        + ": expects NULL/STRING/DURATION for the second argument but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut1.getByteArray()[0]));
                            }

                            if (intervalEnd < intervalStart) {
                                throw new AlgebricksException(FID.getName()
                                        + ": interval end must not be less than the interval start.");
                            }

                            aInterval.setValue(intervalStart, intervalEnd, ATypeTag.DATETIME.serialize());
                            intervalSerde.serialize(aInterval, out);

                        } catch (IOException e1) {
                            throw new AlgebricksException(errorMessage);
                        } catch (Exception e2) {
                            throw new AlgebricksException(e2);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}