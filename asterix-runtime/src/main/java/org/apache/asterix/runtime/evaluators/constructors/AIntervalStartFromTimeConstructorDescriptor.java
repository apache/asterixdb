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

import org.apache.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.AMutableDuration;
import org.apache.asterix.om.base.AMutableInterval;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.temporal.ADurationParserFactory;
import org.apache.asterix.om.base.temporal.ATimeParserFactory;
import org.apache.asterix.om.base.temporal.DurationArithmeticOperations;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.base.temporal.ADurationParserFactory.ADurationParseOption;
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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class AIntervalStartFromTimeConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = AsterixBuiltinFunctions.INTERVAL_CONSTRUCTOR_START_FROM_TIME;
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte SER_TIME_TYPE_TAG = ATypeTag.TIME.serialize();
    private final static byte SER_DURATION_TYPE_TAG = ATypeTag.DURATION.serialize();
    private final static byte SER_DAY_TIME_DURATION_TYPE_TAG = ATypeTag.DAYTIMEDURATION.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AIntervalStartFromTimeConstructorDescriptor();
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

                            if (argOut0.getByteArray()[0] == SER_TIME_TYPE_TAG) {
                                intervalStart = ATimeSerializerDeserializer.getChronon(argOut0.getByteArray(), 1);
                            } else if (argOut0.getByteArray()[0] == SER_STRING_TYPE_TAG) {
                                int stringLength = (argOut0.getByteArray()[1] & 0xff << 8)
                                        + (argOut0.getByteArray()[2] & 0xff << 0);

                                intervalStart = ATimeParserFactory.parseTimePart(argOut0.getByteArray(), 3,
                                        stringLength);
                            } else {
                                throw new AlgebricksException(FID.getName()
                                        + ": expects NULL/STRING/TIME for the first argument, but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut0.getByteArray()[0]));
                            }

                            if (intervalStart < 0) {
                                intervalStart += GregorianCalendarSystem.CHRONON_OF_DAY;
                            }

                            if (argOut1.getByteArray()[0] == SER_DURATION_TYPE_TAG) {

                                if (ADurationSerializerDeserializer.getYearMonth(argOut1.getByteArray(), 1) != 0) {
                                    throw new AlgebricksException(FID.getName()
                                            + ": cannot add a year-month duration to a time value.");
                                }

                                intervalEnd = DurationArithmeticOperations.addDuration(intervalStart, 0,
                                        ADurationSerializerDeserializer.getDayTime(argOut1.getByteArray(), 1), false);

                            } else if (argOut1.getByteArray()[0] == SER_DAY_TIME_DURATION_TYPE_TAG) {

                                intervalEnd = DurationArithmeticOperations.addDuration(intervalStart, 0,
                                        ADayTimeDurationSerializerDeserializer.getDayTime(argOut1.getByteArray(), 1),
                                        false);

                            } else if (argOut1.getByteArray()[0] == SER_STRING_TYPE_TAG) {
                                // duration

                                int stringLength = (argOut1.getByteArray()[1] & 0xff << 8)
                                        + (argOut1.getByteArray()[2] & 0xff << 0);

                                ADurationParserFactory.parseDuration(argOut1.getByteArray(), 3, stringLength,
                                        aDuration, ADurationParseOption.All);

                                if (aDuration.getMonths() != 0) {
                                    throw new AlgebricksException(FID.getName()
                                            + ": cannot add a year-month duration to a time value.");
                                }

                                intervalEnd = DurationArithmeticOperations.addDuration(intervalStart, 0,
                                        aDuration.getMilliseconds(), false);
                            } else {
                                throw new AlgebricksException("Wrong format for interval constructor from time.");
                            }

                            if (intervalEnd > GregorianCalendarSystem.CHRONON_OF_DAY) {
                                intervalEnd = intervalEnd % (int) (GregorianCalendarSystem.CHRONON_OF_DAY);
                            }

                            if (intervalEnd < intervalStart) {
                                throw new AlgebricksException(FID.getName()
                                        + ": interval end must not be less than the interval start.");
                            }

                            aInterval.setValue(intervalStart, intervalEnd, ATypeTag.TIME.serialize());
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