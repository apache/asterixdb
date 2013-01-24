/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInterval;
import edu.uci.ics.asterix.om.base.AMutableDuration;
import edu.uci.ics.asterix.om.base.AMutableInterval;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.temporal.ADateParserFactory;
import edu.uci.ics.asterix.om.base.temporal.ADurationParserFactory;
import edu.uci.ics.asterix.om.base.temporal.ATimeParserFactory;
import edu.uci.ics.asterix.om.base.temporal.ByteArrayCharSequenceAccessor;
import edu.uci.ics.asterix.om.base.temporal.DurationArithmeticOperations;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class AIntervalStartFromDateTimeConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-start-from-datetime", 2);
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

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

                    private ByteArrayCharSequenceAccessor charAccessor = new ByteArrayCharSequenceAccessor();

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
                            } else if (argOut0.getByteArray()[0] == SER_STRING_TYPE_TAG
                                    && argOut1.getByteArray()[0] == SER_STRING_TYPE_TAG) {
                                // start date

                                int stringLength = (argOut0.getByteArray()[1] & 0xff << 8)
                                        + (argOut0.getByteArray()[2] & 0xff << 0);

                                charAccessor.reset(argOut0.getByteArray(), 3, stringLength);
                                // get offset for time part: +1 if it is negative (-)
                                short timeOffset = (short) ((charAccessor.getCharAt(0) == '-') ? 1 : 0);

                                if (charAccessor.getCharAt(timeOffset + 10) != 'T'
                                        && charAccessor.getCharAt(timeOffset + 8) != 'T') {
                                    throw new AlgebricksException(errorMessage + ": missing T");
                                }

                                // if extended form 11, else 9
                                timeOffset += (charAccessor.getCharAt(timeOffset + 13) == ':') ? (short) (11)
                                        : (short) (9);
                                long intervalStart = ADateParserFactory.parseDatePart(charAccessor, false);
                                charAccessor.reset(argOut0.getByteArray(), 3 + timeOffset, stringLength - timeOffset);
                                intervalStart += ATimeParserFactory.parseTimePart(charAccessor);

                                // duration

                                stringLength = (argOut1.getByteArray()[1] & 0xff << 8)
                                        + (argOut1.getByteArray()[2] & 0xff << 0);

                                charAccessor.reset(argOut1.getByteArray(), 3, stringLength);
                                ADurationParserFactory.parseDuration(charAccessor, aDuration);

                                long intervalEnd = DurationArithmeticOperations.addDuration(intervalStart,
                                        aDuration.getMonths(), aDuration.getMilliseconds());

                                if (intervalEnd < intervalStart) {
                                    throw new AlgebricksException(
                                            "Interval end must not be less than the interval start.");
                                }

                                aInterval.setValue(intervalStart, intervalEnd, ATypeTag.DATETIME.serialize());
                                intervalSerde.serialize(aInterval, out);
                            } else {
                                throw new AlgebricksException("Wrong format for interval constructor from dates.");
                            }

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