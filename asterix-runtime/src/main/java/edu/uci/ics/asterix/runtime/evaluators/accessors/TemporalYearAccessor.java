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
package edu.uci.ics.asterix.runtime.evaluators.accessors;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
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
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class TemporalYearAccessor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private static final FunctionIdentifier FID = AsterixBuiltinFunctions.ACCESSOR_TEMPORAL_YEAR;

    // allowed input types
    private static final byte SER_DATE_TYPE_TAG = ATypeTag.DATE.serialize();
    private static final byte SER_DATETIME_TYPE_TAG = ATypeTag.DATETIME.serialize();
    private static final byte SER_DURATION_TYPE_TAG = ATypeTag.DURATION.serialize();
    private static final byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new TemporalYearAccessor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();

                    private ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();

                    private ICopyEvaluator eval = args[0].createEvaluator(argOut);

                    private GregorianCalendarSystem calSystem = GregorianCalendarSystem.getInstance();

                    // for output: type integer
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInt32> intSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT32);
                    private AMutableInt32 aMutableInt32 = new AMutableInt32(0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut.reset();
                        eval.evaluate(tuple);
                        byte[] bytes = argOut.getByteArray();

                        try {

                            if (bytes[0] == SER_DURATION_TYPE_TAG) {
                                aMutableInt32.setValue(calSystem.getDurationYear(ADurationSerializerDeserializer
                                        .getYearMonth(bytes, 1)));
                                intSerde.serialize(aMutableInt32, out);
                                return;
                            }

                            long chrononTimeInMs = 0;
                            if (bytes[0] == SER_DATE_TYPE_TAG) {
                                chrononTimeInMs = AInt32SerializerDeserializer.getInt(bytes, 1)
                                        * GregorianCalendarSystem.CHRONON_OF_DAY;
                            } else if (bytes[0] == SER_DATETIME_TYPE_TAG) {
                                chrononTimeInMs = AInt64SerializerDeserializer.getLong(bytes, 1);
                            } else if (bytes[0] == SER_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                                return;
                            } else if (bytes[0] == SER_STRING_TYPE_TAG) {
                                int year;
                                if (UTF8StringPointable.charAt(bytes, 3) == '-') {
                                    // in case of a negative year
                                    year = -1
                                            * ((UTF8StringPointable.charAt(bytes, 4) - '0') * 1000
                                                    + (UTF8StringPointable.charAt(bytes, 5) - '0') * 100
                                                    + (UTF8StringPointable.charAt(bytes, 6) - '0') * 10 + (UTF8StringPointable
                                                    .charAt(bytes, 7) - '0'));
                                } else {
                                    year = (UTF8StringPointable.charAt(bytes, 3) - '0') * 1000
                                            + (UTF8StringPointable.charAt(bytes, 4) - '0') * 100
                                            + (UTF8StringPointable.charAt(bytes, 5) - '0') * 10
                                            + (UTF8StringPointable.charAt(bytes, 6) - '0');
                                }
                                aMutableInt32.setValue(year);
                                intSerde.serialize(aMutableInt32, out);
                                return;
                            } else {
                                throw new AlgebricksException("Inapplicable input type: " + bytes[0]);
                            }

                            int year = calSystem.getYear(chrononTimeInMs);

                            aMutableInt32.setValue(year);
                            intSerde.serialize(aMutableInt32, out);

                        } catch (IOException e) {
                            throw new AlgebricksException(e);
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
