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
package org.apache.asterix.runtime.evaluators.accessors;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.UTF8StringCharacterIterator;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class TemporalYearAccessor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private static final FunctionIdentifier FID = AsterixBuiltinFunctions.ACCESSOR_TEMPORAL_YEAR;

    // allowed input types
    private static final byte SER_DATE_TYPE_TAG = ATypeTag.DATE.serialize();
    private static final byte SER_DATETIME_TYPE_TAG = ATypeTag.DATETIME.serialize();
    private static final byte SER_DURATION_TYPE_TAG = ATypeTag.DURATION.serialize();
    private static final byte SER_YEAR_MONTH_DURATION_TYPE_TAG = ATypeTag.YEARMONTHDURATION.serialize();
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

                    private final DataOutput out = output.getDataOutput();

                    private final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();

                    private final ICopyEvaluator eval = args[0].createEvaluator(argOut);

                    private final GregorianCalendarSystem calSystem = GregorianCalendarSystem.getInstance();

                    private final UTF8StringPointable strExprPtr = new UTF8StringPointable();
                    private final UTF8StringCharacterIterator strIter = new UTF8StringCharacterIterator();

                    // for output: type integer
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInt64> intSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT64);
                    private final AMutableInt64 aMutableInt64 = new AMutableInt64(0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut.reset();
                        eval.evaluate(tuple);
                        byte[] bytes = argOut.getByteArray();

                        try {

                            if (bytes[0] == SER_DURATION_TYPE_TAG) {
                                aMutableInt64.setValue(calSystem.getDurationYear(ADurationSerializerDeserializer
                                        .getYearMonth(bytes, 1)));
                                intSerde.serialize(aMutableInt64, out);
                                return;
                            }

                            if (bytes[0] == SER_YEAR_MONTH_DURATION_TYPE_TAG) {
                                aMutableInt64.setValue(calSystem
                                        .getDurationYear(AYearMonthDurationSerializerDeserializer
                                                .getYearMonth(bytes, 1)));
                                intSerde.serialize(aMutableInt64, out);
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
                                strExprPtr.set(bytes, 1, bytes.length);
                                strIter.reset(strExprPtr);
                                char firstChar = strIter.next();
                                if (firstChar == '-') {
                                    // in case of a negative year
                                    year = -1
                                            * ((strIter.next() - '0') * 1000
                                            + (strIter.next() - '0') * 100
                                            + (strIter.next() - '0') * 10 + (strIter.next() - '0'));
                                } else {
                                    year = (firstChar - '0') * 1000
                                            + (strIter.next() - '0') * 100
                                            + (strIter.next() - '0') * 10
                                            + (strIter.next() - '0');
                                }
                                aMutableInt64.setValue(year);
                                intSerde.serialize(aMutableInt64, out);
                                return;
                            } else {
                                throw new AlgebricksException("Inapplicable input type: " + bytes[0]);
                            }

                            int year = calSystem.getYear(chrononTimeInMs);

                            aMutableInt64.setValue(year);
                            intSerde.serialize(aMutableInt64, out);

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
