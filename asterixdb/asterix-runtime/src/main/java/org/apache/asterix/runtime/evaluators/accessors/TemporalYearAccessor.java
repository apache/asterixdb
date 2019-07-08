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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
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
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.UTF8StringCharacterIterator;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class TemporalYearAccessor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private static final FunctionIdentifier FID = BuiltinFunctions.ACCESSOR_TEMPORAL_YEAR;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new TemporalYearAccessor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IPointable argPtr = new VoidPointable();
                    private final IScalarEvaluator eval = args[0].createScalarEvaluator(ctx);

                    private final GregorianCalendarSystem calSystem = GregorianCalendarSystem.getInstance();

                    private final UTF8StringPointable strExprPtr = new UTF8StringPointable();
                    private final UTF8StringCharacterIterator strIter = new UTF8StringCharacterIterator();

                    // for output: type integer
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInt64> intSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
                    private final AMutableInt64 aMutableInt64 = new AMutableInt64(0);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        eval.evaluate(tuple, argPtr);

                        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr)) {
                            return;
                        }

                        byte[] bytes = argPtr.getByteArray();
                        int startOffset = argPtr.getStartOffset();
                        int len = argPtr.getLength();

                        resultStorage.reset();
                        try {
                            if (bytes[startOffset] == ATypeTag.SERIALIZED_DURATION_TYPE_TAG) {
                                aMutableInt64.setValue(calSystem.getDurationYear(
                                        ADurationSerializerDeserializer.getYearMonth(bytes, startOffset + 1)));
                                intSerde.serialize(aMutableInt64, out);
                                result.set(resultStorage);
                                return;
                            }
                            if (bytes[startOffset] == ATypeTag.SERIALIZED_YEAR_MONTH_DURATION_TYPE_TAG) {
                                aMutableInt64.setValue(calSystem.getDurationYear(
                                        AYearMonthDurationSerializerDeserializer.getYearMonth(bytes, startOffset + 1)));
                                intSerde.serialize(aMutableInt64, out);
                                result.set(resultStorage);
                                return;
                            }

                            long chrononTimeInMs = 0;
                            if (bytes[startOffset] == ATypeTag.SERIALIZED_DATE_TYPE_TAG) {
                                chrononTimeInMs = AInt32SerializerDeserializer.getInt(bytes, startOffset + 1)
                                        * GregorianCalendarSystem.CHRONON_OF_DAY;
                            } else if (bytes[startOffset] == ATypeTag.SERIALIZED_DATETIME_TYPE_TAG) {
                                chrononTimeInMs = AInt64SerializerDeserializer.getLong(bytes, startOffset + 1);
                            } else if (bytes[startOffset] == ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                                int year;
                                strExprPtr.set(bytes, startOffset + 1, len - 1);
                                strIter.reset(strExprPtr);
                                char firstChar = strIter.next();
                                if (firstChar == '-') {
                                    // in case of a negative year
                                    year = -1 * ((strIter.next() - '0') * 1000 + (strIter.next() - '0') * 100
                                            + (strIter.next() - '0') * 10 + (strIter.next() - '0'));
                                } else {
                                    year = (firstChar - '0') * 1000 + (strIter.next() - '0') * 100
                                            + (strIter.next() - '0') * 10 + (strIter.next() - '0');
                                }
                                aMutableInt64.setValue(year);
                                intSerde.serialize(aMutableInt64, out);
                                result.set(resultStorage);
                                return;
                            } else {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes[startOffset],
                                        ATypeTag.SERIALIZED_DURATION_TYPE_TAG,
                                        ATypeTag.SERIALIZED_YEAR_MONTH_DURATION_TYPE_TAG,
                                        ATypeTag.SERIALIZED_DATE_TYPE_TAG, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG,
                                        ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            }

                            int year = calSystem.getYear(chrononTimeInMs);
                            aMutableInt64.setValue(year);
                            intSerde.serialize(aMutableInt64, out);
                            result.set(resultStorage);
                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
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
