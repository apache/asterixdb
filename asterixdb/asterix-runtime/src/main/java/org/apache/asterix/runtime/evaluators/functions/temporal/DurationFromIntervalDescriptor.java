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
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADayTimeDuration;
import org.apache.asterix.om.base.AMutableDayTimeDuration;
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

@MissingNullInOutFunction
public class DurationFromIntervalDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = BuiltinFunctions.DURATION_FROM_INTERVAL;
    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new DurationFromIntervalDescriptor();
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
                    private IPointable argPtr = new VoidPointable();
                    private IScalarEvaluator eval = args[0].createScalarEvaluator(ctx);

                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADayTimeDuration> dayTimeDurationSerde =
                            SerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.ADAYTIMEDURATION);

                    private AMutableDayTimeDuration aDayTimeDuration = new AMutableDayTimeDuration(0);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        eval.evaluate(tuple, argPtr);

                        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr)) {
                            return;
                        }

                        byte[] bytes = argPtr.getByteArray();
                        int offset = argPtr.getStartOffset();

                        if (bytes[offset] != ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes[offset],
                                    ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                        }
                        long chrononStart = AIntervalSerializerDeserializer.getIntervalStart(bytes, offset + 1);
                        long chrononEnd = AIntervalSerializerDeserializer.getIntervalEnd(bytes, offset + 1);
                        byte intervalTypeTag = AIntervalSerializerDeserializer.getIntervalTimeType(bytes, offset + 1);

                        if (intervalTypeTag == ATypeTag.SERIALIZED_DATE_TYPE_TAG) {
                            chrononStart *= GregorianCalendarSystem.CHRONON_OF_DAY;
                            chrononEnd *= GregorianCalendarSystem.CHRONON_OF_DAY;
                        }

                        aDayTimeDuration.setMilliseconds(chrononEnd - chrononStart);
                        dayTimeDurationSerde.serialize(aDayTimeDuration, out);
                        result.set(resultStorage);
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
