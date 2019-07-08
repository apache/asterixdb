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

package org.apache.asterix.runtime.evaluators.functions;

import java.io.DataOutput;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
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
public class TreatAsIntegerDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new TreatAsIntegerDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {

                final IScalarEvaluator inputEval = args[0].createScalarEvaluator(ctx);
                final IPointable inputArg = new VoidPointable();
                final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                final DataOutput out = resultStorage.getDataOutput();
                final AMutableInt32 aInt32 = new AMutableInt32(0);

                @SuppressWarnings("unchecked")
                final ISerializerDeserializer<AInt32> int32Ser =
                        SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);

                return new IScalarEvaluator() {
                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        inputEval.evaluate(tuple, inputArg);

                        if (PointableHelper.checkAndSetMissingOrNull(result, inputArg)) {
                            return;
                        }

                        int intValue;
                        byte[] bytes = inputArg.getByteArray();
                        int startOffset = inputArg.getStartOffset();
                        ATypeTag tt = ATypeTag.VALUE_TYPE_MAPPING[bytes[startOffset]];
                        switch (tt) {
                            case TINYINT:
                            case SMALLINT:
                            case INTEGER:
                            case BIGINT:
                                intValue = ATypeHierarchy.getIntegerValue(getIdentifier().getName(), 0, bytes,
                                        startOffset, true);
                                break;
                            case FLOAT:
                            case DOUBLE:
                                double doubleValue =
                                        ATypeHierarchy.getDoubleValue(getIdentifier().getName(), 0, bytes, startOffset);
                                intValue = asInt(doubleValue);
                                break;
                            default:
                                throw new TypeMismatchException(sourceLoc, bytes[startOffset],
                                        ATypeTag.SERIALIZED_INT8_TYPE_TAG, ATypeTag.SERIALIZED_INT16_TYPE_TAG,
                                        ATypeTag.SERIALIZED_INT32_TYPE_TAG, ATypeTag.SERIALIZED_INT64_TYPE_TAG,
                                        ATypeTag.SERIALIZED_FLOAT_TYPE_TAG, ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                        }

                        resultStorage.reset();
                        aInt32.setValue(intValue);
                        int32Ser.serialize(aInt32, out);
                        result.set(resultStorage);
                    }

                    private int asInt(double d) throws HyracksDataException {
                        if (Double.isFinite(d)) {
                            long v = (long) d;
                            if (v == d && Integer.MIN_VALUE <= v && v <= Integer.MAX_VALUE) {
                                return (int) v;
                            }
                        }
                        throw new RuntimeDataException(ErrorCode.INTEGER_VALUE_EXPECTED, sourceLoc, d);
                    }
                };
            }
        };

    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.TREAT_AS_INTEGER;
    }
}
