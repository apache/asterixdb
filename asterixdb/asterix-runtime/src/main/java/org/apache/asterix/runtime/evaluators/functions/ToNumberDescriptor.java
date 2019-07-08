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
import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.NumberUtils;
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
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class ToNumberDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ToNumberDescriptor();
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
                final AMutableInt64 aInt64 = new AMutableInt64(0);
                final AMutableDouble aDouble = new AMutableDouble(0);
                final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

                @SuppressWarnings("unchecked")
                final ISerializerDeserializer<AInt64> INT64_SERDE =
                        SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

                @SuppressWarnings("unchecked")
                final ISerializerDeserializer<ADouble> DOUBLE_SERDE =
                        SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

                return new IScalarEvaluator() {
                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        inputEval.evaluate(tuple, inputArg);
                        resultStorage.reset();

                        if (PointableHelper.checkAndSetMissingOrNull(result, inputArg)) {
                            return;
                        }

                        byte[] bytes = inputArg.getByteArray();
                        int startOffset = inputArg.getStartOffset();
                        ATypeTag tt = ATypeTag.VALUE_TYPE_MAPPING[bytes[startOffset]];
                        switch (tt) {
                            case TINYINT:
                            case SMALLINT:
                            case INTEGER:
                            case BIGINT:
                            case FLOAT:
                            case DOUBLE:
                                result.set(inputArg);
                                break;

                            case BOOLEAN:
                                boolean b = ABooleanSerializerDeserializer.getBoolean(bytes, startOffset + 1);
                                aInt64.setValue(b ? 1 : 0);
                                INT64_SERDE.serialize(aInt64, out);
                                result.set(resultStorage);
                                break;

                            case STRING:
                                utf8Ptr.set(bytes, startOffset + 1, inputArg.getLength() - 1);
                                if (NumberUtils.parseInt64(utf8Ptr, aInt64)) {
                                    INT64_SERDE.serialize(aInt64, out);
                                    result.set(resultStorage);
                                } else if (NumberUtils.parseDouble(utf8Ptr, aDouble)) {
                                    DOUBLE_SERDE.serialize(aDouble, out);
                                    result.set(resultStorage);
                                } else {
                                    PointableHelper.setNull(result);
                                }
                                break;

                            case ARRAY:
                            case MULTISET:
                            case OBJECT:
                                PointableHelper.setNull(result);
                                break;

                            default:
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes[startOffset],
                                        ATypeTag.SERIALIZED_INT8_TYPE_TAG, ATypeTag.SERIALIZED_INT16_TYPE_TAG,
                                        ATypeTag.SERIALIZED_INT32_TYPE_TAG, ATypeTag.SERIALIZED_INT64_TYPE_TAG,
                                        ATypeTag.SERIALIZED_FLOAT_TYPE_TAG, ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG,
                                        ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG, ATypeTag.SERIALIZED_STRING_TYPE_TAG,
                                        ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG,
                                        ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG,
                                        ATypeTag.SERIALIZED_RECORD_TYPE_TAG);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.TO_NUMBER;
    }
}
