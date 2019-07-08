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
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.base.AMutableInt16;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableInt8;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
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
public class NumericSubtractDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new NumericSubtractDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.NUMERIC_SUBTRACT;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {

                return new IScalarEvaluator() {
                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    // one temp. buffer re-used by both children
                    private IPointable argPtr = new VoidPointable();
                    private IScalarEvaluator evalLeft = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator evalRight = args[1].createScalarEvaluator(ctx);
                    private double[] operands = new double[args.length];
                    private boolean metInt8 = false, metInt16 = false, metInt32 = false, metInt64 = false,
                            metFloat = false, metDouble = false;
                    private ATypeTag typeTag;
                    private AMutableDouble aDouble = new AMutableDouble(0);
                    private AMutableFloat aFloat = new AMutableFloat(0);
                    private AMutableInt64 aInt64 = new AMutableInt64(0);
                    private AMutableInt32 aInt32 = new AMutableInt32(0);
                    private AMutableInt16 aInt16 = new AMutableInt16((short) 0);
                    private AMutableInt8 aInt8 = new AMutableInt8((byte) 0);
                    @SuppressWarnings("rawtypes")
                    private ISerializerDeserializer serde;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        for (int i = 0; i < args.length; i++) {
                            if (i == 0) {
                                evalLeft.evaluate(tuple, argPtr);
                            } else {
                                evalRight.evaluate(tuple, argPtr);
                            }
                            byte[] data = argPtr.getByteArray();
                            int offset = argPtr.getStartOffset();
                            typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
                            switch (typeTag) {
                                case TINYINT:
                                    metInt8 = true;
                                    operands[i] = AInt8SerializerDeserializer.getByte(data, offset + 1);
                                    break;
                                case SMALLINT:
                                    metInt16 = true;
                                    operands[i] = AInt16SerializerDeserializer.getShort(data, offset + 1);
                                    break;
                                case INTEGER:
                                    metInt32 = true;
                                    operands[i] = AInt32SerializerDeserializer.getInt(data, offset + 1);
                                    break;
                                case BIGINT:
                                    metInt64 = true;
                                    operands[i] = AInt64SerializerDeserializer.getLong(data, offset + 1);
                                    break;
                                case FLOAT:
                                    metFloat = true;
                                    operands[i] = AFloatSerializerDeserializer.getFloat(data, offset + 1);
                                    break;
                                case DOUBLE:
                                    metDouble = true;
                                    operands[i] = ADoubleSerializerDeserializer.getDouble(data, offset + 1);
                                    break;
                                default:
                                    throw new TypeMismatchException(sourceLoc, getIdentifier(), i, data[offset],
                                            ATypeTag.SERIALIZED_INT8_TYPE_TAG, ATypeTag.SERIALIZED_INT16_TYPE_TAG,
                                            ATypeTag.SERIALIZED_INT32_TYPE_TAG, ATypeTag.SERIALIZED_INT64_TYPE_TAG,
                                            ATypeTag.SERIALIZED_FLOAT_TYPE_TAG, ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                            }
                        }

                        if (metDouble) {
                            serde = SerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.ADOUBLE);
                            aDouble.setValue(operands[0] - operands[1]);
                            serde.serialize(aDouble, out);
                        } else if (metFloat) {
                            serde = SerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.AFLOAT);
                            aFloat.setValue((float) (operands[0] - operands[1]));
                            serde.serialize(aFloat, out);
                        } else if (metInt64) {
                            serde = SerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.AINT64);
                            aInt64.setValue((long) (operands[0] - operands[1]));
                            serde.serialize(aInt64, out);
                        } else if (metInt32) {
                            serde = SerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.AINT32);
                            aInt32.setValue((int) (operands[0] - operands[1]));
                            serde.serialize(aInt32, out);
                        } else if (metInt16) {
                            serde = SerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.AINT16);
                            aInt16.setValue((short) (operands[0] - operands[1]));
                            serde.serialize(aInt16, out);
                        } else if (metInt8) {
                            serde = SerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.AINT8);
                            aInt8.setValue((byte) (operands[0] - operands[1]));
                            serde.serialize(aInt8, out);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

}
