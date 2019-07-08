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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AMutableInt8;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
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
public class AInt8ConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AInt8ConstructorDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable inputArg = new VoidPointable();
                    private IScalarEvaluator eval = args[0].createScalarEvaluator(ctx);
                    private byte value;
                    private int offset;
                    private boolean positive;
                    private AMutableInt8 aInt8 = new AMutableInt8((byte) 0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInt8> int8Serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);
                    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        try {
                            eval.evaluate(tuple, inputArg);

                            if (PointableHelper.checkAndSetMissingOrNull(result, inputArg)) {
                                return;
                            }

                            byte[] serString = inputArg.getByteArray();
                            int startOffset = inputArg.getStartOffset();
                            int len = inputArg.getLength();

                            byte tt = serString[startOffset];
                            if (tt == ATypeTag.SERIALIZED_INT8_TYPE_TAG) {
                                result.set(inputArg);
                            } else if (tt == ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                                resultStorage.reset();
                                utf8Ptr.set(serString, startOffset + 1, len - 1);
                                offset = utf8Ptr.getCharStartOffset();
                                //accumulating value in negative domain
                                //otherwise Byte.MIN_VALUE = -(Byte.MAX_VALUE + 1) would have caused overflow
                                value = 0;
                                positive = true;
                                byte limit = -Byte.MAX_VALUE;
                                if (serString[offset] == '+') {
                                    offset++;
                                } else if (serString[offset] == '-') {
                                    offset++;
                                    positive = false;
                                    limit = Byte.MIN_VALUE;
                                }
                                int end = startOffset + len;
                                for (; offset < end; offset++) {
                                    int digit;
                                    if (serString[offset] >= '0' && serString[offset] <= '9') {
                                        value = (byte) (value * 10);
                                        digit = serString[offset] - '0';
                                    } else if (serString[offset] == 'i' && serString[offset + 1] == '8'
                                            && offset + 2 == end) {
                                        break;
                                    } else {
                                        throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                                ATypeTag.SERIALIZED_INT8_TYPE_TAG);
                                    }
                                    if (value < limit + digit) {
                                        throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                                ATypeTag.SERIALIZED_INT8_TYPE_TAG);
                                    }
                                    value = (byte) (value - digit);
                                }
                                if (value > 0) {
                                    throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                            ATypeTag.SERIALIZED_INT8_TYPE_TAG);
                                }
                                if (value < 0 && positive) {
                                    value *= -1;
                                }

                                aInt8.setValue(value);
                                int8Serde.serialize(aInt8, out);
                                result.set(resultStorage);
                            } else {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, tt,
                                        ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            }
                        } catch (IOException e1) {
                            throw new InvalidDataFormatException(sourceLoc, getIdentifier(), e1,
                                    ATypeTag.SERIALIZED_INT8_TYPE_TAG);
                        }
                    }
                };
            }
        };

    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.INT8_CONSTRUCTOR;
    }

}
