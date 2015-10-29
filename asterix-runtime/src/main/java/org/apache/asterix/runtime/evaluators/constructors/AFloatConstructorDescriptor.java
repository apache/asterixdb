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

import org.apache.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.base.ANull;
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
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class AFloatConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AFloatConstructorDescriptor();
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
                    private ArrayBackedValueStorage outInput = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval = args[0].createEvaluator(outInput);
                    private String errorMessage = "This can not be an instance of float";
                    private final byte[] POSITIVE_INF = UTF8StringUtil.writeStringToBytes("INF");
                    private final byte[] NEGATIVE_INF = UTF8StringUtil.writeStringToBytes("-INF");
                    private final byte[] NAN = UTF8StringUtil.writeStringToBytes("NaN");

                    // private int offset = 3, value = 0, pointIndex = 0, eIndex
                    // = 1;
                    // private int integerPart = 0, fractionPart = 0,
                    // exponentPart = 0;
                    // float floatValue = 0;
                    // boolean positiveInteger = true, positiveExponent = true,
                    // expectingInteger = true,
                    // expectingFraction = false, expectingExponent = false;
                    IBinaryComparator utf8BinaryComparator = AqlBinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE
                            .createBinaryComparator();
                    private AMutableFloat aFloat = new AMutableFloat(0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AFloat> floatSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AFLOAT);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                        try {
                            outInput.reset();
                            eval.evaluate(tuple);
                            byte[] serString = outInput.getByteArray();
                            if (serString[0] == SER_STRING_TYPE_TAG) {
                                if (utf8BinaryComparator
                                        .compare(serString, 1, outInput.getLength(), POSITIVE_INF, 0, 5) == 0) {
                                    aFloat.setValue(Float.POSITIVE_INFINITY);
                                } else if (utf8BinaryComparator.compare(serString, 1, outInput.getLength(),
                                        NEGATIVE_INF, 0, 6) == 0) {
                                    aFloat.setValue(Float.NEGATIVE_INFINITY);
                                } else if (utf8BinaryComparator.compare(serString, 1, outInput.getLength(), NAN, 0, 5)
                                        == 0) {
                                    aFloat.setValue(Float.NaN);
                                } else {
                                    utf8Ptr.set(serString, 1, outInput.getLength() -1);
                                    aFloat.setValue(Float.parseFloat(utf8Ptr.toString()));
                                }
                                floatSerde.serialize(aFloat, out);

                            } else if (serString[0] == SER_NULL_TYPE_TAG)
                                nullSerde.serialize(ANull.NULL, out);
                            else
                                throw new AlgebricksException(errorMessage);
                        } catch (IOException e1) {
                            throw new AlgebricksException(errorMessage);
                        }
                    }

                    // private float parseFloat(byte[] serString) throws
                    // AlgebricksException {
                    //
                    // if (serString[offset] == '+')
                    // offset++;
                    // else if (serString[offset] == '-') {
                    // offset++;
                    // positiveInteger = false;
                    // }
                    //
                    // if ((serString[offset] == '.') || (serString[offset] ==
                    // 'e') || (serString[offset] == 'E')
                    // || (serString[outInput.getLength() - 1] == '.')
                    // || (serString[outInput.getLength() - 1] == 'E')
                    // || (serString[outInput.getLength() - 1] == 'e'))
                    // throw new AlgebricksException(errorMessage);
                    //
                    // for (; offset < outInput.getLength(); offset++) {
                    // if (serString[offset] >= '0' && serString[offset] <= '9')
                    // {
                    // value = value * 10 + serString[offset] - '0';
                    // } else
                    // switch (serString[offset]) {
                    // case '.':
                    // if (expectingInteger) {
                    // if (serString[offset + 1] < '0' || serString[offset + 1]
                    // > '9')
                    // throw new AlgebricksException(errorMessage);
                    // expectingInteger = false;
                    // expectingFraction = true;
                    // integerPart = value;
                    // value = 0;
                    // pointIndex = offset;
                    // eIndex = outInput.getLength();
                    // } else
                    // throw new AlgebricksException(errorMessage);
                    // break;
                    // case 'e':
                    // case 'E':
                    // if (expectingInteger) {
                    // expectingInteger = false;
                    // integerPart = value;
                    // pointIndex = offset - 1;
                    // eIndex = offset;
                    // value = 0;
                    // expectingExponent = true;
                    // } else if (expectingFraction) {
                    //
                    // expectingFraction = false;
                    // fractionPart = value;
                    // eIndex = offset;
                    // value = 0;
                    // expectingExponent = true;
                    // } else
                    // throw new AlgebricksException();
                    //
                    // if (serString[offset + 1] == '+')
                    // offset++;
                    // else if (serString[offset + 1] == '-') {
                    // offset++;
                    // positiveExponent = false;
                    // } else if (serString[offset + 1] < '0' ||
                    // serString[offset + 1] > '9')
                    // throw new AlgebricksException(errorMessage);
                    // break;
                    // default:
                    // throw new AlgebricksException(errorMessage);
                    // }
                    // }
                    //
                    // if (expectingInteger)
                    // integerPart = value;
                    // else if (expectingFraction)
                    // fractionPart = value;
                    // else if (expectingExponent)
                    // exponentPart = value * (positiveExponent ? 1 : -1);
                    //
                    // // floatValue = (float) ( integerPart + ( fractionPart *
                    // (1.0f / Math.pow(10.0f, eIndex - pointIndex - 1))));
                    // // floatValue *= (float) Math.pow(10.0f, exponentPart);
                    //
                    // floatValue = Float.parseFloat(integerPart+"."+
                    // fractionPart+"e"+ exponentPart);
                    //
                    // if (integerPart != 0
                    // && (floatValue == Float.POSITIVE_INFINITY || floatValue
                    // == Float.NEGATIVE_INFINITY || floatValue == 0))
                    // throw new AlgebricksException(errorMessage);
                    //
                    // if (floatValue > 0 && !positiveInteger)
                    // floatValue *= -1;
                    //
                    // return floatValue;
                    // }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.FLOAT_CONSTRUCTOR;
    }
}