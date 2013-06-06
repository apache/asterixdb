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
package edu.uci.ics.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.ANull;
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
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ADoubleConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ADoubleConstructorDescriptor();
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
                    private String errorMessage = "This can not be an instance of double";
                    private final byte[] POSITIVE_INF = { 0, 3, 'I', 'N', 'F' };
                    private final byte[] NEGATIVE_INF = { 0, 4, '-', 'I', 'N', 'F' };
                    private final byte[] NAN = { 0, 3, 'N', 'a', 'N' };
                    // private int offset = 3, value = 0, integerPart = 0,
                    // fractionPart = 0, exponentPart = 0,
                    // pointIndex = 0, eIndex = 1;
                    // double doubleValue = 0;
                    // boolean positiveInteger = true, positiveExponent = true,
                    // expectingInteger = true,
                    // expectingFraction = false, expectingExponent = false;
                    IBinaryComparator utf8BinaryComparator = AqlBinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE
                            .createBinaryComparator();
                    private AMutableDouble aDouble = new AMutableDouble(0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADouble> doubleSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADOUBLE);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                        try {
                            outInput.reset();
                            eval.evaluate(tuple);
                            byte[] serString = outInput.getByteArray();
                            if (serString[0] == SER_STRING_TYPE_TAG) {

                                if (utf8BinaryComparator
                                        .compare(serString, 1, outInput.getLength(), POSITIVE_INF, 0, 5) == 0) {
                                    aDouble.setValue(Double.POSITIVE_INFINITY);
                                } else if (utf8BinaryComparator.compare(serString, 1, outInput.getLength(),
                                        NEGATIVE_INF, 0, 6) == 0) {
                                    aDouble.setValue(Double.NEGATIVE_INFINITY);
                                } else if (utf8BinaryComparator.compare(serString, 1, outInput.getLength(), NAN, 0, 5) == 0) {
                                    aDouble.setValue(Double.NaN);
                                } else
                                    // out.writeDouble(parseDouble(serString));
                                    aDouble.setValue(Double.parseDouble(new String(serString, 3,
                                            outInput.getLength() - 3, "UTF-8")));
                                doubleSerde.serialize(aDouble, out);
                            } else if (serString[0] == SER_NULL_TYPE_TAG)
                                nullSerde.serialize(ANull.NULL, out);
                            else
                                throw new AlgebricksException(errorMessage);
                        } catch (IOException e1) {
                            throw new AlgebricksException(errorMessage);
                        }
                    }

                    // private double parseDouble(byte[] serString) throws
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
                    // doubleValue = (float) (integerPart + (fractionPart * (1 /
                    // Math.pow(10, eIndex - pointIndex - 1))));
                    // doubleValue *= (float) Math.pow(10.0, exponentPart);
                    // if (integerPart != 0
                    // && (doubleValue == Float.POSITIVE_INFINITY || doubleValue
                    // == Float.NEGATIVE_INFINITY || doubleValue == 0))
                    // throw new AlgebricksException(errorMessage);
                    //
                    // if (doubleValue > 0 && !positiveInteger)
                    // doubleValue *= -1;
                    //
                    // return doubleValue;
                    // }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.DOUBLE_CONSTRUCTOR;
    }

}