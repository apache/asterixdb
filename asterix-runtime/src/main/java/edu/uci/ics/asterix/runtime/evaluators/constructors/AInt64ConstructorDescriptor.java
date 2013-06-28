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

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.AMutableInt64;
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
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class AInt64ConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AInt64ConstructorDescriptor();
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
                    private long value;
                    private int offset;
                    private boolean positive;
                    private String errorMessage = "This can not be an instance of int64";
                    private AMutableInt64 aInt64 = new AMutableInt64(0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInt64> int64Serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT64);
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
                                offset = 3;
                                value = 0;
                                positive = true;
                                if (serString[offset] == '+')
                                    offset++;
                                else if (serString[offset] == '-') {
                                    offset++;
                                    positive = false;
                                }
                                for (; offset < outInput.getLength(); offset++) {
                                    if (serString[offset] >= '0' && serString[offset] <= '9')
                                        value = value * 10 + serString[offset] - '0';
                                    else if (serString[offset] == 'i' && serString[offset + 1] == '6'
                                            && serString[offset + 2] == '4' && offset + 3 == outInput.getLength())
                                        break;
                                    else
                                        throw new AlgebricksException(errorMessage);
                                }
                                if (value < 0 && value != -9223372036854775808L) {
                                    throw new AlgebricksException(errorMessage);
                                }
                                if (value > 0 && !positive)
                                    value *= -1;

                                aInt64.setValue(value);
                                int64Serde.serialize(aInt64, out);
                            } else if (serString[0] == SER_NULL_TYPE_TAG)
                                nullSerde.serialize(ANull.NULL, out);
                            else
                                throw new AlgebricksException(errorMessage);
                        } catch (IOException e1) {
                            throw new AlgebricksException(errorMessage);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.INT64_CONSTRUCTOR;
    }

}