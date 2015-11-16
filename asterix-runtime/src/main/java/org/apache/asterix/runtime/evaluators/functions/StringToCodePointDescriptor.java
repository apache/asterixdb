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
import java.io.IOException;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class StringToCodePointDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringToCodePointDescriptor();
        }
    };
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {
                    protected final DataOutput out = output.getDataOutput();
                    ;
                    protected final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
                    protected final ICopyEvaluator stringEval = args[0].createEvaluator(argOut);
                    protected final AOrderedListType intListType = new AOrderedListType(BuiltinType.AINT64, null);

                    private OrderedListBuilder listBuilder = new OrderedListBuilder();
                    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInt64> int64Serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT64);
                    private final AMutableInt64 aInt64 = new AMutableInt64(0);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            argOut.reset();
                            stringEval.evaluate(tuple);
                            byte[] serString = argOut.getByteArray();

                            if (serString[0] == SER_STRING_TYPE_TAG) {
                                byte[] bytes = argOut.getByteArray();
                                int len = UTF8StringUtil.getUTFLength(bytes, 1);

                                int start = 1 + UTF8StringUtil.getNumBytesToStoreLength(len);
                                int pos = 0;
                                listBuilder.reset(intListType);
                                while (pos < len) {
                                    int codePoint = UTF8StringUtil.UTF8ToCodePoint(bytes, start + pos);
                                    pos += UTF8StringUtil.charSize(bytes, start + pos);

                                    inputVal.reset();
                                    aInt64.setValue(codePoint);
                                    int64Serde.serialize(aInt64, inputVal.getDataOutput());
                                    listBuilder.addItem(inputVal);

                                }
                                listBuilder.write(out, true);
                            } else
                                throw new AlgebricksException(AsterixBuiltinFunctions.STRING_TO_CODEPOINT.getName()
                                        + ": expects input type STRING but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serString[0]));
                        } catch (IOException e1) {
                            throw new AlgebricksException(e1.getMessage());
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.STRING_TO_CODEPOINT;
    }

}
