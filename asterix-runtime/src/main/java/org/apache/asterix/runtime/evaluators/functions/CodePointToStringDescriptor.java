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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class CodePointToStringDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CodePointToStringDescriptor();
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
                    private ICopyEvaluatorFactory listEvalFactory = args[0];
                    private ArrayBackedValueStorage outInputList = new ArrayBackedValueStorage();
                    private ICopyEvaluator evalList = listEvalFactory.createEvaluator(outInputList);

                    private final byte[] currentUTF8 = new byte[6];
                    private final byte[] tempStoreForLength = new byte[5];
                    private final byte stringTypeTag = ATypeTag.STRING.serialize();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            outInputList.reset();
                            evalList.evaluate(tuple);
                            byte[] serOrderedList = outInputList.getByteArray();
                            int size = 0;

                            if (ATypeTag.VALUE_TYPE_MAPPING[serOrderedList[0]] != ATypeTag.ORDEREDLIST) {
                                cannotProcessException(serOrderedList[0], serOrderedList[1]);
                            } else {
                                switch (ATypeTag.VALUE_TYPE_MAPPING[serOrderedList[1]]) {
                                    case INT8:
                                    case INT16:
                                    case INT32:
                                    case INT64:
                                    case FLOAT:
                                    case DOUBLE:
                                    case ANY:
                                        size = AOrderedListSerializerDeserializer.getNumberOfItems(serOrderedList);
                                        break;
                                    default:
                                        cannotProcessException(serOrderedList[0], serOrderedList[1]);
                                }
                            }

                            try {
                                // calculate length first
                                int utf_8_len = 0;
                                for (int i = 0; i < size; i++) {
                                    int itemOffset = AOrderedListSerializerDeserializer
                                            .getItemOffset(serOrderedList, i);
                                    int codePoint = 0;
                                    codePoint = ATypeHierarchy.getIntegerValueWithDifferentTypeTagPosition(
                                            serOrderedList, itemOffset, 1);
                                    utf_8_len += UTF8StringUtil.codePointToUTF8(codePoint, currentUTF8);
                                }
                                out.writeByte(stringTypeTag);
                                UTF8StringUtil.writeUTF8Length(utf_8_len, tempStoreForLength, out);
                                for (int i = 0; i < size; i++) {
                                    int itemOffset = AOrderedListSerializerDeserializer
                                            .getItemOffset(serOrderedList, i);
                                    int codePoint = 0;
                                    codePoint = ATypeHierarchy.getIntegerValueWithDifferentTypeTagPosition(
                                            serOrderedList, itemOffset, 1);
                                    utf_8_len = UTF8StringUtil.codePointToUTF8(codePoint, currentUTF8);
                                    for (int j = 0; j < utf_8_len; j++) {
                                        out.writeByte(currentUTF8[j]);
                                    }
                                }
                            } catch (AsterixException ex) {
                                throw new AlgebricksException(ex);
                            }
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
        return AsterixBuiltinFunctions.CODEPOINT_TO_STRING;
    }

    private void cannotProcessException(byte tag1, byte tag2) throws AlgebricksException {
        throw new AlgebricksException(AsterixBuiltinFunctions.CODEPOINT_TO_STRING.getName()
                + ": expects input type ORDEREDLIST/[INT8|INT16|INT32|INT64|FLOAT|DOUBLE] but got "
                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(tag1) + "/"
                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(tag2));
    }

}
