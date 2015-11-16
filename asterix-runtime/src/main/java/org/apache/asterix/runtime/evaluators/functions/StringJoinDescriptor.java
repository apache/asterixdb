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
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class StringJoinDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringJoinDescriptor();
        }
    };
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
    private final byte stringTypeTag = ATypeTag.STRING.serialize();

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();
                    private ICopyEvaluatorFactory listEvalFactory = args[0];
                    private ICopyEvaluatorFactory sepEvalFactory = args[1];
                    private ArrayBackedValueStorage outInputList = new ArrayBackedValueStorage();
                    private ArrayBackedValueStorage outInputSep = new ArrayBackedValueStorage();
                    private ICopyEvaluator evalList = listEvalFactory.createEvaluator(outInputList);
                    private ICopyEvaluator evalSep = sepEvalFactory.createEvaluator(outInputSep);
                    private final byte[] tempLengthArray = new byte[5];

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            outInputList.reset();
                            evalList.evaluate(tuple);
                            byte[] serOrderedList = outInputList.getByteArray();

                            outInputSep.reset();
                            evalSep.evaluate(tuple);
                            byte[] serSep = outInputSep.getByteArray();
                            if (serOrderedList[0] != SER_ORDEREDLIST_TYPE_TAG
                                    && serOrderedList[1] != SER_STRING_TYPE_TAG) {
                                throw new AlgebricksException(AsterixBuiltinFunctions.STRING_JOIN.getName()
                                        + ": expects input type ORDEREDLIST but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serOrderedList[0]));
                            }

                            if (serSep[0] != SER_STRING_TYPE_TAG) {
                                throw new AlgebricksException(AsterixBuiltinFunctions.STRING_JOIN.getName()
                                        + ": expects STRING type for the seperator but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serSep[0]));
                            }

                            int size = AOrderedListSerializerDeserializer.getNumberOfItems(serOrderedList);
                            try {
                                // calculate length first
                                int utf_8_len = 0;
                                int sep_len = UTF8StringUtil.getUTFLength(serSep, 1);
                                int sep_meta_len = UTF8StringUtil.getNumBytesToStoreLength(sep_len);

                                for (int i = 0; i < size; i++) {
                                    int itemOffset = AOrderedListSerializerDeserializer
                                            .getItemOffset(serOrderedList, i);

                                    int currentSize = UTF8StringUtil.getUTFLength(serOrderedList, itemOffset);
                                    if (i != size - 1 && currentSize != 0) {
                                        utf_8_len += sep_len;
                                    }
                                    utf_8_len += currentSize;
                                }
                                out.writeByte(stringTypeTag);
                                int length = UTF8StringUtil.encodeUTF8Length(utf_8_len, tempLengthArray, 0);
                                out.write(tempLengthArray, 0, length);
                                for (int i = 0; i < size; i++) {
                                    int itemOffset = AOrderedListSerializerDeserializer
                                            .getItemOffset(serOrderedList, i);
                                    utf_8_len = UTF8StringUtil.getUTFLength(serOrderedList, itemOffset);
                                    out.write(serOrderedList,
                                            itemOffset + UTF8StringUtil.getNumBytesToStoreLength(utf_8_len), utf_8_len);
                                    if (i == size - 1 || utf_8_len == 0)
                                        continue;
                                    for (int j = 0; j < sep_len; j++) {
                                        out.writeByte(serSep[1 + sep_meta_len + j]);
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
        return AsterixBuiltinFunctions.STRING_JOIN;
    }
}
