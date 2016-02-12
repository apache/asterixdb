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
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class StringJoinDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringJoinDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws AlgebricksException {
                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IScalarEvaluatorFactory listEvalFactory = args[0];
                    private IScalarEvaluatorFactory sepEvalFactory = args[1];
                    private IPointable inputArgList = new VoidPointable();
                    private IPointable inputArgSep = new VoidPointable();
                    private IScalarEvaluator evalList = listEvalFactory.createScalarEvaluator(ctx);
                    private IScalarEvaluator evalSep = sepEvalFactory.createScalarEvaluator(ctx);
                    private final byte[] tempLengthArray = new byte[5];

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                        try {
                            resultStorage.reset();
                            evalList.evaluate(tuple, inputArgList);
                            byte[] serOrderedList = inputArgList.getByteArray();
                            int serOrderedListOffset = inputArgList.getStartOffset();

                            evalSep.evaluate(tuple, inputArgSep);
                            byte[] serSep = inputArgSep.getByteArray();
                            int serSepOffset = inputArgSep.getStartOffset();

                            if (serOrderedList[serOrderedListOffset] != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG
                                    && serOrderedList[serOrderedListOffset
                                            + 1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                                throw new AlgebricksException(AsterixBuiltinFunctions.STRING_JOIN.getName()
                                        + ": expects input type ORDEREDLIST but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER
                                                .deserialize(serOrderedList[serOrderedListOffset]));
                            }

                            if (serSep[serSepOffset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                                throw new AlgebricksException(AsterixBuiltinFunctions.STRING_JOIN.getName()
                                        + ": expects STRING type for the seperator but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serSep[serSepOffset]));
                            }

                            int size = AOrderedListSerializerDeserializer.getNumberOfItems(serOrderedList,
                                    serOrderedListOffset);
                            try {
                                // calculate length first
                                int utf_8_len = 0;
                                int sep_len = UTF8StringUtil.getUTFLength(serSep, serSepOffset + 1);
                                int sep_meta_len = UTF8StringUtil.getNumBytesToStoreLength(sep_len);

                                for (int i = 0; i < size; i++) {
                                    int itemOffset = AOrderedListSerializerDeserializer.getItemOffset(serOrderedList,
                                            serOrderedListOffset, i);

                                    int currentSize = UTF8StringUtil.getUTFLength(serOrderedList, itemOffset);
                                    if (i != size - 1 && currentSize != 0) {
                                        utf_8_len += sep_len;
                                    }
                                    utf_8_len += currentSize;
                                }
                                out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                                int length = UTF8StringUtil.encodeUTF8Length(utf_8_len, tempLengthArray, 0);
                                out.write(tempLengthArray, 0, length);
                                for (int i = 0; i < size; i++) {
                                    int itemOffset = AOrderedListSerializerDeserializer.getItemOffset(serOrderedList,
                                            serOrderedListOffset, i);
                                    utf_8_len = UTF8StringUtil.getUTFLength(serOrderedList, itemOffset);
                                    out.write(serOrderedList,
                                            itemOffset + UTF8StringUtil.getNumBytesToStoreLength(utf_8_len), utf_8_len);
                                    if (i == size - 1 || utf_8_len == 0) {
                                        continue;
                                    }
                                    for (int j = 0; j < sep_len; j++) {
                                        out.writeByte(serSep[serSepOffset + 1 + sep_meta_len + j]);
                                    }
                                }
                            } catch (AsterixException ex) {
                                throw new AlgebricksException(ex);
                            }
                            result.set(resultStorage);
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
