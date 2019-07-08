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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.asterix.runtime.exceptions.UnsupportedTypeException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

@MissingNullInOutFunction
public class CodePointToStringDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CodePointToStringDescriptor();
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
                    private IScalarEvaluatorFactory listEvalFactory = args[0];
                    private IPointable inputArgList = new VoidPointable();
                    private IScalarEvaluator evalList = listEvalFactory.createScalarEvaluator(ctx);

                    private final byte[] currentUTF8 = new byte[6];
                    private final byte[] tempStoreForLength = new byte[5];
                    private final byte stringTypeTag = ATypeTag.SERIALIZED_STRING_TYPE_TAG;

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        try {
                            resultStorage.reset();
                            evalList.evaluate(tuple, inputArgList);

                            if (PointableHelper.checkAndSetMissingOrNull(result, inputArgList)) {
                                return;
                            }

                            byte[] serOrderedList = inputArgList.getByteArray();
                            int offset = inputArgList.getStartOffset();
                            int size;

                            if (ATypeTag.VALUE_TYPE_MAPPING[serOrderedList[offset]] != ATypeTag.ARRAY) {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, serOrderedList[offset]);
                            } else {
                                switch (ATypeTag.VALUE_TYPE_MAPPING[serOrderedList[offset + 1]]) {
                                    case TINYINT:
                                    case SMALLINT:
                                    case INTEGER:
                                    case BIGINT:
                                    case FLOAT:
                                    case DOUBLE:
                                    case ANY:
                                        size = AOrderedListSerializerDeserializer.getNumberOfItems(serOrderedList,
                                                offset);
                                        break;
                                    default:
                                        throw new UnsupportedTypeException(sourceLoc, getIdentifier(),
                                                serOrderedList[offset]);
                                }
                            }
                            // calculate length first
                            int utf_8_len = 0;
                            for (int i = 0; i < size; i++) {
                                int itemOffset =
                                        AOrderedListSerializerDeserializer.getItemOffset(serOrderedList, offset, i);
                                int codePoint = 0;
                                codePoint = ATypeHierarchy.getIntegerValueWithDifferentTypeTagPosition(
                                        getIdentifier().getName(), 0, serOrderedList, itemOffset, offset + 1);
                                utf_8_len += UTF8StringUtil.codePointToUTF8(codePoint, currentUTF8);
                            }
                            out.writeByte(stringTypeTag);
                            UTF8StringUtil.writeUTF8Length(utf_8_len, tempStoreForLength, out);
                            for (int i = 0; i < size; i++) {
                                int itemOffset =
                                        AOrderedListSerializerDeserializer.getItemOffset(serOrderedList, offset, i);
                                int codePoint = 0;
                                codePoint = ATypeHierarchy.getIntegerValueWithDifferentTypeTagPosition(
                                        getIdentifier().getName(), 0, serOrderedList, itemOffset, offset + 1);
                                utf_8_len = UTF8StringUtil.codePointToUTF8(codePoint, currentUTF8);
                                for (int j = 0; j < utf_8_len; j++) {
                                    out.writeByte(currentUTF8[j]);
                                }
                            }
                            result.set(resultStorage);
                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.CODEPOINT_TO_STRING;
    }

}
