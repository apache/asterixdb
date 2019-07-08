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
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
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
import org.apache.hyracks.util.string.UTF8StringUtil;

@MissingNullInOutFunction
public class StringConcatDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringConcatDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final ListAccessor listAccessor = new ListAccessor();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IScalarEvaluatorFactory listEvalFactory = args[0];
                    private final IPointable inputArgList = new VoidPointable();
                    private final IScalarEvaluator evalList = listEvalFactory.createScalarEvaluator(ctx);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AMissing> missingSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AMISSING);
                    private final byte[] tempLengthArray = new byte[5];

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        try {
                            evalList.evaluate(tuple, inputArgList);

                            if (PointableHelper.checkAndSetMissingOrNull(result, inputArgList)) {
                                return;
                            }

                            byte[] listBytes = inputArgList.getByteArray();
                            int listOffset = inputArgList.getStartOffset();

                            if (listBytes[listOffset] != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG
                                    && listBytes[listOffset] != ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, listBytes[listOffset],
                                        ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG,
                                        ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG);
                            }
                            listAccessor.reset(listBytes, listOffset);
                            // calculate length first
                            int utf8Len = 0;
                            boolean itemIsNull = false;
                            for (int i = 0; i < listAccessor.size(); i++) {
                                int itemOffset = listAccessor.getItemOffset(i);
                                ATypeTag itemType = listAccessor.getItemType(itemOffset);
                                // Increase the offset by 1 if the give list has heterogeneous elements,
                                // since the item itself has a typetag.
                                if (listAccessor.itemsAreSelfDescribing()) {
                                    itemOffset += 1;
                                }
                                if (itemType != ATypeTag.STRING) {
                                    if (itemType == ATypeTag.NULL) {
                                        itemIsNull = true;
                                        continue;
                                    }
                                    if (itemType == ATypeTag.MISSING) {
                                        missingSerde.serialize(AMissing.MISSING, out);
                                        result.set(resultStorage);
                                        return;
                                    }
                                    throw new UnsupportedItemTypeException(sourceLoc, getIdentifier(),
                                            itemType.serialize());
                                }
                                utf8Len += UTF8StringUtil.getUTFLength(listBytes, itemOffset);
                            }
                            if (itemIsNull) {
                                nullSerde.serialize(ANull.NULL, out);
                                result.set(resultStorage);
                                return;
                            }
                            out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            int cbytes = UTF8StringUtil.encodeUTF8Length(utf8Len, tempLengthArray, 0);
                            out.write(tempLengthArray, 0, cbytes);
                            for (int i = 0; i < listAccessor.size(); i++) {
                                int itemOffset = listAccessor.getItemOffset(i);
                                if (listAccessor.itemsAreSelfDescribing()) {
                                    itemOffset += 1;
                                }
                                utf8Len = UTF8StringUtil.getUTFLength(listBytes, itemOffset);
                                out.write(listBytes, UTF8StringUtil.getNumBytesToStoreLength(utf8Len) + itemOffset,
                                        utf8Len);
                            }
                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.STRING_CONCAT;
    }
}
