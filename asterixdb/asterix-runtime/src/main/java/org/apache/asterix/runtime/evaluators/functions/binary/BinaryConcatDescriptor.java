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

package org.apache.asterix.runtime.evaluators.functions.binary;

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
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

@MissingNullInOutFunction
public class BinaryConcatDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new BinaryConcatDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.BINARY_CONCAT;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractBinaryScalarEvaluator(ctx, args, getIdentifier(), sourceLoc) {

                    private final ListAccessor listAccessor = new ListAccessor();
                    private final byte[] metaBuffer = new byte[5];
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AMissing> missingSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AMISSING);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        evaluators[0].evaluate(tuple, pointables[0]);

                        if (PointableHelper.checkAndSetMissingOrNull(result, pointables[0])) {
                            return;
                        }

                        byte[] data = pointables[0].getByteArray();
                        int offset = pointables[0].getStartOffset();
                        byte typeTag = data[offset];
                        if (typeTag != ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG
                                && typeTag != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, typeTag,
                                    ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG,
                                    ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG);
                        }
                        try {
                            listAccessor.reset(data, offset);
                            int concatLength = 0;
                            boolean itemIsNull = false;
                            for (int i = 0; i < listAccessor.size(); i++) {
                                int itemOffset = listAccessor.getItemOffset(i);
                                ATypeTag itemType = listAccessor.getItemType(itemOffset);
                                if (itemType != ATypeTag.BINARY) {
                                    if (itemType == ATypeTag.NULL) {
                                        itemIsNull = true;
                                        continue;
                                    }
                                    if (itemType == ATypeTag.MISSING) {
                                        missingSerde.serialize(AMissing.MISSING, dataOutput);
                                        result.set(resultStorage);
                                        return;
                                    }
                                    throw new UnsupportedItemTypeException(sourceLoc, getIdentifier(),
                                            itemType.serialize());
                                }
                                concatLength += ByteArrayPointable.getContentLength(data, itemOffset);
                            }
                            if (itemIsNull) {
                                nullSerde.serialize(ANull.NULL, dataOutput);
                                result.set(resultStorage);
                                return;
                            }
                            dataOutput.writeByte(ATypeTag.SERIALIZED_BINARY_TYPE_TAG);
                            int metaLen = VarLenIntEncoderDecoder.encode(concatLength, metaBuffer, 0);
                            dataOutput.write(metaBuffer, 0, metaLen);

                            for (int i = 0; i < listAccessor.size(); i++) {
                                int itemOffset = listAccessor.getItemOffset(i);
                                int length = ByteArrayPointable.getContentLength(data, itemOffset);
                                dataOutput.write(data,
                                        itemOffset + ByteArrayPointable.getNumberBytesToStoreMeta(length), length);
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
}
