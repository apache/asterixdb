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
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
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
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
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

@MissingNullInOutFunction
public class CreatePolygonDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CreatePolygonDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private final ListAccessor listAccessor = new ListAccessor();
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IScalarEvaluatorFactory listEvalFactory = args[0];
                    private final IPointable inputArgList = new VoidPointable();
                    private final IScalarEvaluator evalList = listEvalFactory.createScalarEvaluator(ctx);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ANull> nullSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AMissing> missingSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AMISSING);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        try {
                            resultStorage.reset();
                            evalList.evaluate(tuple, inputArgList);

                            if (PointableHelper.checkAndSetMissingOrNull(result, inputArgList)) {
                                return;
                            }

                            byte[] listBytes = inputArgList.getByteArray();
                            int offset = inputArgList.getStartOffset();

                            if (listBytes[offset] != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG
                                    && listBytes[offset] != ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, listBytes[offset],
                                        ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG,
                                        ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG);
                            }
                            listAccessor.reset(listBytes, offset);
                            // First check the list consists of a valid items
                            for (int i = 0; i < listAccessor.size(); i++) {
                                int itemOffset = listAccessor.getItemOffset(i);
                                ATypeTag itemType = listAccessor.getItemType(itemOffset);
                                if (itemType != ATypeTag.DOUBLE) {
                                    if (itemType == ATypeTag.NULL) {
                                        nullSerde.serialize(ANull.NULL, out);
                                        return;
                                    }
                                    if (itemType == ATypeTag.MISSING) {
                                        missingSerde.serialize(AMissing.MISSING, out);
                                        return;
                                    }
                                    throw new UnsupportedItemTypeException(sourceLoc, BuiltinFunctions.CREATE_POLYGON,
                                            itemType.serialize());
                                }

                            }
                            if (listAccessor.size() < 6) {
                                throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                        ATypeTag.SERIALIZED_POLYGON_TYPE_TAG);
                            } else if (listAccessor.size() % 2 != 0) {
                                throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                        ATypeTag.SERIALIZED_POLYGON_TYPE_TAG);
                            }
                            out.writeByte(ATypeTag.SERIALIZED_POLYGON_TYPE_TAG);
                            out.writeShort(listAccessor.size() / 2);

                            final int skipTypeTag = listAccessor.itemsAreSelfDescribing() ? 1 : 0;
                            for (int i = 0; i < listAccessor.size() / 2; i++) {
                                int firstDoubleOffset = listAccessor.getItemOffset(i * 2) + skipTypeTag;
                                int secondDobuleOffset = listAccessor.getItemOffset((i * 2) + 1) + skipTypeTag;

                                APointSerializerDeserializer.serialize(
                                        ADoubleSerializerDeserializer.getDouble(listBytes, firstDoubleOffset),
                                        ADoubleSerializerDeserializer.getDouble(listBytes, secondDobuleOffset), out);
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
        return BuiltinFunctions.CREATE_POLYGON;
    }
}
