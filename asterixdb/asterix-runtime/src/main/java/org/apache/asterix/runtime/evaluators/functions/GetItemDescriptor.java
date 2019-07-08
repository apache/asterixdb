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

import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class GetItemDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new GetItemDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new GetItemEvalFactory(args);
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.GET_ITEM;
    }

    private class GetItemEvalFactory implements IScalarEvaluatorFactory {

        private static final long serialVersionUID = 1L;
        private IScalarEvaluatorFactory listEvalFactory;
        private IScalarEvaluatorFactory indexEvalFactory;

        public GetItemEvalFactory(IScalarEvaluatorFactory[] args) {
            this.listEvalFactory = args[0];
            this.indexEvalFactory = args[1];
        }

        @Override
        public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
            return new IScalarEvaluator() {

                private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                private final DataOutput output = resultStorage.getDataOutput();
                private IPointable inputArgList = new VoidPointable();
                private IPointable inputArgIdx = new VoidPointable();
                private IScalarEvaluator evalList = listEvalFactory.createScalarEvaluator(ctx);
                private IScalarEvaluator evalIdx = indexEvalFactory.createScalarEvaluator(ctx);
                private byte[] missingBytes = new byte[] { ATypeTag.SERIALIZED_MISSING_TYPE_TAG };

                @Override
                public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                    try {
                        evalList.evaluate(tuple, inputArgList);
                        evalIdx.evaluate(tuple, inputArgIdx);

                        if (PointableHelper.checkAndSetMissingOrNull(result, inputArgList, inputArgIdx)) {
                            return;
                        }

                        byte[] serList = inputArgList.getByteArray();
                        int offset = inputArgList.getStartOffset();
                        byte[] indexBytes = inputArgIdx.getByteArray();
                        int indexOffset = inputArgIdx.getStartOffset();

                        int itemCount;
                        byte serListTypeTag = serList[offset];
                        if (serListTypeTag == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                            itemCount = AOrderedListSerializerDeserializer.getNumberOfItems(serList, offset);
                        } else if (serListTypeTag == ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                            itemCount = AUnorderedListSerializerDeserializer.getNumberOfItems(serList, offset);
                        } else {
                            throw new TypeMismatchException(sourceLoc, BuiltinFunctions.GET_ITEM, 0, serListTypeTag,
                                    ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG,
                                    ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG);
                        }

                        int itemIndex = ATypeHierarchy.getIntegerValue(BuiltinFunctions.GET_ITEM.getName(), 0,
                                indexBytes, indexOffset);

                        if (itemIndex < 0 || itemIndex >= itemCount) {
                            // Out-of-bound index access should return MISSING.
                            result.set(missingBytes, 0, 1);
                            return;
                        }

                        byte serItemTypeTag = serList[offset + 1];
                        ATypeTag itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serItemTypeTag);
                        boolean selfDescList = itemTag == ATypeTag.ANY;

                        int itemOffset = serListTypeTag == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG
                                ? AOrderedListSerializerDeserializer.getItemOffset(serList, offset, itemIndex)
                                : AUnorderedListSerializerDeserializer.getItemOffset(serList, offset, 0);

                        if (selfDescList) {
                            itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[itemOffset]);
                            int itemLength =
                                    NonTaggedFormatUtil.getFieldValueLength(serList, itemOffset, itemTag, true) + 1;
                            result.set(serList, itemOffset, itemLength);
                        } else {
                            int itemLength =
                                    NonTaggedFormatUtil.getFieldValueLength(serList, itemOffset, itemTag, false);
                            resultStorage.reset();
                            output.writeByte(serItemTypeTag);
                            output.write(serList, itemOffset, itemLength);
                            result.set(resultStorage);
                        }
                    } catch (IOException e) {
                        throw HyracksDataException.create(e);
                    }
                }
            };
        }

    }

}
