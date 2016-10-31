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
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
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
        return AsterixBuiltinFunctions.GET_ITEM;
    }

    private static class GetItemEvalFactory implements IScalarEvaluatorFactory {

        private static final long serialVersionUID = 1L;
        private IScalarEvaluatorFactory listEvalFactory;
        private IScalarEvaluatorFactory indexEvalFactory;
        private byte serItemTypeTag;
        private ATypeTag itemTag;
        private boolean selfDescList = false;

        public GetItemEvalFactory(IScalarEvaluatorFactory[] args) {
            this.listEvalFactory = args[0];
            this.indexEvalFactory = args[1];
        }

        @Override
        public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
            return new IScalarEvaluator() {

                private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                private final DataOutput output = resultStorage.getDataOutput();
                private IPointable inputArgList = new VoidPointable();
                private IPointable inputArgIdx = new VoidPointable();
                private IScalarEvaluator evalList = listEvalFactory.createScalarEvaluator(ctx);
                private IScalarEvaluator evalIdx = indexEvalFactory.createScalarEvaluator(ctx);
                private byte[] missingBytes = new byte[] { ATypeTag.SERIALIZED_MISSING_TYPE_TAG };
                private int itemIndex;
                private int itemOffset;
                private int itemLength;

                @Override
                public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                    try {
                        evalList.evaluate(tuple, inputArgList);
                        evalIdx.evaluate(tuple, inputArgIdx);

                        byte[] serOrderedList = inputArgList.getByteArray();
                        int offset = inputArgList.getStartOffset();
                        byte[] indexBytes = inputArgIdx.getByteArray();
                        int indexOffset = inputArgIdx.getStartOffset();

                        if (serOrderedList[offset] == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                            itemIndex = ATypeHierarchy.getIntegerValue(AsterixBuiltinFunctions.GET_ITEM.getName(), 0,
                                    indexBytes, indexOffset);
                        } else {
                            throw new TypeMismatchException(AsterixBuiltinFunctions.GET_ITEM,
                                    0, serOrderedList[offset], ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG);
                        }

                        if (itemIndex < 0 || itemIndex >= AOrderedListSerializerDeserializer
                                .getNumberOfItems(serOrderedList, offset)) {
                            // Out-of-bound index access should return MISSING.
                            result.set(missingBytes, 0, 1);
                            return;
                        }

                        itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serOrderedList[offset + 1]);
                        if (itemTag == ATypeTag.ANY) {
                            selfDescList = true;
                        } else {
                            serItemTypeTag = serOrderedList[offset + 1];
                        }

                        itemOffset = AOrderedListSerializerDeserializer.getItemOffset(serOrderedList, offset,
                                itemIndex);

                        if (selfDescList) {
                            itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serOrderedList[itemOffset]);
                            itemLength = NonTaggedFormatUtil.getFieldValueLength(serOrderedList, itemOffset, itemTag,
                                    true) + 1;
                            result.set(serOrderedList, itemOffset, itemLength);
                        } else {
                            itemLength = NonTaggedFormatUtil.getFieldValueLength(serOrderedList, itemOffset, itemTag,
                                    false);
                            resultStorage.reset();
                            output.writeByte(serItemTypeTag);
                            output.write(serOrderedList, itemOffset, itemLength);
                            result.set(resultStorage);
                        }
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    } catch (AsterixException e) {
                        throw new HyracksDataException(e);
                    }
                }
            };
        }

    }

}
