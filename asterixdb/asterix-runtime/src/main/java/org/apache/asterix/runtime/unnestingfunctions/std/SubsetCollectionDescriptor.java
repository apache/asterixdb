/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.unnestingfunctions.std;

import java.io.IOException;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.runtime.unnestingfunctions.base.AbstractUnnestingFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SubsetCollectionDescriptor extends AbstractUnnestingFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SubsetCollectionDescriptor();
        }
    };

    @Override
    public IUnnestingEvaluatorFactory createUnnestingEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IUnnestingEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IUnnestingEvaluator createUnnestingEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IUnnestingEvaluator() {
                    private IPointable inputVal = new VoidPointable();
                    private IScalarEvaluator evalList = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator evalStart = args[1].createScalarEvaluator(ctx);
                    private IScalarEvaluator evalLen = args[2].createScalarEvaluator(ctx);
                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private int numItems;
                    private int numItemsMax;
                    private int posStart;
                    private int posCrt;
                    private ATypeTag itemTag;
                    private boolean selfDescList = false;
                    private boolean metUnknown = false;

                    @Override
                    public void init(IFrameTupleReference tuple) throws HyracksDataException {
                        try {
                            evalStart.evaluate(tuple, inputVal);
                            posStart = ATypeHierarchy.getIntegerValue(getIdentifier().getName(), 0,
                                    inputVal.getByteArray(), inputVal.getStartOffset());

                            evalLen.evaluate(tuple, inputVal);
                            numItems = ATypeHierarchy.getIntegerValue(getIdentifier().getName(), 1,
                                    inputVal.getByteArray(), inputVal.getStartOffset());

                            evalList.evaluate(tuple, inputVal);
                            byte[] serList = inputVal.getByteArray();
                            int offset = inputVal.getStartOffset();
                            metUnknown = false;

                            byte typeTag = serList[offset];
                            if (typeTag == ATypeTag.SERIALIZED_MISSING_TYPE_TAG
                                    || typeTag == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                                metUnknown = true;
                                return;
                            }

                            if (typeTag != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG
                                    && typeTag != ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                                throw new RuntimeDataException(ErrorCode.COERCION, getIdentifier().getName());
                            }
                            if (typeTag == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                                numItemsMax = AOrderedListSerializerDeserializer.getNumberOfItems(serList, offset);
                            } else {
                                numItemsMax = AUnorderedListSerializerDeserializer.getNumberOfItems(serList, offset);
                            }

                            itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[offset + 1]);
                            if (itemTag == ATypeTag.ANY) {
                                selfDescList = true;
                            }

                            posCrt = posStart;
                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                    }

                    @Override
                    public boolean step(IPointable result) throws HyracksDataException {
                        if (!metUnknown && posCrt < posStart + numItems && posCrt < numItemsMax) {
                            resultStorage.reset();
                            byte[] serList = inputVal.getByteArray();
                            int offset = inputVal.getStartOffset();
                            int itemLength = 0;
                            try {
                                int itemOffset =
                                        AOrderedListSerializerDeserializer.getItemOffset(serList, offset, posCrt);
                                if (selfDescList) {
                                    itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[itemOffset]);
                                }
                                itemLength = NonTaggedFormatUtil.getFieldValueLength(serList, itemOffset, itemTag,
                                        selfDescList);
                                if (!selfDescList) {
                                    resultStorage.getDataOutput().writeByte(itemTag.serialize());
                                }
                                resultStorage.getDataOutput().write(serList, itemOffset,
                                        itemLength + (!selfDescList ? 0 : 1));
                            } catch (IOException e) {
                                throw HyracksDataException.create(e);
                            }
                            result.set(resultStorage);
                            ++posCrt;
                            return true;
                        }
                        return false;
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SUBSET_COLLECTION;
    }

}
