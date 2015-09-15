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
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class AnyCollectionMemberDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AnyCollectionMemberDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new AnyCollectionMemberEvalFactory(args[0]);
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.ANY_COLLECTION_MEMBER;
    }

    private static class AnyCollectionMemberEvalFactory implements ICopyEvaluatorFactory {

        private static final long serialVersionUID = 1L;

        private ICopyEvaluatorFactory listEvalFactory;
        private final static byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
        private final static byte SER_UNORDEREDLIST_TYPE_TAG = ATypeTag.UNORDEREDLIST.serialize();
        private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
        private byte serItemTypeTag;
        private ATypeTag itemTag;
        private boolean selfDescList = false;

        public AnyCollectionMemberEvalFactory(ICopyEvaluatorFactory arg) {
            this.listEvalFactory = arg;
        }

        @Override
        public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
            return new ICopyEvaluator() {

                private DataOutput out = output.getDataOutput();
                private ArrayBackedValueStorage outInputList = new ArrayBackedValueStorage();
                private ICopyEvaluator evalList = listEvalFactory.createEvaluator(outInputList);
                @SuppressWarnings("unchecked")
                private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                        .getSerializerDeserializer(BuiltinType.ANULL);
                private int itemOffset;
                private int itemLength;

                @Override
                public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                    try {
                        outInputList.reset();
                        evalList.evaluate(tuple);
                        byte[] serList = outInputList.getByteArray();

                        if (serList[0] == SER_NULL_TYPE_TAG) {
                            nullSerde.serialize(ANull.NULL, out);
                            return;
                        }

                        if (serList[0] != SER_ORDEREDLIST_TYPE_TAG && serList[0] != SER_UNORDEREDLIST_TYPE_TAG) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.ANY_COLLECTION_MEMBER.getName()
                                    + ": expects input type ORDEREDLIST/UNORDEREDLIST, but got "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[0]));
                        }

                        if (serList[0] == SER_ORDEREDLIST_TYPE_TAG) {
                            if (AOrderedListSerializerDeserializer.getNumberOfItems(serList) == 0) {
                                out.writeByte(SER_NULL_TYPE_TAG);
                                return;
                            }
                            itemOffset = AOrderedListSerializerDeserializer.getItemOffset(serList, 0);
                        } else {
                            if (AUnorderedListSerializerDeserializer.getNumberOfItems(serList) == 0) {
                                out.writeByte(SER_NULL_TYPE_TAG);
                                return;
                            }
                            itemOffset = AUnorderedListSerializerDeserializer.getItemOffset(serList, 0);
                        }

                        itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[1]);
                        if (itemTag == ATypeTag.ANY)
                            selfDescList = true;
                        else
                            serItemTypeTag = serList[1];

                        if (selfDescList) {
                            itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[itemOffset]);
                            itemLength = NonTaggedFormatUtil.getFieldValueLength(serList, itemOffset, itemTag, true) + 1;
                            out.write(serList, itemOffset, itemLength);
                        } else {
                            itemLength = NonTaggedFormatUtil.getFieldValueLength(serList, itemOffset, itemTag, false);
                            out.writeByte(serItemTypeTag);
                            out.write(serList, itemOffset, itemLength);
                        }
                    } catch (IOException e) {
                        throw new AlgebricksException(e);
                    } catch (AsterixException e) {
                        throw new AlgebricksException(e);
                    }
                }
            };
        }

    }

}
