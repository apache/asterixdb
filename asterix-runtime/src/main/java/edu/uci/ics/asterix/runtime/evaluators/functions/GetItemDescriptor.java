/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class GetItemDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new GetItemDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new GetItemEvalFactory(args);
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.GET_ITEM;
    }

    private static class GetItemEvalFactory implements ICopyEvaluatorFactory {

        private static final long serialVersionUID = 1L;

        private ICopyEvaluatorFactory listEvalFactory;
        private ICopyEvaluatorFactory indexEvalFactory;
        private final static byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
        private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
        private final static byte SER_INT32_TYPE_TAG = ATypeTag.INT32.serialize();
        private byte serItemTypeTag;
        private ATypeTag itemTag;
        private boolean selfDescList = false;

        public GetItemEvalFactory(ICopyEvaluatorFactory[] args) {
            this.listEvalFactory = args[0];
            this.indexEvalFactory = args[1];
        }

        @Override
        public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
            return new ICopyEvaluator() {

                private DataOutput out = output.getDataOutput();
                private ArrayBackedValueStorage outInputList = new ArrayBackedValueStorage();
                private ArrayBackedValueStorage outInputIdx = new ArrayBackedValueStorage();
                private ICopyEvaluator evalList = listEvalFactory.createEvaluator(outInputList);
                private ICopyEvaluator evalIdx = indexEvalFactory.createEvaluator(outInputIdx);
                @SuppressWarnings("unchecked")
                private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                        .getSerializerDeserializer(BuiltinType.ANULL);
                private int itemIndex;
                private int itemOffset;
                private int itemLength;

                @Override
                public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                    try {
                        outInputList.reset();
                        evalList.evaluate(tuple);
                        outInputIdx.reset();
                        evalIdx.evaluate(tuple);
                        byte[] serOrderedList = outInputList.getByteArray();

                        if (serOrderedList[0] == SER_NULL_TYPE_TAG) {
                            nullSerde.serialize(ANull.NULL, out);
                            return;
                        }

                        if (serOrderedList[0] != SER_ORDEREDLIST_TYPE_TAG
                                || outInputIdx.getByteArray()[0] != SER_INT32_TYPE_TAG) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.GET_ITEM.getName()
                                    + ": expects input type (NULL/ORDEREDLIST, INT32), but got ("
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serOrderedList[0]) + ", "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInputIdx.getByteArray()[0])
                                    + ").");
                        }

                        itemIndex = IntegerSerializerDeserializer.getInt(outInputIdx.getByteArray(), 1);
                        if (itemIndex >= AOrderedListSerializerDeserializer.getNumberOfItems(serOrderedList)) {
                            out.writeByte(SER_NULL_TYPE_TAG);
                            return;
                        }
                        if (itemIndex < 0)
                            throw new AlgebricksException(AsterixBuiltinFunctions.GET_ITEM.getName()
                                    + ": item index cannot be negative!");

                        itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serOrderedList[1]);
                        if (itemTag == ATypeTag.ANY)
                            selfDescList = true;
                        else
                            serItemTypeTag = serOrderedList[1];

                        itemOffset = AOrderedListSerializerDeserializer.getItemOffset(serOrderedList, itemIndex);

                        if (selfDescList) {
                            itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serOrderedList[itemOffset]);
                            itemLength = NonTaggedFormatUtil.getFieldValueLength(serOrderedList, itemOffset, itemTag,
                                    true) + 1;
                            out.write(serOrderedList, itemOffset, itemLength);
                        } else {
                            itemLength = NonTaggedFormatUtil.getFieldValueLength(serOrderedList, itemOffset, itemTag,
                                    false);
                            out.writeByte(serItemTypeTag);
                            out.write(serOrderedList, itemOffset, itemLength);
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
