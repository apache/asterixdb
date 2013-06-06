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
package edu.uci.ics.asterix.runtime.unnestingfunctions.std;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.runtime.unnestingfunctions.base.AbstractUnnestingFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class SubsetCollectionDescriptor extends AbstractUnnestingFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private final static byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
    private final static byte SER_UNORDEREDLIST_TYPE_TAG = ATypeTag.UNORDEREDLIST.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SubsetCollectionDescriptor();
        }
    };

    @Override
    public ICopyUnnestingFunctionFactory createUnnestingFunctionFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyUnnestingFunctionFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyUnnestingFunction createUnnestingFunction(IDataOutputProvider provider)
                    throws AlgebricksException {

                final DataOutput out = provider.getDataOutput();

                return new ICopyUnnestingFunction() {
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private ICopyEvaluator evalList = args[0].createEvaluator(inputVal);
                    private ICopyEvaluator evalStart = args[1].createEvaluator(inputVal);
                    private ICopyEvaluator evalLen = args[2].createEvaluator(inputVal);
                    private int numItems;
                    private int numItemsMax;
                    private int posStart;
                    private int posCrt;
                    private ATypeTag itemTag;
                    private boolean selfDescList = false;

                    @Override
                    public void init(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            inputVal.reset();
                            evalStart.evaluate(tuple);
                            posStart = IntegerSerializerDeserializer.getInt(inputVal.getByteArray(), 1);

                            inputVal.reset();
                            evalLen.evaluate(tuple);
                            numItems = IntegerSerializerDeserializer.getInt(inputVal.getByteArray(), 1);

                            inputVal.reset();
                            evalList.evaluate(tuple);

                            byte[] serList = inputVal.getByteArray();

                            if (serList[0] == SER_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                                return;
                            }

                            if (serList[0] != SER_ORDEREDLIST_TYPE_TAG && serList[0] != SER_UNORDEREDLIST_TYPE_TAG) {
                                throw new AlgebricksException("Subset-collection is not defined for values of type"
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[0]));
                            }
                            if (serList[0] == SER_ORDEREDLIST_TYPE_TAG)
                                numItemsMax = AOrderedListSerializerDeserializer.getNumberOfItems(serList);
                            else
                                numItemsMax = AUnorderedListSerializerDeserializer.getNumberOfItems(serList);

                            itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[1]);
                            if (itemTag == ATypeTag.ANY)
                                selfDescList = true;

                            posCrt = posStart;
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                    @Override
                    public boolean step() throws AlgebricksException {
                        if (posCrt < posStart + numItems && posCrt < numItemsMax) {
                            byte[] serList = inputVal.getByteArray();
                            int itemLength = 0;
                            try {
                                int itemOffset = AOrderedListSerializerDeserializer.getItemOffset(serList, posCrt);
                                if (selfDescList)
                                    itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[itemOffset]);
                                itemLength = NonTaggedFormatUtil.getFieldValueLength(serList, itemOffset, itemTag,
                                        selfDescList);
                                if (!selfDescList)
                                    out.writeByte(itemTag.serialize());
                                out.write(serList, itemOffset, itemLength + (!selfDescList ? 0 : 1));
                            } catch (IOException e) {
                                throw new AlgebricksException(e);
                            } catch (AsterixException e) {
                                throw new AlgebricksException(e);
                            }
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
        return AsterixBuiltinFunctions.SUBSET_COLLECTION;
    }

}
