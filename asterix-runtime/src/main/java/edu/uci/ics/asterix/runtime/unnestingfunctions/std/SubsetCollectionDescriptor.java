package edu.uci.ics.asterix.runtime.unnestingfunctions.std;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.runtime.unnestingfunctions.base.AbstractUnnestingFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class SubsetCollectionDescriptor extends AbstractUnnestingFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "subset-collection", 3, true);

    private final static byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
    private final static byte SER_UNORDEREDLIST_TYPE_TAG = ATypeTag.UNORDEREDLIST.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SubsetCollectionDescriptor();
        }
    };

    @Override
    public IUnnestingFunctionFactory createUnnestingFunctionFactory(final IEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IUnnestingFunctionFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IUnnestingFunction createUnnestingFunction(IDataOutputProvider provider) throws AlgebricksException {

                final DataOutput out = provider.getDataOutput();

                return new IUnnestingFunction() {
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private IEvaluator evalList = args[0].createEvaluator(inputVal);
                    private IEvaluator evalStart = args[1].createEvaluator(inputVal);
                    private IEvaluator evalLen = args[2].createEvaluator(inputVal);
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
                            posStart = IntegerSerializerDeserializer.getInt(inputVal.getBytes(), 1);

                            inputVal.reset();
                            evalLen.evaluate(tuple);
                            numItems = IntegerSerializerDeserializer.getInt(inputVal.getBytes(), 1);

                            inputVal.reset();
                            evalList.evaluate(tuple);

                            byte[] serList = inputVal.getBytes();

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
                            byte[] serList = inputVal.getBytes();
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
        return FID;
    }

}
