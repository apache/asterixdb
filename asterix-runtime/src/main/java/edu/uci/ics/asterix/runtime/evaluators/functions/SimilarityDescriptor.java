package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.SimilarityFiltersCache;
import edu.uci.ics.fuzzyjoin.IntArray;
import edu.uci.ics.fuzzyjoin.similarity.PartialIntersect;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFilters;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityMetric;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class SimilarityDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "similarity", 7,
            true);

    private final static byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
    private final static byte SER_UNORDEREDLIST_TYPE_TAG = ATypeTag.UNORDEREDLIST.serialize();
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SimilarityDescriptor();
        }
    };

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) throws AlgebricksException {

        return new IEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {

                return new IEvaluator() {

                    private final DataOutput out = output.getDataOutput();
                    private final ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private final IEvaluator evalLen1 = args[0].createEvaluator(inputVal);
                    private final IEvaluator evalTokens1 = args[1].createEvaluator(inputVal);
                    private final IEvaluator evalLen2 = args[2].createEvaluator(inputVal);
                    private final IEvaluator evalTokens2 = args[3].createEvaluator(inputVal);
                    private final IEvaluator evalTokenPrefix = args[4].createEvaluator(inputVal);
                    private final IEvaluator evalSimilarity = args[5].createEvaluator(inputVal);
                    private final IEvaluator evalThreshold = args[6].createEvaluator(inputVal);

                    private final SimilarityFiltersCache similarityFiltersCache = new SimilarityFiltersCache();

                    private final IntArray tokens1 = new IntArray();
                    private final IntArray tokens2 = new IntArray();
                    private final PartialIntersect parInter = new PartialIntersect();

                    // result
                    private final AMutableDouble res = new AMutableDouble(0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ADouble> doubleSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADOUBLE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        // similarity threshold
                        inputVal.reset();
                        evalThreshold.evaluate(tuple);
                        float similarityThreshold = (float) ADoubleSerializerDeserializer.getDouble(
                                inputVal.getBytes(), 1);

                        // similarity name
                        inputVal.reset();
                        evalSimilarity.evaluate(tuple);
                        SimilarityFilters similarityFilters = similarityFiltersCache.get(similarityThreshold,
                                inputVal.getBytes());

                        inputVal.reset();
                        evalLen1.evaluate(tuple);
                        int length1 = IntegerSerializerDeserializer.getInt(inputVal.getBytes(), 1);

                        inputVal.reset();
                        evalLen2.evaluate(tuple);
                        int length2 = IntegerSerializerDeserializer.getInt(inputVal.getBytes(), 1);

                        float sim = 0;

                        //
                        // -- - length filter - --
                        //
                        if (similarityFilters.passLengthFilter(length1, length2)) {

                            // -- - tokens1 - --
                            int i;
                            tokens1.reset();
                            inputVal.reset();
                            evalTokens1.evaluate(tuple);

                            byte[] serList = inputVal.getBytes();
                            if (serList[0] != SER_ORDEREDLIST_TYPE_TAG && serList[0] != SER_UNORDEREDLIST_TYPE_TAG) {
                                throw new AlgebricksException("Scan collection is not defined for values of type"
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[0]));
                            }

                            int lengthTokens1;
                            if (serList[0] == SER_ORDEREDLIST_TYPE_TAG) {
                                lengthTokens1 = AOrderedListSerializerDeserializer
                                        .getNumberOfItems(inputVal.getBytes());
                                // read tokens
                                for (i = 0; i < lengthTokens1; i++) {
                                    int itemOffset;
                                    try {
                                        itemOffset = AOrderedListSerializerDeserializer.getItemOffset(serList, i);
                                    } catch (AsterixException e) {
                                        throw new AlgebricksException(e);
                                    }
                                    tokens1.add(IntegerSerializerDeserializer.getInt(serList, itemOffset));
                                }
                            } else {
                                lengthTokens1 = AUnorderedListSerializerDeserializer.getNumberOfItems(inputVal
                                        .getBytes());
                                // read tokens
                                for (i = 0; i < lengthTokens1; i++) {
                                    int itemOffset;
                                    try {
                                        itemOffset = AUnorderedListSerializerDeserializer.getItemOffset(serList, i);
                                    } catch (AsterixException e) {
                                        throw new AlgebricksException(e);
                                    }
                                    tokens1.add(IntegerSerializerDeserializer.getInt(serList, itemOffset));
                                }
                            }
                            // pad tokens
                            for (; i < length1; i++) {
                                tokens1.add(Integer.MAX_VALUE);
                            }

                            // -- - tokens2 - --
                            tokens2.reset();
                            inputVal.reset();
                            evalTokens2.evaluate(tuple);

                            serList = inputVal.getBytes();
                            if (serList[0] != SER_ORDEREDLIST_TYPE_TAG && serList[0] != SER_UNORDEREDLIST_TYPE_TAG) {
                                throw new AlgebricksException("Scan collection is not defined for values of type"
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[0]));
                            }

                            int lengthTokens2;
                            if (serList[0] == SER_ORDEREDLIST_TYPE_TAG) {
                                lengthTokens2 = AOrderedListSerializerDeserializer
                                        .getNumberOfItems(inputVal.getBytes());
                                // read tokens
                                for (i = 0; i < lengthTokens2; i++) {
                                    int itemOffset;
                                    try {
                                        itemOffset = AOrderedListSerializerDeserializer.getItemOffset(serList, i);
                                    } catch (AsterixException e) {
                                        throw new AlgebricksException(e);
                                    }
                                    tokens2.add(IntegerSerializerDeserializer.getInt(serList, itemOffset));
                                }
                            } else {
                                lengthTokens2 = AUnorderedListSerializerDeserializer.getNumberOfItems(inputVal
                                        .getBytes());
                                // read tokens
                                for (i = 0; i < lengthTokens2; i++) {
                                    int itemOffset;
                                    try {
                                        itemOffset = AUnorderedListSerializerDeserializer.getItemOffset(serList, i);
                                    } catch (AsterixException e) {
                                        throw new AlgebricksException(e);
                                    }
                                    tokens2.add(IntegerSerializerDeserializer.getInt(serList, itemOffset));
                                }
                            }
                            // pad tokens
                            for (; i < length2; i++) {
                                tokens2.add(Integer.MAX_VALUE);
                            }

                            // -- - token prefix - --
                            inputVal.reset();
                            evalTokenPrefix.evaluate(tuple);
                            int tokenPrefix = IntegerSerializerDeserializer.getInt(inputVal.getBytes(), 1);

                            //
                            // -- - position filter - --
                            //
                            SimilarityMetric.getPartialIntersectSize(tokens1.get(), 0, tokens1.length(), tokens2.get(),
                                    0, tokens2.length(), tokenPrefix, parInter);
                            if (similarityFilters.passPositionFilter(parInter.intersectSize, parInter.posXStop,
                                    length1, parInter.posYStop, length2)) {

                                //
                                // -- - suffix filter - --
                                //
                                if (similarityFilters.passSuffixFilter(tokens1.get(), 0, tokens1.length(),
                                        parInter.posXStart, tokens2.get(), 0, tokens2.length(), parInter.posYStart)) {

                                    sim = similarityFilters.passSimilarityFilter(tokens1.get(), 0, tokens1.length(),
                                            parInter.posXStop + 1, tokens2.get(), 0, tokens2.length(),
                                            parInter.posYStop + 1, parInter.intersectSize);
                                }
                            }
                        }

                        res.setValue(sim);

                        try {
                            doubleSerde.serialize(res, out);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
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
