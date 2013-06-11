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
package edu.uci.ics.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.fuzzyjoin.IntArray;
import edu.uci.ics.fuzzyjoin.similarity.PartialIntersect;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersJaccard;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityMetric;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class SimilarityJaccardPrefixEvaluator implements ICopyEvaluator {
    // assuming type indicator in serde format
    protected final int typeIndicatorSize = 1;

    protected final DataOutput out;
    protected final ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
    protected final ICopyEvaluator evalLen1;
    protected final ICopyEvaluator evalTokens1;
    protected final ICopyEvaluator evalLen2;
    protected final ICopyEvaluator evalTokens2;
    protected final ICopyEvaluator evalTokenPrefix;
    protected final ICopyEvaluator evalThreshold;

    protected float similarityThresholdCache;
    protected SimilarityFiltersJaccard similarityFilters;

    private final static byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
    private final static byte SER_UNORDEREDLIST_TYPE_TAG = ATypeTag.UNORDEREDLIST.serialize();

    protected final IntArray tokens1 = new IntArray();
    protected final IntArray tokens2 = new IntArray();
    protected final PartialIntersect parInter = new PartialIntersect();

    protected float sim = 0.0f;

    // result
    protected final AMutableFloat res = new AMutableFloat(0);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AFloat> reusSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AFLOAT);

    public SimilarityJaccardPrefixEvaluator(ICopyEvaluatorFactory[] args, IDataOutputProvider output)
            throws AlgebricksException {
        out = output.getDataOutput();
        evalLen1 = args[0].createEvaluator(inputVal);
        evalTokens1 = args[1].createEvaluator(inputVal);
        evalLen2 = args[2].createEvaluator(inputVal);
        evalTokens2 = args[3].createEvaluator(inputVal);
        evalTokenPrefix = args[4].createEvaluator(inputVal);
        evalThreshold = args[5].createEvaluator(inputVal);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        // similarity threshold
        sim = 0;
        inputVal.reset();
        evalThreshold.evaluate(tuple);
        float similarityThreshold = AFloatSerializerDeserializer.getFloat(inputVal.getByteArray(), 1);

        if (similarityThreshold != similarityThresholdCache || similarityFilters == null) {
            similarityFilters = new SimilarityFiltersJaccard(similarityThreshold);
            similarityThresholdCache = similarityThreshold;
        }

        inputVal.reset();
        evalLen1.evaluate(tuple);
        int length1 = IntegerSerializerDeserializer.getInt(inputVal.getByteArray(), 1);

        inputVal.reset();
        evalLen2.evaluate(tuple);
        int length2 = IntegerSerializerDeserializer.getInt(inputVal.getByteArray(), 1);

        //
        // -- - length filter - --
        //
        if (similarityFilters.passLengthFilter(length1, length2)) {

            // -- - tokens1 - --
            int i;
            tokens1.reset();
            inputVal.reset();
            evalTokens1.evaluate(tuple);

            byte[] serList = inputVal.getByteArray();
            if (serList[0] != SER_ORDEREDLIST_TYPE_TAG && serList[0] != SER_UNORDEREDLIST_TYPE_TAG) {
                throw new AlgebricksException("Scan collection is not defined for values of type"
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[0]));
            }

            int lengthTokens1;
            if (serList[0] == SER_ORDEREDLIST_TYPE_TAG) {
                lengthTokens1 = AOrderedListSerializerDeserializer.getNumberOfItems(inputVal.getByteArray());
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
                lengthTokens1 = AUnorderedListSerializerDeserializer.getNumberOfItems(inputVal.getByteArray());
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

            serList = inputVal.getByteArray();
            if (serList[0] != SER_ORDEREDLIST_TYPE_TAG && serList[0] != SER_UNORDEREDLIST_TYPE_TAG) {
                throw new AlgebricksException("Scan collection is not defined for values of type"
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[0]));
            }

            int lengthTokens2;
            if (serList[0] == SER_ORDEREDLIST_TYPE_TAG) {
                lengthTokens2 = AOrderedListSerializerDeserializer.getNumberOfItems(inputVal.getByteArray());
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
                lengthTokens2 = AUnorderedListSerializerDeserializer.getNumberOfItems(inputVal.getByteArray());
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
            int tokenPrefix = IntegerSerializerDeserializer.getInt(inputVal.getByteArray(), 1);

            //
            // -- - position filter - --
            //
            SimilarityMetric.getPartialIntersectSize(tokens1.get(), 0, tokens1.length(), tokens2.get(), 0,
                    tokens2.length(), tokenPrefix, parInter);
            if (similarityFilters.passPositionFilter(parInter.intersectSize, parInter.posXStop, length1,
                    parInter.posYStop, length2)) {

                //
                // -- - suffix filter - --
                //
                if (similarityFilters.passSuffixFilter(tokens1.get(), 0, tokens1.length(), parInter.posXStart,
                        tokens2.get(), 0, tokens2.length(), parInter.posYStart)) {

                    sim = similarityFilters.passSimilarityFilter(tokens1.get(), 0, tokens1.length(),
                            parInter.posXStop + 1, tokens2.get(), 0, tokens2.length(), parInter.posYStop + 1,
                            parInter.intersectSize);
                }
            }
        }

        try {
            writeResult();
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    public void writeResult() throws AlgebricksException, IOException {
        res.setValue(sim);
        reusSerde.serialize(res, out);
    }
}