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

import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.fuzzyjoin.IntArray;
import org.apache.asterix.fuzzyjoin.similarity.PartialIntersect;
import org.apache.asterix.fuzzyjoin.similarity.SimilarityFilters;
import org.apache.asterix.fuzzyjoin.similarity.SimilarityMetric;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.SimilarityFiltersCache;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SimilarityDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "similarity@7", 7);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SimilarityDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {

                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IPointable inputVal = new VoidPointable();
                    private final IScalarEvaluator evalLen1 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalTokens1 = args[1].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalLen2 = args[2].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalTokens2 = args[3].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalTokenPrefix = args[4].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalSimilarity = args[5].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalThreshold = args[6].createScalarEvaluator(ctx);

                    private final SimilarityFiltersCache similarityFiltersCache = new SimilarityFiltersCache();

                    private final IntArray tokens1 = new IntArray();
                    private final IntArray tokens2 = new IntArray();
                    private final PartialIntersect parInter = new PartialIntersect();

                    // result
                    private final AMutableDouble res = new AMutableDouble(0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ADouble> doubleSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        // similarity threshold
                        evalThreshold.evaluate(tuple, inputVal);
                        byte[] data = inputVal.getByteArray();
                        int offset = inputVal.getStartOffset();

                        if (data[offset] != ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, data[offset],
                                    ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                        }
                        float similarityThreshold = (float) ADoubleSerializerDeserializer.getDouble(data, offset + 1);

                        // similarity name
                        evalSimilarity.evaluate(tuple, inputVal);
                        data = inputVal.getByteArray();
                        offset = inputVal.getStartOffset();
                        int len = inputVal.getLength();
                        if (data[offset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, data[offset],
                                    ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                        }
                        SimilarityFilters similarityFilters =
                                similarityFiltersCache.get(similarityThreshold, data, offset, len);

                        evalLen1.evaluate(tuple, inputVal);
                        data = inputVal.getByteArray();
                        offset = inputVal.getStartOffset();
                        if (data[offset] != ATypeTag.SERIALIZED_INT32_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 2, data[offset],
                                    ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                        }
                        int length1 = IntegerPointable.getInteger(data, offset + 1);

                        evalLen2.evaluate(tuple, inputVal);
                        data = inputVal.getByteArray();
                        offset = inputVal.getStartOffset();
                        if (data[offset] != ATypeTag.SERIALIZED_INT32_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 3, data[offset],
                                    ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                        }
                        int length2 = IntegerPointable.getInteger(data, offset + 1);

                        float sim = 0;

                        //
                        // -- - length filter - --
                        //
                        if (similarityFilters.passLengthFilter(length1, length2)) {

                            // -- - tokens1 - --
                            int i;
                            tokens1.reset();
                            evalTokens1.evaluate(tuple, inputVal);
                            byte[] serList = inputVal.getByteArray();
                            offset = inputVal.getStartOffset();

                            if (serList[offset] != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG
                                    && serList[offset] != ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 4, data[offset],
                                        ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG,
                                        ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG);
                            }

                            int lengthTokens1;
                            if (serList[offset] == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                                lengthTokens1 = AOrderedListSerializerDeserializer.getNumberOfItems(serList, offset);
                                // read tokens
                                for (i = 0; i < lengthTokens1; i++) {
                                    int itemOffset =
                                            AOrderedListSerializerDeserializer.getItemOffset(serList, offset, i);
                                    tokens1.add(IntegerPointable.getInteger(serList, itemOffset));
                                }
                            } else {
                                lengthTokens1 = AUnorderedListSerializerDeserializer.getNumberOfItems(serList, offset);
                                // read tokens
                                for (i = 0; i < lengthTokens1; i++) {
                                    int itemOffset =
                                            AUnorderedListSerializerDeserializer.getItemOffset(serList, offset, i);
                                    tokens1.add(IntegerPointable.getInteger(serList, itemOffset));
                                }
                            }
                            // pad tokens
                            for (; i < length1; i++) {
                                tokens1.add(Integer.MAX_VALUE);
                            }

                            // -- - tokens2 - --
                            tokens2.reset();
                            evalTokens2.evaluate(tuple, inputVal);
                            serList = inputVal.getByteArray();
                            offset = inputVal.getStartOffset();

                            if (serList[offset] != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG
                                    && serList[offset] != ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 5, data[offset],
                                        ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG,
                                        ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG);
                            }

                            int lengthTokens2;
                            if (serList[0] == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                                lengthTokens2 = AOrderedListSerializerDeserializer.getNumberOfItems(serList, offset);
                                // read tokens
                                for (i = 0; i < lengthTokens2; i++) {
                                    int itemOffset =
                                            AOrderedListSerializerDeserializer.getItemOffset(serList, offset, i);
                                    tokens2.add(IntegerPointable.getInteger(serList, itemOffset));
                                }
                            } else {
                                lengthTokens2 = AUnorderedListSerializerDeserializer.getNumberOfItems(serList, offset);
                                // read tokens
                                for (i = 0; i < lengthTokens2; i++) {
                                    int itemOffset =
                                            AUnorderedListSerializerDeserializer.getItemOffset(serList, offset, i);
                                    tokens2.add(IntegerPointable.getInteger(serList, itemOffset));
                                }
                            }
                            // pad tokens
                            for (; i < length2; i++) {
                                tokens2.add(Integer.MAX_VALUE);
                            }

                            // -- - token prefix - --
                            evalTokenPrefix.evaluate(tuple, inputVal);
                            int tokenPrefix =
                                    IntegerPointable.getInteger(inputVal.getByteArray(), inputVal.getStartOffset() + 1);

                            //
                            // -- - position filter - --
                            //
                            SimilarityMetric.getPartialIntersectSize(tokens1.get(), 0, tokens1.length(), tokens2.get(),
                                    0, tokens2.length(), tokenPrefix, parInter);
                            if (similarityFilters.passPositionFilter(parInter.intersectSize, parInter.posXStop, length1,
                                    parInter.posYStop, length2)) {

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
                            throw HyracksDataException.create(e);
                        }
                        result.set(resultStorage);
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
