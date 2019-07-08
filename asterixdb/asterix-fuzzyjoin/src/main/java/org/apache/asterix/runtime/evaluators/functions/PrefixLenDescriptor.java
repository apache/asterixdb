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
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.fuzzyjoin.similarity.SimilarityFilters;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
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

public class PrefixLenDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "prefix-len@3", 3);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new PrefixLenDescriptor();
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
                    private final IScalarEvaluator evalLen = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalSimilarity = args[1].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalThreshold = args[2].createScalarEvaluator(ctx);

                    private final SimilarityFiltersCache similarityFiltersCache = new SimilarityFiltersCache();

                    // result
                    private final AMutableInt32 res = new AMutableInt32(0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInt32> int32Serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        // length
                        evalLen.evaluate(tuple, inputVal);
                        byte[] data = inputVal.getByteArray();
                        int offset = inputVal.getStartOffset();
                        if (data[offset] != ATypeTag.SERIALIZED_INT32_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, data[offset],
                                    ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                        }
                        int length = IntegerPointable.getInteger(data, offset + 1);

                        // similarity threshold
                        evalThreshold.evaluate(tuple, inputVal);
                        data = inputVal.getByteArray();
                        offset = inputVal.getStartOffset();
                        if (data[offset] != ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, data[offset],
                                    ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                        }
                        float similarityThreshold = (float) ADoubleSerializerDeserializer.getDouble(data, offset + 1);

                        // similarity name
                        evalSimilarity.evaluate(tuple, inputVal);
                        data = inputVal.getByteArray();
                        offset = inputVal.getStartOffset();
                        int len = inputVal.getLength();
                        if (data[offset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 2, data[offset],
                                    ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                        }
                        SimilarityFilters similarityFilters =
                                similarityFiltersCache.get(similarityThreshold, data, offset, len);

                        int prefixLength = similarityFilters.getPrefixLength(length);
                        res.setValue(prefixLength);

                        try {
                            int32Serde.serialize(res, out);
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
