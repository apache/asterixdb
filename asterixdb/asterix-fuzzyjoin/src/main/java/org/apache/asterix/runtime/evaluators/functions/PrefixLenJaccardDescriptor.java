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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.fuzzyjoin.similarity.SimilarityFiltersJaccard;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class PrefixLenJaccardDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new PrefixLenJaccardDescriptor();
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
                    private final IPointable lenPtr = new VoidPointable();
                    private final IPointable thresholdPtr = new VoidPointable();
                    private final IScalarEvaluator evalLen = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalThreshold = args[1].createScalarEvaluator(ctx);

                    private float similarityThresholdCache;
                    private SimilarityFiltersJaccard similarityFilters;

                    // result
                    private final AMutableInt32 res = new AMutableInt32(0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInt32> int32Serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        evalLen.evaluate(tuple, lenPtr);
                        evalThreshold.evaluate(tuple, thresholdPtr);

                        if (PointableHelper.checkAndSetMissingOrNull(result, lenPtr, thresholdPtr)) {
                            return;
                        }

                        // length
                        int length = ATypeHierarchy.getIntegerValue(getIdentifier().getName(), 0, lenPtr.getByteArray(),
                                lenPtr.getStartOffset());
                        // similarity threshold
                        byte[] data = thresholdPtr.getByteArray();
                        int offset = thresholdPtr.getStartOffset();
                        if (data[offset] != ATypeTag.SERIALIZED_FLOAT_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, data[offset],
                                    ATypeTag.SERIALIZED_FLOAT_TYPE_TAG);
                        }
                        float similarityThreshold = AFloatSerializerDeserializer.getFloat(data, offset + 1);

                        if (similarityThreshold != similarityThresholdCache || similarityFilters == null) {
                            similarityFilters = new SimilarityFiltersJaccard(similarityThreshold);
                        }

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
        return BuiltinFunctions.PREFIX_LEN_JACCARD;
    }

}
