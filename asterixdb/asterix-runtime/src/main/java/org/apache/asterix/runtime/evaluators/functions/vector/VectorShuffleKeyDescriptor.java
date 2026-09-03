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

package org.apache.asterix.runtime.evaluators.functions.vector;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.operators.kmeans.KMeansLoopIO;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * {@code vector-shuffle-key(vector, seed)} -> AINT64: a hash of every component of the vector mixed with the
 * seed; NULL for a non-list, an empty list or a non-numeric component. CLUSTER BY orders the rows on it to draw
 * its starting centres.
 * <p>
 * The key is a pure function of the vector and the seed: equal vectors key equally on any partition or position,
 * and distinct vectors key distinctly, so {@code ORDER BY key LIMIT n} has a winner fixed by the seed and the data
 * alone. The previous key, {@code random(vector[0] + seed)}, looked at one component only, so distinct vectors
 * sharing it tied and the merge broke the tie by partition index; and {@code random()} is a stream that reseeds
 * only when its argument changes, so a row's key also depended on the rows read before it.
 * <p>
 * The hash is {@link KMeansLoopIO#fingerprint}, which the oversampling rounds already draw with.
 */
public class VectorShuffleKeyDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = VectorShuffleKeyDescriptor::new;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.VECTOR_SHUFFLE_KEY;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput dataOutput = resultStorage.getDataOutput();
                    private final IScalarEvaluator vectorEval = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator seedEval = args[1].createScalarEvaluator(ctx);
                    private final IPointable vectorVal = new VoidPointable();
                    private final IPointable seedVal = new VoidPointable();
                    private final ListAccessor list = new ListAccessor();
                    private final VectorListDecoder decoder = new VectorListDecoder();
                    // Sized to the vector exactly: fingerprint hashes the whole array, length included.
                    private double[] components = new double[0];
                    private final AMutableInt64 aInt64 = new AMutableInt64(0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInt64> int64Serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        vectorEval.evaluate(tuple, vectorVal);
                        seedEval.evaluate(tuple, seedVal);
                        if (PointableHelper.checkAndSetMissingOrNull(result, vectorVal, seedVal)) {
                            return;
                        }
                        if (!decoder.checkListType(vectorVal)) {
                            PointableHelper.setNull(result);
                            return;
                        }
                        list.reset(vectorVal.getByteArray(), vectorVal.getStartOffset());
                        int dimension = list.size();
                        if (dimension == 0) {
                            PointableHelper.setNull(result);
                            return;
                        }
                        if (components.length != dimension) {
                            components = new double[dimension];
                        }
                        try {
                            decoder.createArrayFromList(list, components);
                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                        for (double component : components) {
                            if (Double.isNaN(component)) {
                                PointableHelper.setNull(result);
                                return;
                            }
                        }
                        long seed = ATypeHierarchy.getLongValue(getIdentifier().getName(), 1, seedVal.getByteArray(),
                                seedVal.getStartOffset());
                        long key = KMeansLoopIO.mix64(KMeansLoopIO.fingerprint(components) ^ KMeansLoopIO.mix64(seed));
                        resultStorage.reset();
                        aInt64.setValue(key);
                        int64Serde.serialize(aInt64, dataOutput);
                        result.set(resultStorage);
                    }
                };
            }
        };
    }
}
