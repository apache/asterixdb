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
package org.apache.asterix.runtime.evaluators.functions.temporal;

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.AMutableInterval;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class GetOverlappingIntervalDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new GetOverlappingIntervalDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws AlgebricksException {
                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable argPtr0 = new VoidPointable();
                    private IPointable argPtr1 = new VoidPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    private final AMutableInterval aInterval = new AMutableInterval(0, 0, (byte) -1);

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInterval> intervalSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINTERVAL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, argPtr0);
                        eval1.evaluate(tuple, argPtr1);

                        byte[] bytes0 = argPtr0.getByteArray();
                        int offset0 = argPtr0.getStartOffset();
                        byte[] bytes1 = argPtr1.getByteArray();
                        int offset1 = argPtr1.getStartOffset();

                        try {
                            if (bytes0[offset0] == ATypeTag.SERIALIZED_NULL_TYPE_TAG
                                    || bytes1[offset1] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                            } else if (bytes0[offset0] == ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG
                                    && bytes0[offset0] == bytes1[offset1]) {
                                byte type0 = AIntervalSerializerDeserializer.getIntervalTimeType(bytes0, offset0 + 1);
                                byte type1 = AIntervalSerializerDeserializer.getIntervalTimeType(bytes1, offset1 + 1);
                                if (type0 != type1) {
                                    throw new AlgebricksException(getIdentifier().getName()
                                            + ": expecting two (nullable) interval values with the same internal time type but got interval of "
                                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(type0)
                                            + " and interval of "
                                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(type1));
                                }

                                long start0 = AIntervalSerializerDeserializer.getIntervalStart(bytes0, offset0 + 1);
                                long end0 = AIntervalSerializerDeserializer.getIntervalEnd(bytes0, offset0 + 1);

                                long start1 = AIntervalSerializerDeserializer.getIntervalStart(bytes1, offset1 + 1);
                                long end1 = AIntervalSerializerDeserializer.getIntervalEnd(bytes1, offset1 + 1);

                                if (IntervalLogic.overlap(start0, end0, start1, end1)
                                        || IntervalLogic.overlappedBy(start0, end0, start1, end1)
                                        || IntervalLogic.covers(start0, end0, start1, end1)
                                        || IntervalLogic.coveredBy(start0, end0, start1, end1)) {
                                    aInterval.setValue(Math.max(start0, start1), Math.min(end0, end1), type0);
                                    intervalSerde.serialize(aInterval, out);
                                } else {
                                    nullSerde.serialize(ANull.NULL, out);
                                }
                            } else {
                                throw new AlgebricksException(getIdentifier().getName()
                                        + ": expecting two (nullable) interval values but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]) + " and "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]));
                            }
                        } catch (HyracksDataException hex) {
                            throw new AlgebricksException(hex);
                        }
                        result.set(resultStorage);
                    }

                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.GET_OVERLAPPING_INTERVAL;
    }
}
