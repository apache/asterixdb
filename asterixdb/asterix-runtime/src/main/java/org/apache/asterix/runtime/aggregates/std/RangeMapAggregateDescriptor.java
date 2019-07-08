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
package org.apache.asterix.runtime.aggregates.std;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.AMutableBinary;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

/**
 * <pre>
 * {@see {@link org.apache.hyracks.dataflow.common.data.partition.range.RangeMap}} for some description of the range map
 * structure that is produced by this function. Given a list of samples and a number of partitions "k", the algorithm
 * of this function operates as follows (s = sample):
 * It picks (k - 1) split points out of the samples by dividing num_samples/num_partitions. For 4 partitions, it's 3:
 * s0,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15; 16/4 = 4; range map = [s3, s7, s11]
 *           |           |             |
 *
 * s0,s1,s2,s3,s4,s5,s6; 7/4 = 2; range map = [s1, s3, s5]
 *     |     |     |
 *
 * s0,s1,s2,s3,s4; 5/4 = 2; range map = [s1, s3, s4]; if we go out of bound for the last split, we pick the last item.
 *     |     |  |
 *
 * s0,s1,s2,s3; if #_samples <= #_partitions, we sweep from the beginning (should be rare). range map = [s0, s1, s2]
 *  |  |  |
 *
 * s0,s1; if there are way less samples, we sweep and repeat the last item; range map = [s0, s1, s1];
 * Note: a sample (and therefore also a split point) could be single-column or multi-column.
 * </pre>
 */
public class RangeMapAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private boolean[] ascFlags;
    private int numPartitions;
    private int numOrderFields;
    private IAType[] argsTypes;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RangeMapAggregateDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_SORTING_PARAMETERS;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.RANGE_MAP;
    }

    /**
     * The sampling function, which generates the splitting vector, needs to know the number of partitions in order to
     * determine how many split points to pick out of the samples. It also needs to know the ascending/descending of
     * each sort field so that it can sort the samples accordingly first and then pick the (number of partitions - 1)
     * split points out of the sorted samples.
     * @param states states[0]: number of partitions, states[1]: ascending flags, states[2]: inputs types
     */
    @Override
    public void setImmutableStates(Object... states) {
        numPartitions = (int) states[0];
        ascFlags = (boolean[]) states[1];
        numOrderFields = ascFlags.length;
        argsTypes = (IAType[]) states[2];
    }

    @Override
    public IAggregateEvaluatorFactory createAggregateEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IAggregateEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IAggregateEvaluator createAggregateEvaluator(final IEvaluatorContext ctx)
                    throws HyracksDataException {
                return new RangeMapFunction(args, ctx, ascFlags, numPartitions, numOrderFields, sourceLoc, argsTypes);
            }
        };
    }

    private static class RangeMapFunction extends AbstractAggregateFunction {
        @SuppressWarnings("unchecked")
        private ISerializerDeserializer<ABinary> binarySerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABINARY);
        private final AMutableBinary binary = new AMutableBinary(null, 0, 0);
        private final List<List<byte[]>> finalSamples = new ArrayList<>();
        private final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        private final IPointable input = new VoidPointable();
        private final ByteArrayPointable rangeMapPointable = new ByteArrayPointable();
        private final IScalarEvaluator localSamplesEval;
        private final Comparator<List<byte[]>> comparator;
        private final int numOfPartitions;
        private final int numOrderByFields;

        @SuppressWarnings("unchecked")
        private RangeMapFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context, boolean[] ascending,
                int numOfPartitions, int numOrderByFields, SourceLocation sourceLocation, IAType[] argsTypes)
                throws HyracksDataException {
            super(sourceLocation);
            this.localSamplesEval = args[0].createScalarEvaluator(context);
            this.comparator = createComparator(ascending, argsTypes);
            this.numOfPartitions = numOfPartitions;
            this.numOrderByFields = numOrderByFields;
        }

        @Override
        public void init() throws HyracksDataException {
            finalSamples.clear();
        }

        /**
         * Receives the local samples and appends them to the final list of samples.
         * @param tuple the partition's samples
         * @throws HyracksDataException IO Exception
         */
        @Override
        public void step(IFrameTupleReference tuple) throws HyracksDataException {
            // check if empty stream (system_null), i.e. partition is empty, so no samples
            localSamplesEval.evaluate(tuple, input);
            if (input.getByteArray()[input.getStartOffset()] == ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG) {
                return;
            }
            rangeMapPointable.set(input.getByteArray(), input.getStartOffset() + 1, input.getLength() - 1);
            byte[] rangeMapBytes = rangeMapPointable.getByteArray();
            int pointer = rangeMapPointable.getContentStartOffset();
            int numSamples = IntegerPointable.getInteger(rangeMapBytes, pointer);
            pointer += Integer.BYTES; // eat the 4 bytes of the integer (number of samples)
            for (int i = 0; i < numSamples; i++) {
                List<byte[]> oneSample = new ArrayList<>(numOrderByFields);
                for (int j = 0; j < numOrderByFields; j++) {
                    int valueLength = IntegerPointable.getInteger(rangeMapBytes, pointer);
                    pointer += Integer.BYTES; // eat the 4 bytes of the integer and move to the value
                    oneSample.add(Arrays.copyOfRange(rangeMapBytes, pointer, pointer + valueLength));
                    pointer += valueLength; // eat the length of the value and move to the next pair length:value
                }
                finalSamples.add(oneSample);
            }
        }

        /**
         * Produces the range map out of the collected samples from each partition. The final list of samples is sorted
         * first. Then, we select the split points by dividing the samples evenly.
         * @param result contains the serialized range map.
         * @throws HyracksDataException IO Exception
         */
        @Override
        public void finish(IPointable result) throws HyracksDataException {
            // storage == all serialized split values of all split points
            storage.reset();
            DataOutput allSplitValuesOut = storage.getDataOutput();
            int[] endOffsets;
            try {
                // check if empty dataset, i.e. no samples have been received from any partition
                if (finalSamples.isEmpty()) {
                    // a range map with null values
                    endOffsets = new int[numOrderByFields];
                    for (int sortField = 0; sortField < numOrderByFields; sortField++) {
                        allSplitValuesOut.write(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                        endOffsets[sortField] = storage.getLength();
                    }
                } else {
                    finalSamples.sort(comparator);
                    // divide the samples evenly and pick the boundaries as split points
                    int nextSplitOffset = (int) Math.ceil(finalSamples.size() / (double) numOfPartitions);
                    int nextSplitIndex = nextSplitOffset - 1;
                    int endOffsetsCounter = 0;
                    int numRequiredSplits = numOfPartitions - 1;
                    endOffsets = new int[numRequiredSplits * numOrderByFields];
                    List<byte[]> sample;
                    for (int split = 1; split <= numRequiredSplits; split++) {
                        // pick the split point from sorted samples (could be <3> or <4,"John"> if it's multi-column)
                        sample = finalSamples.get(nextSplitIndex);
                        for (int column = 0; column < sample.size(); column++) {
                            allSplitValuesOut.write(sample.get(column));
                            endOffsets[endOffsetsCounter++] = storage.getLength();
                        }
                        // go to the next split point
                        nextSplitIndex += nextSplitOffset;
                        // in case we go beyond the boundary of samples, we pick the last sample repeatedly
                        if (nextSplitIndex >= finalSamples.size()) {
                            nextSplitIndex = finalSamples.size() - 1;
                        }
                    }
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            serializeRangeMap(numOrderByFields, storage.getByteArray(), endOffsets, result);
        }

        @Override
        public void finishPartial(IPointable result) throws HyracksDataException {
            finish(result);
        }

        /**
         * Creates the comparator that sorts all the collected samples before picking split points.
         * @param asc ascending or descending flag for each sort column.
         * @param types types of inputs to range map function produced by the local step and holding sort fields types
         * @return the described comparator
         */
        private static Comparator<List<byte[]>> createComparator(boolean[] asc, IAType[] types) {
            // create the generic comparator for each sort field, sort fields types start at index 1
            IBinaryComparator[] fieldsComparators = new IBinaryComparator[asc.length];
            for (int i = 0, fieldIdx = 1; fieldIdx < types.length; i++, fieldIdx++) {
                fieldsComparators[i] = BinaryComparatorFactoryProvider.INSTANCE
                        .getBinaryComparatorFactory(types[fieldIdx], types[fieldIdx], asc[i]).createBinaryComparator();
            }
            return (splitPoint1, splitPoint2) -> {
                try {
                    // two split points must have the same num of fields
                    int numFields = splitPoint1.size();
                    int result = 0;
                    byte[] field1;
                    byte[] field2;
                    for (int i = 0; i < numFields; i++) {
                        field1 = splitPoint1.get(i);
                        field2 = splitPoint2.get(i);
                        result = fieldsComparators[i].compare(field1, 0, field1.length, field2, 0, field2.length);
                        if (result != 0) {
                            return result;
                        }
                    }
                    return result;
                } catch (HyracksDataException e) {
                    throw new IllegalStateException(e);
                }
            };
        }

        /**
         * Serializes the range map object defined by the below attributes into the "result". The range map object is
         * serialized as binary data.
         * @param numberFields the number of order-by fields (the sort fields)
         * @param splitValues the serialized split values stored one after the other
         * @param endOffsets the end offsets of each split value
         * @param result where the range map object is serialized
         * @throws HyracksDataException IO Exception
         */
        private void serializeRangeMap(int numberFields, byte[] splitValues, int[] endOffsets, IPointable result)
                throws HyracksDataException {
            ArrayBackedValueStorage serRangeMap = new ArrayBackedValueStorage();
            IntegerSerializerDeserializer.write(numberFields, serRangeMap.getDataOutput());
            ByteArraySerializerDeserializer.write(splitValues, serRangeMap.getDataOutput());
            IntArraySerializerDeserializer.write(endOffsets, serRangeMap.getDataOutput());
            binary.setValue(serRangeMap.getByteArray(), serRangeMap.getStartOffset(), serRangeMap.getLength());
            storage.reset();
            binarySerde.serialize(binary, storage.getDataOutput());
            result.set(storage);
        }
    }
}
