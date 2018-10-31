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

import org.apache.asterix.dataflow.data.nontagged.comparators.AObjectAscBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AObjectDescBinaryComparatorFactory;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
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
    private boolean[] ascendingFlags;
    private int numOfPartitions;
    private int numOrderFields;

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
     * @param states states[0]: number of partitions, states[1]: ascending flags
     */
    @Override
    public void setImmutableStates(Object... states) {
        numOfPartitions = (int) states[0];
        ascendingFlags = (boolean[]) states[1];
        numOrderFields = ascendingFlags.length;
    }

    @Override
    public IAggregateEvaluatorFactory createAggregateEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IAggregateEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IAggregateEvaluator createAggregateEvaluator(final IHyracksTaskContext ctx)
                    throws HyracksDataException {
                return new RangeMapFunction(args, ctx, ascendingFlags, numOfPartitions, numOrderFields);
            }
        };
    }

    private class RangeMapFunction implements IAggregateEvaluator {
        private final IScalarEvaluator localSamplesEval;
        private final IPointable localSamples;
        private final List<List<byte[]>> finalSamples;
        private final Comparator<List<byte[]>> comparator;
        private final int numOfPartitions;
        private final int numOrderByFields;
        private final ListAccessor listOfSamples;
        private final ListAccessor oneSample;
        private final IPointable oneSamplePointable;
        private final ArrayBackedValueStorage oneSampleStorage;
        private final IPointable field;
        private final ArrayBackedValueStorage storage;

        @SuppressWarnings("unchecked")
        private RangeMapFunction(IScalarEvaluatorFactory[] args, IHyracksTaskContext context, boolean[] ascending,
                int numOfPartitions, int numOrderByFields) throws HyracksDataException {
            localSamples = new VoidPointable();
            localSamplesEval = args[0].createScalarEvaluator(context);
            finalSamples = new ArrayList<>();
            comparator = createComparator(ascending);
            this.numOfPartitions = numOfPartitions;
            this.numOrderByFields = numOrderByFields;
            listOfSamples = new ListAccessor();
            oneSample = new ListAccessor();
            oneSamplePointable = new VoidPointable();
            oneSampleStorage = new ArrayBackedValueStorage();
            field = new VoidPointable();
            storage = new ArrayBackedValueStorage();
        }

        @Override
        public void init() throws HyracksDataException {
            finalSamples.clear();
        }

        /**
         * Receives the local samples and appends them to the final list of samples.
         * @param tuple the partition's samples
         * @throws HyracksDataException
         */
        @Override
        public void step(IFrameTupleReference tuple) throws HyracksDataException {
            // check if empty stream (system_null), i.e. partition is empty, so no samples
            localSamplesEval.evaluate(tuple, localSamples);
            byte tag = localSamples.getByteArray()[localSamples.getStartOffset()];
            if (tag == ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG) {
                return;
            }
            // deserialize the samples received from the local partition
            listOfSamples.reset(localSamples.getByteArray(), localSamples.getStartOffset());
            int numberOfSamples = listOfSamples.size();

            // "sample" & "addedSample" are lists to support multi-column instead of one value, i.e. <3,"dept">
            List<byte[]> addedSample;
            int numberOfFields;
            // add the samples to the final samples
            try {
                for (int i = 0; i < numberOfSamples; i++) {
                    oneSampleStorage.reset();
                    listOfSamples.getOrWriteItem(i, oneSamplePointable, oneSampleStorage);
                    oneSample.reset(oneSamplePointable.getByteArray(), oneSamplePointable.getStartOffset());
                    numberOfFields = oneSample.size();
                    addedSample = new ArrayList<>(numberOfFields);
                    for (int j = 0; j < numberOfFields; j++) {
                        storage.reset();
                        oneSample.getOrWriteItem(j, field, storage);
                        addedSample.add(Arrays.copyOfRange(field.getByteArray(), field.getStartOffset(),
                                field.getStartOffset() + field.getLength()));
                    }
                    finalSamples.add(addedSample);
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Produces the range map out of the collected samples from each partition. The final list of samples is sorted
         * first. Then, we select the split points by dividing the samples evenly.
         * @param result contains the serialized range map.
         * @throws HyracksDataException
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

            serializeRangemap(numOrderByFields, storage.getByteArray(), endOffsets, result);
        }

        @Override
        public void finishPartial(IPointable result) throws HyracksDataException {
            finish(result);
        }

        /**
         * Creates the comparator that sorts all the collected samples before picking split points.
         * @param ascending ascending or descending flag for each sort column.
         * @return the described comparator
         */
        private Comparator<List<byte[]>> createComparator(boolean[] ascending) {
            // create the generic comparator for each sort field
            IBinaryComparator[] fieldsComparators = new IBinaryComparator[ascending.length];
            for (int i = 0; i < ascending.length; i++) {
                if (ascending[i]) {
                    fieldsComparators[i] = AObjectAscBinaryComparatorFactory.INSTANCE.createBinaryComparator();
                } else {
                    fieldsComparators[i] = AObjectDescBinaryComparatorFactory.INSTANCE.createBinaryComparator();
                }
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
         * @throws HyracksDataException
         */
        private void serializeRangemap(int numberFields, byte[] splitValues, int[] endOffsets, IPointable result)
                throws HyracksDataException {
            ArrayBackedValueStorage serRangeMap = new ArrayBackedValueStorage();
            IntegerSerializerDeserializer.write(numberFields, serRangeMap.getDataOutput());
            ByteArraySerializerDeserializer.write(splitValues, serRangeMap.getDataOutput());
            IntArraySerializerDeserializer.write(endOffsets, serRangeMap.getDataOutput());

            result.set(serRangeMap.getByteArray(), serRangeMap.getStartOffset(), serRangeMap.getLength());
        }
    }
}
