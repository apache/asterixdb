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

package org.apache.hyracks.tests.unit;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryRangeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRangeMap;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITupleRangePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITupleRangePartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.dataflow.value.IRangePartitionType.RangePartitioningType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.storage.IGrowableIntArray;
import org.apache.hyracks.data.std.accessors.PointableBinaryRangeAscComparatorFactory;
import org.apache.hyracks.data.std.accessors.PointableBinaryRangeDescComparatorFactory;
import org.apache.hyracks.data.std.api.IComparable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.range.FieldRangePartitionComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

public class FieldRangePartitionComputerFactoryTest extends TestCase {

    private final Integer64SerializerDeserializer int64Serde = Integer64SerializerDeserializer.INSTANCE;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] SerDers = new ISerializerDeserializer[] {
            Integer64SerializerDeserializer.INSTANCE };
    private final RecordDescriptor RecordDesc = new RecordDescriptor(SerDers);

    IBinaryRangeComparatorFactory[] BINARY_ASC_COMPARATOR_FACTORIES = new IBinaryRangeComparatorFactory[] {
            new PointableBinaryRangeAscComparatorFactory(LongPointable.FACTORY) };
    IBinaryRangeComparatorFactory[] BINARY_DESC_COMPARATOR_FACTORIES = new IBinaryRangeComparatorFactory[] {
            new PointableBinaryRangeDescComparatorFactory(LongPointable.FACTORY) };
    IBinaryRangeComparatorFactory[] BINARY_REPLICATE_COMPARATOR_FACTORIES = new IBinaryRangeComparatorFactory[] {
            new PointableBinaryReplicateRangeComparatorFactory(LongPointable.FACTORY) };
    /*
     * The following points (X) will be tested for these 4 partitions.
     *
     * X-------X----XXX----X----XXX----X----XXX----X-------X
     *    -----------|-----------|-----------|-----------
     *
     * The following points (X) will be tested for these 16 partitions.
     *
     * X-------X----XXX----X----XXX----X----XXX----X-------X
     *    --|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--
     */

    private final int FRAME_SIZE = 320;
    private final int INTEGER_LENGTH = Long.BYTES;

    //result index {      0,   1,   2,   3,    4,    5,    6,    7,    8,    9,   10,   11,   12,   13,   14,   15   };
    //points       {     20l, 45l, 70l, 95l, 120l, 145l, 170l, 195l, 220l, 245l, 270l, 295l, 320l, 345l, 370l, 395l  };
    private final Long[] EACH_PARTITION = new Long[] { 20l, 45l, 70l, 95l, 120l, 145l, 170l, 195l, 220l, 245l, 270l,
            295l, 320l, 345l, 370l, 395l };

    //result index {      0,   1,   2,   3,    4,    5,    6,    7,    8,    9,    10,   11,   12,   13,   14        };
    //points       {    -25l, 50l, 99l, 100l, 101l, 150l, 199l, 200l, 201l, 250l, 299l, 300l, 301l, 350l, 425l       };
    private final Long[] PARTITION_BOUNDRIES = new Long[] { -25l, 50l, 99l, 100l, 101l, 150l, 199l, 200l, 201l, 250l,
            299l, 300l, 301l, 350l, 425l };

    //map          { 0l, 25l, 50l, 75l, 100l, 125l, 150l, 175l, 200l, 225l, 250l, 275l, 300l, 325l, 350l, 375l, 400l };
    //partitions   {    0,   1,   2,   3,    4,    5,    6,    7,    8,    9,    10,   11,   12,   13,   14,  15     };
    private final Long[] MAP_POINTS = new Long[] { 0l, 25l, 50l, 75l, 100l, 125l, 150l, 175l, 200l, 225l, 250l, 275l,
            300l, 325l, 350l, 375l, 400l };

    private byte[] getIntegerBytes(Long[] integers) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            for (int i = 0; i < integers.length; ++i) {
                int64Serde.serialize(integers[i], dos);
            }
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    private IRangeMap getRangeMap(Long[] integers) throws HyracksDataException {
        int offsets[] = new int[integers.length];
        for (int i = 0; i < integers.length; ++i) {
            offsets[i] = (i + 1) * INTEGER_LENGTH;
        }
        return new RangeMap(1, getIntegerBytes(integers), offsets);
    }

    private ByteBuffer prepareData(IHyracksTaskContext ctx, Long[] integers) throws HyracksDataException {
        IFrame frame = new VSizeFrame(ctx);
        FrameFixedFieldTupleAppender fffta = new FrameFixedFieldTupleAppender(RecordDesc.getFieldCount());
        fffta.reset(frame, true);

        byte[] serializedIntegers = getIntegerBytes(integers);
        for (int i = 0; i < integers.length; ++i) {
            fffta.appendField(serializedIntegers, i * INTEGER_LENGTH, INTEGER_LENGTH);
        }

        return frame.getBuffer();
    }

    private void executeFieldRangePartitionTests(Long[] integers, IRangeMap rangeMap,
            IBinaryRangeComparatorFactory[] comparatorFactories, RangePartitioningType rangeType, int nParts,
            int[][] results) throws HyracksDataException {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int[] rangeFields = new int[] { 0 };
        ITupleRangePartitionComputerFactory frpcf = new FieldRangePartitionComputerFactory(rangeFields,
                comparatorFactories, rangeType);
        ITupleRangePartitionComputer partitioner = frpcf.createPartitioner(rangeMap);

        IFrameTupleAccessor accessor = new FrameTupleAccessor(RecordDesc);
        ByteBuffer buffer = prepareData(ctx, integers);
        accessor.reset(buffer);

        IGrowableIntArray map = new IntArrayList(16, 1);

        for (int i = 0; i < results.length; ++i) {
            map.clear();
            partitioner.partition(accessor, i, nParts, map);
            checkPartitionResult(integers[i], results[i], map);
        }
    }

    private String getString(int[] results) {
        String result = "[";
        for (int i = 0; i < results.length; ++i) {
            result += results[i];
            if (i < results.length - 1) {
                result += ", ";
            }
        }
        result += "]";
        return result;
    }

    private String getString(IGrowableIntArray results) {
        String result = "[";
        for (int i = 0; i < results.size(); ++i) {
            result += results.get(i);
            if (i < results.size() - 1) {
                result += ", ";
            }
        }
        result += "]";
        return result;
    }

    private void checkPartitionResult(Long value, int[] results, IGrowableIntArray map) {
        if (results.length != map.size()) {
            Assert.assertEquals("The partition for value (" + value + ") gives different number of partitions",
                    results.length, map.size());
        }
        for (int i = 0; i < results.length; ++i) {
            boolean match = false;
            for (int j = 0; j < results.length; ++j) {
                if (results[i] == map.get(j)) {
                    match = true;
                    continue;
                }
            }
            if (!match) {
                Assert.assertEquals("Individual partitions for " + value + " do not match", getString(results),
                        getString(map));
                return;
            }
        }
    }

    @Test
    public void testFieldRangePartitionAscProject4AllPartitions() throws HyracksDataException {
        int[][] results = new int[16][];
        results[0] = new int[] { 0 };
        results[1] = new int[] { 0 };
        results[2] = new int[] { 0 };
        results[3] = new int[] { 0 };
        results[4] = new int[] { 1 };
        results[5] = new int[] { 1 };
        results[6] = new int[] { 1 };
        results[7] = new int[] { 1 };
        results[8] = new int[] { 2 };
        results[9] = new int[] { 2 };
        results[10] = new int[] { 2 };
        results[11] = new int[] { 2 };
        results[12] = new int[] { 3 };
        results[13] = new int[] { 3 };
        results[14] = new int[] { 3 };
        results[15] = new int[] { 3 };

        IRangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangePartitionTests(EACH_PARTITION, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT, 4, results);
    }

    @Test
    public void testFieldRangePartitionDescProject4AllPartitions() throws HyracksDataException {
        int[][] results = new int[16][];
        results[0] = new int[] { 3 };
        results[1] = new int[] { 3 };
        results[2] = new int[] { 3 };
        results[3] = new int[] { 3 };
        results[4] = new int[] { 2 };
        results[5] = new int[] { 2 };
        results[6] = new int[] { 2 };
        results[7] = new int[] { 2 };
        results[8] = new int[] { 1 };
        results[9] = new int[] { 1 };
        results[10] = new int[] { 1 };
        results[11] = new int[] { 1 };
        results[12] = new int[] { 0 };
        results[13] = new int[] { 0 };
        results[14] = new int[] { 0 };
        results[15] = new int[] { 0 };

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        IRangeMap rangeMap = getRangeMap(map);

        executeFieldRangePartitionTests(EACH_PARTITION, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT, 4, results);
    }

    @Test
    public void testFieldRangePartitionAscProject16AllPartitions() throws HyracksDataException {
        int[][] results = new int[16][];
        results[0] = new int[] { 0 };
        results[1] = new int[] { 1 };
        results[2] = new int[] { 2 };
        results[3] = new int[] { 3 };
        results[4] = new int[] { 4 };
        results[5] = new int[] { 5 };
        results[6] = new int[] { 6 };
        results[7] = new int[] { 7 };
        results[8] = new int[] { 8 };
        results[9] = new int[] { 9 };
        results[10] = new int[] { 10 };
        results[11] = new int[] { 11 };
        results[12] = new int[] { 12 };
        results[13] = new int[] { 13 };
        results[14] = new int[] { 14 };
        results[15] = new int[] { 15 };

        IRangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangePartitionTests(EACH_PARTITION, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT, 16, results);
    }

    @Test
    public void testFieldRangePartitionDescProject16AllPartitions() throws HyracksDataException {
        int[][] results = new int[16][];
        results[0] = new int[] { 15 };
        results[1] = new int[] { 14 };
        results[2] = new int[] { 13 };
        results[3] = new int[] { 12 };
        results[4] = new int[] { 11 };
        results[5] = new int[] { 10 };
        results[6] = new int[] { 9 };
        results[7] = new int[] { 8 };
        results[8] = new int[] { 7 };
        results[9] = new int[] { 6 };
        results[10] = new int[] { 5 };
        results[11] = new int[] { 4 };
        results[12] = new int[] { 3 };
        results[13] = new int[] { 2 };
        results[14] = new int[] { 1 };
        results[15] = new int[] { 0 };

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        IRangeMap rangeMap = getRangeMap(map);

        executeFieldRangePartitionTests(EACH_PARTITION, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT, 16, results);
    }

    @Test
    public void testFieldRangePartitionAscProject16Partitions() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0 };
        results[1] = new int[] { 2 };
        results[2] = new int[] { 3 };
        results[3] = new int[] { 4 };
        results[4] = new int[] { 4 };
        results[5] = new int[] { 6 };
        results[6] = new int[] { 7 };
        results[7] = new int[] { 8 };
        results[8] = new int[] { 8 };
        results[9] = new int[] { 10 };
        results[10] = new int[] { 11 };
        results[11] = new int[] { 12 };
        results[12] = new int[] { 12 };
        results[13] = new int[] { 14 };
        results[14] = new int[] { 15 };

        IRangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangePartitionTests(PARTITION_BOUNDRIES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT, 16, results);
    }

    @Test
    public void testFieldRangePartitionAscProject4Partitions() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0 };
        results[1] = new int[] { 0 };
        results[2] = new int[] { 0 };
        results[3] = new int[] { 1 };
        results[4] = new int[] { 1 };
        results[5] = new int[] { 1 };
        results[6] = new int[] { 1 };
        results[7] = new int[] { 2 };
        results[8] = new int[] { 2 };
        results[9] = new int[] { 2 };
        results[10] = new int[] { 2 };
        results[11] = new int[] { 3 };
        results[12] = new int[] { 3 };
        results[13] = new int[] { 3 };
        results[14] = new int[] { 3 };

        IRangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangePartitionTests(PARTITION_BOUNDRIES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT, 4, results);
    }

    @Test
    public void testFieldRangePartitionAscReplicate4Partitions() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0, 1, 2, 3 };
        results[1] = new int[] { 0, 1, 2, 3 };
        results[2] = new int[] { 0, 1, 2, 3 };
        results[3] = new int[] { 1, 2, 3 };
        results[4] = new int[] { 1, 2, 3 };
        results[5] = new int[] { 1, 2, 3 };
        results[6] = new int[] { 1, 2, 3 };
        results[7] = new int[] { 2, 3 };
        results[8] = new int[] { 2, 3 };
        results[9] = new int[] { 2, 3 };
        results[10] = new int[] { 2, 3 };
        results[11] = new int[] { 3 };
        results[12] = new int[] { 3 };
        results[13] = new int[] { 3 };
        results[14] = new int[] { 3 };

        IRangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangePartitionTests(PARTITION_BOUNDRIES, rangeMap, BINARY_REPLICATE_COMPARATOR_FACTORIES,
                RangePartitioningType.REPLICATE, 4, results);
    }

    @Test
    public void testFieldRangePartitionAscReplicate16Partitions() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[1] = new int[] { 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[2] = new int[] { 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[3] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[4] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[5] = new int[] { 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[6] = new int[] { 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[7] = new int[] { 8, 9, 10, 11, 12, 13, 14, 15 };
        results[8] = new int[] { 8, 9, 10, 11, 12, 13, 14, 15 };
        results[9] = new int[] { 10, 11, 12, 13, 14, 15 };
        results[10] = new int[] { 11, 12, 13, 14, 15 };
        results[11] = new int[] { 12, 13, 14, 15 };
        results[12] = new int[] { 12, 13, 14, 15 };
        results[13] = new int[] { 14, 15 };
        results[14] = new int[] { 15 };

        IRangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangePartitionTests(PARTITION_BOUNDRIES, rangeMap, BINARY_REPLICATE_COMPARATOR_FACTORIES,
                RangePartitioningType.REPLICATE, 16, results);
    }

    private class PointableBinaryReplicateRangeComparatorFactory implements IBinaryRangeComparatorFactory {
        private static final long serialVersionUID = 1L;

        protected final IPointableFactory pf;

        public PointableBinaryReplicateRangeComparatorFactory(IPointableFactory pf) {
            this.pf = pf;
        }

        @Override
        public IBinaryComparator createMinBinaryComparator() {
            final IPointable p = pf.createPointable();
            return new IBinaryComparator() {
                @Override
                public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                    if (l1 == 0 && l2 != 0)
                        return -1;
                    if (l1 != 0 && l2 == 0)
                        return 1;
                    p.set(b1, s1, l1);
                    return ((IComparable) p).compareTo(b2, s2, l2);
                }
            };
        }

        @Override
        public IBinaryComparator createMaxBinaryComparator() {
            return new IBinaryComparator() {
                @Override
                public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                    return -1;
                }
            };
        }
    }

}
