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

package org.apache.asterix.dataflow.data.nontagged.comparators.rangeinterval;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.base.AInterval;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryRangeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITupleRangePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITupleRangePartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.storage.IGrowableIntArray;
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.range.FieldRangePartitionComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangePartitionType.RangePartitioningType;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

public class IntervalRangePartitionComputerFactoryTest extends TestCase {

    private final ISerializerDeserializer<AInterval> intervalSerde = AIntervalSerializerDeserializer.INSTANCE;
    private final Integer64SerializerDeserializer int64Serde = Integer64SerializerDeserializer.INSTANCE;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] SerDers = new ISerializerDeserializer[] {
            AIntervalSerializerDeserializer.INSTANCE };
    private final RecordDescriptor RecordDesc = new RecordDescriptor(SerDers);

    IBinaryRangeComparatorFactory[] BINARY_ASC_COMPARATOR_FACTORIES = new IBinaryRangeComparatorFactory[] {
            IntervalAscRangeBinaryComparatorFactory.INSTANCE };
    IBinaryRangeComparatorFactory[] BINARY_DESC_COMPARATOR_FACTORIES = new IBinaryRangeComparatorFactory[] {
            IntervalDescRangeBinaryComparatorFactory.INSTANCE };
    /*
     * The following three intervals (+++) will be tested for these 4 partitions.
     *
     *    ----------+++++++++++++++++++++++++++----------
     *    -----------+++++++++++++++++++++++++-----------
     *    ------------+++++++++++++++++++++++------------
     *    -----------|-----------|-----------|-----------
     *     or 16 partitions.
     *    --|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--
     */

    private final int FRAME_SIZE = 320;
    private final int INTEGER_LENGTH = Long.BYTES;

    private final AInterval[] INTERVALS = new AInterval[] { new AInterval(99, 301, (byte) 16),
            new AInterval(100, 300, (byte) 16), new AInterval(101, 299, (byte) 16) };
    private final int INTERVAL_LENGTH = Byte.BYTES + Long.BYTES + Long.BYTES;

    //map          { 0l, 25l, 50l, 75l, 100l, 125l, 150l, 175l, 200l, 225l, 250l, 275l, 300l, 325l, 350l, 375l, 400l };
    //partitions   {    0,   1,   2,   3,    4,    5,    6,    7,    8,    9,    10,   11,   12,   13,   14,  15     };
    private final Long[] MAP_POINTS = new Long[] { 0l, 25l, 50l, 75l, 100l, 125l, 150l, 175l, 200l, 225l, 250l, 275l,
            300l, 325l, 350l, 375l, 400l };

    @SuppressWarnings("unused")
    private byte[] getIntervalBytes(AInterval[] intervals) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            for (int i = 0; i < intervals.length; ++i) {
                intervalSerde.serialize(intervals[i], dos);
            }
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

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

    private ByteBuffer prepareData(IHyracksTaskContext ctx, AInterval[] intervals) throws HyracksDataException {
        IFrame frame = new VSizeFrame(ctx);
        FrameFixedFieldTupleAppender fffta = new FrameFixedFieldTupleAppender(RecordDesc.getFieldCount());
        fffta.reset(frame, true);

        byte[] serializedIntervals = getIntervalBytes(intervals);
        for (int i = 0; i < intervals.length; ++i) {
            fffta.appendField(serializedIntervals, i * INTERVAL_LENGTH, INTERVAL_LENGTH);
        }

        return frame.getBuffer();
    }

    private void executeFieldRangePartitionTests(AInterval[] intervals, IRangeMap rangeMap,
            IBinaryRangeComparatorFactory[] comparatorFactories, RangePartitioningType rangeType, int nParts,
            int[][] results) throws HyracksDataException {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int[] rangeFields = new int[] { 0 };
        ITupleRangePartitionComputerFactory frpcf = new FieldRangePartitionComputerFactory(rangeFields,
                comparatorFactories, rangeMap, rangeType);
        ITupleRangePartitionComputer partitioner = frpcf.createPartitioner();

        IFrameTupleAccessor accessor = new FrameTupleAccessor(RecordDesc);
        ByteBuffer buffer = prepareData(ctx, intervals);
        accessor.reset(buffer);

        IGrowableIntArray map = new IntArrayList(16, 1);

        for (int i = 0; i < results.length; ++i) {
            map.clear();
            partitioner.partition(accessor, i, nParts, map);
            checkPartitionResult(intervals[i], results[i], map);
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

    private void checkPartitionResult(AInterval value, int[] results, IGrowableIntArray map) {
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
    public void testFieldRangePartitionAscProject16Partitions() throws HyracksDataException {
        int[][] results = new int[3][];
        results[0] = new int[] { 3 };
        results[1] = new int[] { 4 };
        results[2] = new int[] { 4 };

        IRangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangePartitionTests(INTERVALS, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT, 16, results);
    }

    @Test
    public void testFieldRangePartitionDescProject16Partitions() throws HyracksDataException {
        int[][] results = new int[3][];
        results[0] = new int[] { 3 };
        results[1] = new int[] { 4 };
        results[2] = new int[] { 4 };

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        IRangeMap rangeMap = getRangeMap(map);

        executeFieldRangePartitionTests(INTERVALS, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT, 16, results);
    }

    @Test
    public void testFieldRangePartitionAscProjectEnd16Partitions() throws HyracksDataException {
        int[][] results = new int[3][];
        results[0] = new int[] { 12 };
        results[1] = new int[] { 12 };
        results[2] = new int[] { 11 };

        IRangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangePartitionTests(INTERVALS, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT_END, 16, results);
    }

    @Test
    public void testFieldRangePartitionDescProjectEnd16Partitions() throws HyracksDataException {
        int[][] results = new int[3][];
        results[0] = new int[] { 12 };
        results[1] = new int[] { 12 };
        results[2] = new int[] { 11 };

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        IRangeMap rangeMap = getRangeMap(map);

        executeFieldRangePartitionTests(INTERVALS, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT_END, 16, results);
    }

    @Test
    public void testFieldRangePartitionAscSplit16Partitions() throws HyracksDataException {
        int[][] results = new int[3][];
        results[0] = new int[] { 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        results[1] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        results[2] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11 };

        IRangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangePartitionTests(INTERVALS, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.SPLIT, 16, results);
    }

    @Test
    public void testFieldRangePartitionDescSplit16Partitions() throws HyracksDataException {
        int[][] results = new int[3][];
        results[0] = new int[] { 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        results[1] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        results[2] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11 };

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        IRangeMap rangeMap = getRangeMap(map);

        executeFieldRangePartitionTests(INTERVALS, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES,
                RangePartitioningType.SPLIT, 16, results);
    }

    @Test
    public void testFieldRangePartitionAscReplicate16Partitions() throws HyracksDataException {
        int[][] results = new int[3][];
        results[0] = new int[] { 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[1] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[2] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

        IRangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangePartitionTests(INTERVALS, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.REPLICATE, 16, results);
    }

    @Test
    public void testFieldRangePartitionDescReplicate16Partitions() throws HyracksDataException {
        int[][] results = new int[3][];
        results[0] = new int[] { 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[1] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[2] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        IRangeMap rangeMap = getRangeMap(map);

        executeFieldRangePartitionTests(INTERVALS, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES,
                RangePartitioningType.REPLICATE, 16, results);
    }

}
