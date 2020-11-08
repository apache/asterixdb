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

package org.apache.hyracks.dataflow.common.data.partition.range;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.accessors.LongBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;

import junit.framework.TestCase;

/**
 * These tests check the range partitioning types with various interval sizes and range map split points.
 * For each range type they check the ASCending comparators for intervals with durations of D = 3, and
 * a range map of the overall range that has been split into N = 4 parts.
 * the test for the Split type also checks larger intervals and more splits on the range map to make sure it splits
 * correctly across many partitions, and within single partitions.
 * <p>
 * Currently, this test does not support DESCending intervals, and there is no test case that checks which interval
 * is included in split if the ending point lands on the point between two partitions.
 * <p>
 * The map of the partitions, listed as the rangeMap split points in ascending order:
 * <p>
 * The following points (X) will be tested for these 4 partitions.
 * <p>
 * X  -----------X----------XXX----------X----------XXX----------X------------XXX------------X------------  X
 * -----------------------|-----------------------|-------------------------|--------------------------
 * <p>
 * The following points (X) will be tested for these 16 partitions.
 * <p>
 * X  -----------X----------XXX----------X----------XXX----------X------------XXX------------X------------  X
 * -----|-----|-----|-----|-----|-----|-----|-----|-----|-----|------|------|------|------|------|-----
 * <p>
 * N4                0          )[           1          )[           2            )[             3
 * N16     0  )[  1 )[  2 )[  3 )[  4 )[  5 )[  6 )[  7 )[  8 )[  9 )[  10 )[  11 )[  12 )[  13 )[  14 )[  15
 * ASC   0     25    50    75    100   125   150   175   200   225   250    275    300    325    350    375    400
 * <p>
 * First and last partitions include all values less than and greater than min and max split points respectively.
 * <p>
 * Here are the test points inside EACH_PARTITION:
 * result index {      0,   1,   2,   3,    4,    5,    6,    7,    8,    9,   10,   11,   12,   13,   14,   15   };
 * points       {     20l, 45l, 70l, 95l, 120l, 145l, 170l, 195l, 220l, 245l, 270l, 295l, 320l, 345l, 370l, 395l  };
 * <p>
 * PARTITION_EDGE_CASES: Tests points at or near partition boundaries and at the ends of the partition range.
 * result index {      0,   1,   2,   3,    4,    5,    6,    7,    8,    9,    10,   11,   12,   13,   14        };
 * points       {    -25l, 50l, 99l, 100l, 101l, 150l, 199l, 200l, 201l, 250l, 299l, 300l, 301l, 350l, 425l       };
 * <p>
 * MAP_POINTS: The map of the partitions, listed as the split points.
 * partitions   {  0,   1,   2,   3,    4,    5,    6,    7,    8,    9,   10,   11,   12,   13,   14,   15,   16 };
 * map          { 0l, 25l, 50l, 75l, 100l, 125l, 150l, 175l, 200l, 225l, 250l, 275l, 300l, 325l, 350l, 375l, 400l };
 * <p>
 * Both rangeMap partitions and test intervals are end exclusive.
 * an ascending test interval ending on 200 like (190, 200) is not in partition 8.
 * <p>
 * The Following, Intersect, and Partition partitioning is based off of Split, Replicate, Project in
 * "Processing Interval Joins On Map-Reduce" by Chawda, et. al.
 */
public abstract class AbstractFieldRangeMultiPartitionComputerFactoryTest extends TestCase {

    protected static final Long[] EACH_PARTITION =
            new Long[] { 20l, 45l, 70l, 95l, 120l, 145l, 170l, 195l, 220l, 245l, 270l, 295l, 320l, 345l, 370l, 395l };
    protected static final Long[] PARTITION_EDGE_CASES =
            new Long[] { -25l, 50l, 99l, 100l, 101l, 150l, 199l, 200l, 201l, 250l, 299l, 300l, 301l, 350l, 425l };
    protected static final Long[] MAP_POINTS =
            new Long[] { 25l, 50l, 75l, 100l, 125l, 150l, 175l, 200l, 225l, 250l, 275l, 300l, 325l, 350l, 375l };
    private final Integer64SerializerDeserializer integerSerde = Integer64SerializerDeserializer.INSTANCE;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] twoIntegerSerDers = new ISerializerDeserializer[] {
            Integer64SerializerDeserializer.INSTANCE, Integer64SerializerDeserializer.INSTANCE };
    private final RecordDescriptor recordIntegerDesc = new RecordDescriptor(twoIntegerSerDers);
    private static final int FRAME_SIZE = 640;
    private static final int INTEGER_LENGTH = Long.BYTES;
    static final IBinaryComparatorFactory[] BINARY_ASC_COMPARATOR_FACTORIES =
            new IBinaryComparatorFactory[] { LongBinaryComparatorFactory.INSTANCE };
    static final IBinaryComparatorFactory[] BINARY_DESC_COMPARATOR_FACTORIES =
            new IBinaryComparatorFactory[] { LongDescBinaryComparatorFactory.INSTANCE };
    protected static final int[] START_FIELD = new int[] { 0 };
    protected static final int[] END_FIELD = new int[] { 1 };

    private byte[] getIntegerBytes(Long[] integers) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            for (int i = 0; i < integers.length; ++i) {
                integerSerde.serialize(integers[i], dos);
            }
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected RangeMap getIntegerRangeMap(Long[] integers) throws HyracksDataException {
        int[] offsets = new int[integers.length];
        for (int i = 0; i < integers.length; ++i) {
            offsets[i] = (i + 1) * INTEGER_LENGTH;
        }
        return new RangeMap(1, getIntegerBytes(integers), offsets, null);
    }

    private ByteBuffer prepareData(IHyracksTaskContext ctx, Long[] startPoints, Long duration)
            throws HyracksDataException {
        IFrame frame = new VSizeFrame(ctx);

        FrameTupleAppender appender = new FrameTupleAppender();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(recordIntegerDesc.getFieldCount());
        DataOutput dos = tb.getDataOutput();
        appender.reset(frame, true);

        for (int i = 0; i < startPoints.length; ++i) {
            tb.reset();
            integerSerde.serialize(startPoints[i], dos);
            tb.addFieldEndOffset();
            integerSerde.serialize(startPoints[i] + duration, dos);
            tb.addFieldEndOffset();
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        }
        return frame.getBuffer();
    }

    protected void executeFieldRangeFollowingPartitionTests(Long[] integers, RangeMap rangeMap,
            IBinaryComparatorFactory[] minComparatorFactories, int nParts, int[][] results, long duration,
            int[] rangeFields) throws HyracksDataException {

        StaticRangeMapSupplier rangeMapSupplier = new StaticRangeMapSupplier(rangeMap);
        SourceLocation sourceLocation = new SourceLocation(0, 0);

        ITupleMultiPartitionComputerFactory itmpcf = new FieldRangeFollowingPartitionComputerFactory(rangeFields,
                minComparatorFactories, rangeMapSupplier, sourceLocation);

        executeFieldRangeMultiPartitionTests(integers, itmpcf, nParts, results, duration);

    }

    protected void executeFieldRangeIntersectPartitionTests(Long[] integers, RangeMap rangeMap,
            IBinaryComparatorFactory[] minComparatorFactories, int nParts, int[][] results, long duration,
            int[] startFields, int[] endFields) throws HyracksDataException {

        StaticRangeMapSupplier rangeMapSupplier = new StaticRangeMapSupplier(rangeMap);
        SourceLocation sourceLocation = new SourceLocation(0, 0);

        ITupleMultiPartitionComputerFactory itmpcf = new FieldRangeIntersectPartitionComputerFactory(startFields,
                endFields, minComparatorFactories, rangeMapSupplier, sourceLocation);

        executeFieldRangeMultiPartitionTests(integers, itmpcf, nParts, results, duration);
    }

    protected void executeFieldRangePartitionTests(Long[] integers, RangeMap rangeMap,
            IBinaryComparatorFactory[] minComparatorFactories, int nParts, int[][] results, long duration,
            int[] rangeFields) throws HyracksDataException {

        StaticRangeMapSupplier rangeMapSupplier = new StaticRangeMapSupplier(rangeMap);
        SourceLocation sourceLocation = new SourceLocation(0, 0);

        ITuplePartitionComputerFactory itpcf = new FieldRangePartitionComputerFactory(rangeFields,
                minComparatorFactories, rangeMapSupplier, sourceLocation, false);

        executeFieldRangePartitionTests(integers, itpcf, nParts, results, duration);

    }

    protected void executeFieldRangeMultiPartitionTests(Long[] integers, ITupleMultiPartitionComputerFactory itmpcf,
            int nParts, int[][] results, long duration) throws HyracksDataException {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);

        ITupleMultiPartitionComputer partitioner = itmpcf.createPartitioner(ctx);
        partitioner.initialize();

        IFrameTupleAccessor accessor = new FrameTupleAccessor(recordIntegerDesc);
        ByteBuffer buffer = prepareData(ctx, integers, duration);
        accessor.reset(buffer);

        for (int i = 0; i < results.length; ++i) {
            BitSet map = partitioner.partition(accessor, i, nParts);
            checkPartitionResult(results[i], map);
        }
    }

    protected void executeFieldRangePartitionTests(Long[] integers, ITuplePartitionComputerFactory itpcf, int nParts,
            int[][] results, long duration) throws HyracksDataException {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);

        ITuplePartitionComputer partitioner = itpcf.createPartitioner(ctx);
        partitioner.initialize();

        IFrameTupleAccessor accessor = new FrameTupleAccessor(recordIntegerDesc);
        ByteBuffer buffer = prepareData(ctx, integers, duration);
        accessor.reset(buffer);

        int partition;

        for (int i = 0; i < results.length; ++i) {
            partition = partitioner.partition(accessor, i, nParts);
            Assert.assertEquals("The partitions do not match for test " + i + ".", results[i][0], partition);
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

    private String getString(BitSet results) {
        int count = 0;
        String result = "[";
        for (int i = 0; i < results.length(); ++i) {
            if (results.get(i)) {
                if (count > 0) {
                    result += ", ";
                }
                result += i;
                count++;
            }
        }
        result += "]";
        return result;
    }

    private void checkPartitionResult(int[] results, BitSet map) {
        Assert.assertTrue("The number of partitions in the Bitset:(" + map.cardinality() + ") and the results:("
                + results.length + ") do not match.", results.length == map.cardinality());
        for (int i = 0; i < results.length; ++i) {
            Assert.assertTrue("The map partition " + getString(map) + " and the results " + getString(results)
                    + " do not match. 2.", map.get(results[i]));
        }
    }
}
