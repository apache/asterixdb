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

package org.apache.hyracks.storage.am.vector.frames;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.lsm.vector.tuples.LSMVTreeDataTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * {@link VTreeDataFrame} on pages confiscated from the buffer cache, with no index or LSM component. A data page
 * keeps its tuples ordered by {@code <distance-to-centroid, PK>}, which key routing, the directory's separators and
 * the search-side merge all depend on.
 */
public class VTreeDataFrameTest {

    private static final int PAGE_SIZE = 512;
    private static final int NUM_PAGES = 40;
    private static final int MAX_OPEN_FILES = 10;
    private static final int FRAME_SIZE = 32768;
    private static final int DIMENSIONS = 4;
    /** Small enough that many tuples share a page, so a split has halves worth asserting about. */
    private static final int SMALL_PAYLOAD_BYTES = 8;

    /** Non-quantized data tuple {@code [distance, centroidId, pk, payload]}; the key is distance then PK. */
    private static final ITypeTraits[] DATA_TYPE_TRAITS = { DoublePointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS,
            IntegerPointable.TYPE_TRAITS, VarLengthTypeTrait.INSTANCE };
    private static final int PK_FIELD = VTreeDataTupleAccessor.NQ_KEY_FIELDS_START;
    private static final int[] COMPARATOR_FIELDS = { VTreeDataTupleAccessor.DISTANCE_FIELD, PK_FIELD };
    private static final IBinaryComparatorFactory[] KEY_CMP_FACTORIES =
            { DoubleBinaryComparatorFactory.INSTANCE, IntegerBinaryComparatorFactory.INSTANCE };
    @SuppressWarnings("rawtypes")
    private static final ISerializerDeserializer[] KEY_SERDES =
            { DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

    private IHyracksTaskContext ctx;
    private IBufferCache bufferCache;
    private final List<ICachedPage> confiscated = new ArrayList<>();

    @Before
    public void setUp() throws HyracksDataException {
        ctx = TestUtils.create(FRAME_SIZE);
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
    }

    @After
    public void tearDown() throws HyracksDataException {
        for (ICachedPage page : confiscated) {
            bufferCache.returnPage(page, false);
        }
        confiscated.clear();
        bufferCache.close();
    }

    /** An empty frame has one insertion point, and it is 0. */
    @Test
    public void findInsertPositionOnAnEmptyFrameIsZero() throws HyracksDataException {
        VTreeDataFrame frame = newFrame();

        Assert.assertEquals(0, frame.findInsertPosition(key(1.0, 0)));
        Assert.assertEquals(0, frame.findInsertPosition(key(Double.NEGATIVE_INFINITY, Integer.MIN_VALUE)));
    }

    /**
     * Equal distances are ordered by PK, so a key's position is the lower bound of {@code <distance, PK>}:
     * after every tuple with a smaller key and on the tuple with an equal one, which is the entry a same-key
     * write replaces. This is the property the directory's "first separator whose key >= k" routing mirrors.
     */
    @Test
    public void findInsertPositionOrdersEqualDistancesByPk() throws HyracksDataException {
        VTreeDataFrame frame = newFrame();
        insertSorted(frame, 1.0, 1);
        insertSorted(frame, 2.0, 2);
        insertSorted(frame, 2.0, 3);
        insertSorted(frame, 3.0, 4);

        Assert.assertEquals("between distances", 1, frame.findInsertPosition(key(1.5, 0)));
        Assert.assertEquals("before both 2.0 entries", 1, frame.findInsertPosition(key(2.0, 0)));
        Assert.assertEquals("on an equal key", 2, frame.findInsertPosition(key(2.0, 3)));
        Assert.assertEquals("after both 2.0 entries", 3, frame.findInsertPosition(key(2.0, 9)));
        Assert.assertEquals("after everything", 4, frame.findInsertPosition(key(4.0, 0)));
        Assert.assertEquals("before everything", 0, frame.findInsertPosition(key(0.5, 0)));
        Assert.assertEquals("an equal key is found by lookup", 2, frame.findTupleByKey(key(2.0, 3)));
        Assert.assertEquals("a missing key is not", -1, frame.findTupleByKey(key(2.0, 4)));
    }

    /** Inserting in arbitrary order through findInsertPosition leaves the page ordered. */
    @Test
    public void insertsArriveOutOfOrderAndTheFrameStaysOrdered() throws HyracksDataException {
        VTreeDataFrame frame = newFrame();
        double[] arrivalOrder = { 5.0, 1.0, 3.0, 2.0, 4.0, 3.0 };
        for (int i = 0; i < arrivalOrder.length; i++) {
            insertSorted(frame, arrivalOrder[i], i);
        }

        Assert.assertEquals(arrivalOrder.length, frame.getTupleCount());
        assertOrdered(frame);
    }

    /** A split leaves both halves ordered with every tuple still readable, which the merge cursors rely on. */
    @Test
    public void splitLeavesBothHalvesOrderedAndLosesNothing() throws HyracksDataException {
        VTreeDataFrame left = newFrame();
        int inserted = fillToCapacity(left);
        Assert.assertTrue("need several tuples for a split to be meaningful", inserted >= 4);

        VTreeDataFrame right = newFrame();
        // A distance in the middle of the occupied range, so it lands in whichever half covers it.
        left.split(right, dataTuple(inserted / 2.0, 9999, SMALL_PAYLOAD_BYTES));

        Assert.assertEquals("split must not lose or duplicate tuples", inserted + 1,
                left.getTupleCount() + right.getTupleCount());
        assertOrdered(left);
        assertOrdered(right);
        Assert.assertTrue("the left half's last key must not exceed the right half's first", left.getTupleCount() == 0
                || right.getTupleCount() == 0 || compareKeys(left, left.getTupleCount() - 1, right, 0) <= 0);
    }

    /**
     * A tuple of {@code getMaxTupleSize}, the widest the write path admits, fits the half a split hands it.
     * Wider tuples are refused before any split ({@code VTreePageMutator.requireFits}).
     */
    @Test
    public void splitAlwaysFitsATupleWithinTheMaxTupleSize() throws HyracksDataException {
        VTreeDataFrame left = newFrame();
        int inserted = fillToCapacity(left);
        VTreeDataFrame right = newFrame();

        // The payload's length prefix widens with it, so the widest admitted tuple is found by search.
        int maxTupleBytes = left.getMaxTupleSize(PAGE_SIZE);
        int payload = maxTupleBytes;
        while (left.getBytesRequiredToWriteTuple(dataTuple(inserted / 2.0, 9999, payload)) > maxTupleBytes) {
            payload--;
        }
        ITupleReference widest = dataTuple(inserted / 2.0, 9999, payload);
        Assert.assertTrue("fixture: one byte more would be refused by the write path",
                left.getBytesRequiredToWriteTuple(dataTuple(inserted / 2.0, 9999, payload + 1)) > maxTupleBytes);

        left.split(right, widest);

        Assert.assertEquals("split must not lose or duplicate tuples", inserted + 1,
                left.getTupleCount() + right.getTupleCount());
        assertOrdered(left);
        assertOrdered(right);
    }

    private VTreeDataFrame newFrame() throws HyracksDataException {
        VTreeDataFrameFactory factory =
                new VTreeDataFrameFactory(new LSMVTreeDataTupleWriterFactory(DATA_TYPE_TRAITS, false, null, null),
                        DIMENSIONS, COMPARATOR_FIELDS, KEY_CMP_FACTORIES, null);
        IVTreeDataFrame frame = factory.createFrame();
        ICachedPage page = bufferCache.confiscatePage(IBufferCache.INVALID_DPID);
        confiscated.add(page);
        frame.setPage(page);
        frame.initBuffer((byte) 0);
        return (VTreeDataFrame) frame;
    }

    /** Inserts increasing distances until the frame reports it is out of contiguous space. */
    private int fillToCapacity(VTreeDataFrame frame) throws HyracksDataException {
        int count = 0;
        while (true) {
            ITupleReference tuple = dataTuple(count + 1.0, count, SMALL_PAYLOAD_BYTES);
            if (frame.hasSpaceInsert(tuple) != FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE) {
                return count;
            }
            frame.insert(tuple, frame.findInsertPosition(frame.keyOf(tuple)));
            count++;
        }
    }

    private void insertSorted(VTreeDataFrame frame, double distance, int pk) throws HyracksDataException {
        ITupleReference tuple = dataTuple(distance, pk, SMALL_PAYLOAD_BYTES);
        frame.insert(tuple, frame.findInsertPosition(frame.keyOf(tuple)));
    }

    private static ITupleReference key(double distance, int pk) throws HyracksDataException {
        return TupleUtils.createTuple(KEY_SERDES, distance, pk);
    }

    private static ITupleReference dataTuple(double distance, int pk, int payloadBytes) throws HyracksDataException {
        ArrayTupleBuilder builder = new ArrayTupleBuilder(DATA_TYPE_TRAITS.length);
        try {
            builder.getDataOutput().writeDouble(distance);
            builder.addFieldEndOffset();
            builder.getDataOutput().writeInt(0);
            builder.addFieldEndOffset();
            builder.getDataOutput().writeInt(pk);
            builder.addFieldEndOffset();
            builder.getDataOutput().write(new byte[payloadBytes]);
            builder.addFieldEndOffset();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
        ArrayTupleReference tuple = new ArrayTupleReference();
        tuple.reset(builder.getFieldEndOffsets(), builder.getByteArray());
        return tuple;
    }

    private static int pkAt(VTreeDataFrame frame, int index) {
        ITreeIndexTupleReference tuple = frame.createTupleReference();
        tuple.resetByTupleIndex(frame, index);
        return IntegerPointable.getInteger(tuple.getFieldData(PK_FIELD), tuple.getFieldStart(PK_FIELD));
    }

    /** Orders two stored tuples the way the frame does: by distance, then by PK. */
    private static int compareKeys(VTreeDataFrame a, int i, VTreeDataFrame b, int j) throws HyracksDataException {
        int byDistance = Double.compare(a.getDistanceToCentroid(i), b.getDistanceToCentroid(j));
        return byDistance != 0 ? byDistance : Integer.compare(pkAt(a, i), pkAt(b, j));
    }

    private static void assertOrdered(VTreeDataFrame frame) throws HyracksDataException {
        for (int i = 1; i < frame.getTupleCount(); i++) {
            Assert.assertTrue("keys out of order at " + i, compareKeys(frame, i - 1, frame, i) < 0);
        }
    }
}
