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
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.storage.am.vector.utils.VTreeMetadataTupleAccessor;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * {@link VTreeMetadataFrame}, a directory page with one {@code [key..., data_page_pointer]} separator per data page,
 * where the key {@code <distance, PK>} is that page's last record. Key routing binary-searches the separators, so
 * they must ascend by key. A split re-initialises both halves and resets their next-page pointers, so a caller
 * splitting a page that is not last in its chain must re-link it.
 */
public class VTreeMetadataFrameTest {

    private static final int PAGE_SIZE = 512;
    private static final int NUM_PAGES = 40;
    private static final int MAX_OPEN_FILES = 10;
    private static final int FRAME_SIZE = 32768;
    private static final int DIMENSIONS = 4;

    /** The key schema LSMVTreeUtils derives for an int primary key: a raw double distance, then the PK. */
    @SuppressWarnings("rawtypes")
    private static final ISerializerDeserializer[] KEY_SERDES =
            { DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
    private static final ITypeTraits[] KEY_TYPE_TRAITS = { DoublePointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS };
    private static final IBinaryComparatorFactory[] KEY_CMP_FACTORIES =
            { DoubleBinaryComparatorFactory.INSTANCE, IntegerBinaryComparatorFactory.INSTANCE };

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

    /** Separators inserted in arbitrary order end up ascending by key, ties on distance broken by PK. */
    @Test
    public void entriesStayOrderedByKey() throws HyracksDataException {
        VTreeMetadataFrame frame = newFrame();
        double[] distances = { 5.0, 1.0, 3.0, 3.0, 2.0, 4.0 };
        int[] pks = { 50, 10, 31, 30, 20, 40 };
        for (int i = 0; i < distances.length; i++) {
            insertEntry(frame, distances[i], pks[i], 100 + i);
        }

        Assert.assertEquals(distances.length, frame.getTupleCount());
        assertOrdered(frame);
        Assert.assertEquals(1.0, separatorDistance(frame, 0), 0.0);
        Assert.assertEquals(5.0, separatorDistance(frame, frame.getTupleCount() - 1), 0.0);
        Assert.assertEquals("the lower PK sorts first among equal distances", 30, separatorPk(frame, 2));
        Assert.assertEquals(31, separatorPk(frame, 3));
    }

    /** A separator's data-page pointer travels with its key, not with its arrival position. */
    @Test
    public void eachEntryKeepsItsOwnDataPagePointer() throws HyracksDataException {
        VTreeMetadataFrame frame = newFrame();
        insertEntry(frame, 9.0, 9, 900);
        insertEntry(frame, 1.0, 1, 100);
        insertEntry(frame, 5.0, 5, 500);

        Assert.assertEquals(100, frame.getDataPagePointer(0));
        Assert.assertEquals(500, frame.getDataPagePointer(1));
        Assert.assertEquals(900, frame.getDataPagePointer(2));
    }

    /** replaceSeparator rewrites one separator's key in place, at its position, and touches no other entry. */
    @Test
    public void replaceSeparatorRewritesOnlyThatEntry() throws HyracksDataException {
        VTreeMetadataFrame frame = newFrame();
        insertEntry(frame, 1.0, 1, 100);
        insertEntry(frame, 5.0, 5, 500);

        Assert.assertTrue(frame.replaceSeparator(0, key(2.5, 2), frame.getDataPagePointer(0)));

        Assert.assertEquals(2, frame.getTupleCount());
        Assert.assertEquals(2.5, separatorDistance(frame, 0), 0.0);
        Assert.assertEquals(2, separatorPk(frame, 0));
        Assert.assertEquals("the pointer must be the one passed back in", 100, frame.getDataPagePointer(0));
        Assert.assertEquals(5.0, separatorDistance(frame, 1), 0.0);
        Assert.assertEquals(500, frame.getDataPagePointer(1));
    }

    /** A split moves the upper half out; both halves stay ordered and nothing is lost. */
    @Test
    public void splitLeavesBothHalvesOrderedAndLosesNothing() throws HyracksDataException {
        VTreeMetadataFrame left = newFrame();
        int inserted = fillToCapacity(left);
        Assert.assertTrue("need several entries for a split to be meaningful", inserted >= 4);

        VTreeMetadataFrame right = newFrame();
        left.split(right, metadataTuple(inserted / 2.0, 0, 7777));

        Assert.assertEquals("split must not lose or duplicate entries", inserted + 1,
                left.getTupleCount() + right.getTupleCount());
        assertOrdered(left);
        assertOrdered(right);
        Assert.assertTrue("the left half must not reach past the right half's first entry", left.getTupleCount() == 0
                || right.getTupleCount() == 0 || compareSeparators(left, left.getTupleCount() - 1, right, 0) <= 0);
    }

    /**
     * Neither half carries a next-page pointer out of a split. The caller captures the successor first, then
     * links the left half to the new page and the new page to the old successor.
     */
    @Test
    public void splitDoesNotPreserveTheNextPagePointer() throws HyracksDataException {
        VTreeMetadataFrame left = newFrame();
        int inserted = fillToCapacity(left);
        left.setNextPage(4242);
        Assert.assertEquals("precondition: the page has a successor", 4242, left.getNextPage());

        VTreeMetadataFrame right = newFrame();
        left.split(right, metadataTuple(inserted / 2.0, 0, 7777));

        Assert.assertEquals("split resets the split page's successor, so the caller must have captured it",
                VTreeDataTupleAccessor.NO_NEXT_PAGE, left.getNextPage());
        Assert.assertEquals("and the new half does not inherit it either", VTreeDataTupleAccessor.NO_NEXT_PAGE,
                right.getNextPage());
    }

    /** A split keeps the page's level in both halves. */
    @Test
    public void splitPreservesTheLevel() throws HyracksDataException {
        VTreeMetadataFrame left = newFrame((byte) 3);
        int inserted = fillToCapacity(left);
        VTreeMetadataFrame right = newFrame((byte) 0);

        left.split(right, metadataTuple(inserted / 2.0, 0, 7777));

        Assert.assertEquals("the split page keeps its level", 3, left.getLevel());
        Assert.assertEquals("and stamps it into the new half", 3, right.getLevel());
    }

    /**
     * A separator is a data key plus a pointer, so with a fixed-width PK it stays far below half a page and the
     * frame needs no oversize guard. A variable-length PK is bounded by the data path's maximum tuple size, since
     * the separator copies a key a data page already holds.
     */
    @Test
    public void aDirectoryEntryCannotApproachHalfAPage() throws HyracksDataException {
        VTreeMetadataFrame frame = newFrame();
        int entryBytes = frame.getTupleWriter().bytesRequired(metadataTuple(1.0, 1, 1)) + frame.getSlotSize();
        int usable = PAGE_SIZE - frame.getPageHeaderSize();

        Assert.assertTrue(
                "a directory entry (" + entryBytes + " bytes) must stay far below half a page (" + (usable / 2) + ")",
                entryBytes * 4 < usable / 2);
    }

    private VTreeMetadataFrame newFrame() throws HyracksDataException {
        return newFrame((byte) 0);
    }

    private VTreeMetadataFrame newFrame(byte level) throws HyracksDataException {
        IVTreeMetadataFrame frame = (IVTreeMetadataFrame) new VTreeMetadataFrameFactory(DIMENSIONS, KEY_TYPE_TRAITS,
                KEY_CMP_FACTORIES, null, null).createFrame();
        ICachedPage page = bufferCache.confiscatePage(IBufferCache.INVALID_DPID);
        confiscated.add(page);
        frame.setPage(page);
        frame.initBuffer(level);
        frame.setNextPage(VTreeDataTupleAccessor.NO_NEXT_PAGE);
        return (VTreeMetadataFrame) frame;
    }

    private int fillToCapacity(VTreeMetadataFrame frame) throws HyracksDataException {
        int count = 0;
        while (true) {
            ITupleReference tuple = metadataTuple(count + 1.0, count, 100 + count);
            int spaceNeeded = frame.getTupleWriter().bytesRequired(tuple) + frame.getSlotSize();
            if (spaceNeeded > frame.getTotalFreeSpace()) {
                return count;
            }
            insertEntry(frame, count + 1.0, count, 100 + count);
            count++;
        }
    }

    private static void insertEntry(VTreeMetadataFrame frame, double distance, int pk, int dataPageId)
            throws HyracksDataException {
        frame.insert(metadataTuple(distance, pk, dataPageId), frame.findInsertPosition(key(distance, pk)));
    }

    private static ITupleReference key(double distance, int pk) throws HyracksDataException {
        return TupleUtils.createTuple(KEY_SERDES, distance, pk);
    }

    private static ITupleReference metadataTuple(double distance, int pk, int dataPageId) throws HyracksDataException {
        return VTreeMetadataTupleAccessor.createMetadataTuple(key(distance, pk), dataPageId);
    }

    private static ITreeIndexTupleReference separator(VTreeMetadataFrame frame, int index) {
        ITreeIndexTupleReference tuple = frame.createTupleReference();
        tuple.resetByTupleIndex(frame, index);
        return tuple;
    }

    private static double separatorDistance(VTreeMetadataFrame frame, int index) {
        ITreeIndexTupleReference tuple = separator(frame, index);
        return DoublePointable.getDouble(tuple.getFieldData(0), tuple.getFieldStart(0));
    }

    private static int separatorPk(VTreeMetadataFrame frame, int index) {
        ITreeIndexTupleReference tuple = separator(frame, index);
        return IntegerPointable.getInteger(tuple.getFieldData(1), tuple.getFieldStart(1));
    }

    /** Orders two separators the way the frame does: by distance, then by PK. */
    private static int compareSeparators(VTreeMetadataFrame a, int i, VTreeMetadataFrame b, int j) {
        int byDistance = Double.compare(separatorDistance(a, i), separatorDistance(b, j));
        return byDistance != 0 ? byDistance : Integer.compare(separatorPk(a, i), separatorPk(b, j));
    }

    private static void assertOrdered(VTreeMetadataFrame frame) {
        for (int i = 1; i < frame.getTupleCount(); i++) {
            Assert.assertTrue("directory out of order at " + i + ": <" + separatorDistance(frame, i) + ", "
                    + separatorPk(frame, i) + "> before <" + separatorDistance(frame, i - 1) + ", "
                    + separatorPk(frame, i - 1) + ">", compareSeparators(frame, i - 1, frame, i) <= 0);
        }
    }
}
