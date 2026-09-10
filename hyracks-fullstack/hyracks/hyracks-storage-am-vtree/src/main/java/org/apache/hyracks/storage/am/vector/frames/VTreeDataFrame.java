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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.storage.am.btree.api.ITupleAcceptor;
import org.apache.hyracks.storage.am.btree.frames.OrderedSlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * VTree data frame implementation.
 * <p>
 * Page layout: base header (from {@link VTreeNSMFrame}) followed by a 4-byte next-data-page pointer
 * (sentinel {@code -1}). Tuples are kept sorted by the ordering key, which the caller names through
 * {@code comparatorFields}; this frame does not interpret the fields it is given beyond field 0 being
 * the distance it reports. See {@code VTreeDataTupleAccessor} for the tuple shape.
 */
public class VTreeDataFrame extends VTreeNSMFrame implements IVTreeDataFrame {

    // Offset (in bytes from page start) of the 4-byte next-page pointer.
    private static final int NEXT_PAGE_OFFSET = CENTROID_ID_OFFSET + Integer.BYTES;

    /**
     * The stored side of a comparison, projected onto the ordering key. The projection is the
     * {@code comparatorFields} the caller supplied, so this frame needs no notion of what those fields
     * mean beyond field 0 being the distance it reports.
     */
    private final PermutingTupleReference storedKey;

    /** A caller-supplied stored-layout tuple projected onto the same fields, for {@link #split}. */
    private final PermutingTupleReference probeKey;

    /**
     * Whether a stored tuple may be overwritten by a same-key write. Injected from the LSM layer so
     * this frame needs no notion of deletion polarity, and {@code null} where nothing replaces.
     */
    private final ITupleAcceptor replaceAcceptor;

    public VTreeDataFrame(ITreeIndexTupleWriter tupleWriter, int[] comparatorFields,
            IBinaryComparatorFactory[] keyCmpFactories, ITupleAcceptor replaceAcceptor) {
        super(tupleWriter, new OrderedSlotManager());
        this.storedKey = new PermutingTupleReference(comparatorFields);
        this.probeKey = new PermutingTupleReference(comparatorFields);
        this.replaceAcceptor = replaceAcceptor;
        // Fills the inherited ITreeIndexFrame comparator slot, which is how a BTree frame is told its key.
        setMultiComparator(MultiComparator.create(keyCmpFactories));
    }

    /**
     * Whether the tuple at {@code tupleIndex} may be overwritten by a same-key write. False when no
     * predicate was supplied, which keeps such callers on the append-only path.
     */
    public boolean isReplaceable(int tupleIndex) {
        if (replaceAcceptor == null) {
            return false;
        }
        frameTuple.resetByTupleIndex(this, tupleIndex);
        return replaceAcceptor.accept(frameTuple);
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(NEXT_PAGE_OFFSET, -1);
    }

    @Override
    public int getPageHeaderSize() {
        return NEXT_PAGE_OFFSET + Integer.BYTES;
    }

    @Override
    public void setNextPage(int nextPage) {
        buf.putInt(NEXT_PAGE_OFFSET, nextPage);
    }

    @Override
    public int getNextPage() {
        return buf.getInt(NEXT_PAGE_OFFSET);
    }

    @Override
    public double getDistanceToCentroid(int tupleIndex) {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // First field of a data tuple is the raw double distance (no type tag).
        int distanceOff = frameTuple.getFieldStart(VTreeDataTupleAccessor.DISTANCE_FIELD);
        return buf.getDouble(distanceOff);
    }

    /**
     * Projects a stored-layout tuple onto the ordering key, for handing straight back to
     * {@link #findInsertPosition} or {@link #findTupleByKey}. The frame owns the projection, so a
     * caller holding a data tuple never needs to know which of its fields make up the key. The
     * returned view is valid until the next call.
     */
    @Override
    public ITupleReference keyOf(ITupleReference storedTuple) {
        probeKey.reset(storedTuple);
        return probeKey;
    }

    /**
     * Compares the tuple at {@code tupleIndex} against {@code key}, which must be in key layout. Sole
     * authority for the page's ordering, so an insert position and a lookup probe cannot disagree.
     *
     * @return negative if the stored tuple sorts before the key, positive if after, zero if equal
     */
    private int compareStoredToKey(int tupleIndex, ITupleReference key) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        storedKey.reset(frameTuple);
        return cmp.compare(storedKey, key);
    }

    /**
     * Position at which {@code key} belongs. A plain lower bound suffices because the key is unique
     * within a component: an insert replaces an existing entry for the same key instead of appending,
     * so there is no run of equal keys to position within.
     */
    @Override
    public int findInsertPosition(ITupleReference key) throws HyracksDataException {
        int left = 0;
        int right = getTupleCount();
        while (left < right) {
            int mid = (left + right) >>> 1;
            if (compareStoredToKey(mid, key) < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    /**
     * Point search for the tuple whose key is exactly {@code key}, or {@code -1}. One binary search
     * suffices because at most one entry per key exists in a component.
     */
    public int findTupleByKey(ITupleReference key) throws HyracksDataException {
        int index = findInsertPosition(key);
        if (index < getTupleCount() && compareStoredToKey(index, key) == 0) {
            return index;
        }
        return -1;
    }

    /**
     * The ordering key of the tuple at {@code tupleIndex}, projected in place. Callers building a
     * directory separator read the last tuple's key through this. The view is valid until the next call
     * on this frame, so a caller keeping it past that must copy it.
     */
    @Override
    public ITupleReference keyAt(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        return keyOf(frameTuple);
    }

    /**
     * Split this frame into {@code this} (left) and {@code rightFrame} (right) and insert {@code tuple} into
     * the half that covers its key. The split point is the tuple at which the accumulated bytes reach half
     * the page, and that tuple goes to the side the new one does not, so the receiving half holds under
     * half a page and any tuple within {@link #getMaxTupleSize} fits, as in BTreeNSMLeafFrame.split.
     */
    @Override
    public void split(IVTreeDataFrame rightFrameArg, ITupleReference tuple) throws HyracksDataException {
        VTreeDataFrame rightFrame = (VTreeDataFrame) rightFrameArg;
        int tupleCount = getTupleCount();
        int slotSize = slotManager.getSlotSize();
        int halfPage = (buf.capacity() - getPageHeaderSize()) / 2;

        // The caller splits only for a tuple within getMaxTupleSize that does not fit, so the live bytes
        // exceed half the page and the loop always stops at a tuple.
        int boundary;
        int bytesToLeft = 0;
        for (boundary = 0; boundary < tupleCount; boundary++) {
            frameTuple.resetByTupleIndex(this, boundary);
            bytesToLeft += tupleWriter.getCopySpaceRequired(frameTuple) + slotSize;
            if (bytesToLeft >= halfPage) {
                break;
            }
        }
        // The boundary tuple goes to the side the new tuple does not, so the receiving half keeps room for it.
        probeKey.reset(tuple);
        int tuplesToLeft;
        VTreeDataFrame targetFrame;
        if (compareStoredToKey(boundary, probeKey) <= 0) {
            tuplesToLeft = boundary + 1;
            targetFrame = rightFrame;
        } else {
            tuplesToLeft = boundary;
            targetFrame = this;
        }
        int tuplesToRight = tupleCount - tuplesToLeft;

        // Mirror entire page buffer into right frame, then shift its right-half slot range left.
        ByteBuffer rightBuffer = rightFrame.getBuffer();
        System.arraycopy(buf.array(), 0, rightBuffer.array(), 0, buf.capacity());

        int src = rightFrame.getSlotManager().getSlotEndOff();
        int dest = rightFrame.getSlotManager().getSlotEndOff() + tuplesToLeft * slotSize;
        int length = slotSize * tuplesToRight;
        System.arraycopy(rightBuffer.array(), src, rightBuffer.array(), dest, length);

        rightBuffer.putInt(Constants.TUPLE_COUNT_OFFSET, tuplesToRight);
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, tuplesToLeft);

        rightFrame.compact();
        this.compact();

        // compact() and the slot shift do not touch probeKey, which still views the new tuple.
        targetFrame.insert(tuple, targetFrame.findInsertPosition(probeKey));
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder(super.printHeader());
        strBuilder.append("nextPage:          ").append(getNextPage()).append('\n');
        return strBuilder.toString();
    }
}
