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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.frames.OrderedSlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.vector.api.IVTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.utils.VTreeMetadataTupleAccessor;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * VTree metadata frame.
 * <p>
 * Page layout: base header (from {@link VTreeNSMFrame}) followed by a 4-byte next-metadata-page pointer
 * (sentinel {@code -1}). Entries are {@code <key fields..., data_page_pointer>} and are kept sorted by
 * the key ascending, so a search compares a key tuple against an entry's leading fields with the same
 * comparator that orders data pages and the trailing pointer is never visited.
 */
public class VTreeMetadataFrame extends VTreeNSMFrame implements IVTreeMetadataFrame {

    // Offset (bytes from page start) of the 4-byte next-metadata-page pointer.
    private static final int NEXT_PAGE_OFFSET = CENTROID_ID_OFFSET + Integer.BYTES;

    public VTreeMetadataFrame(ITreeIndexTupleWriter tupleWriter, IBinaryComparatorFactory[] keyCmpFactories) {
        super(tupleWriter, new OrderedSlotManager());
        // Same comparators the data frames order pages by, so a separator and the page it describes
        // cannot be ordered differently.
        setMultiComparator(MultiComparator.create(keyCmpFactories));
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

    /**
     * Compares the separator at {@code tupleIndex} against {@code key}. The separator's leading fields
     * are the key of the last record on the page it points at, so the comparator reads those and stops.
     *
     * @return negative if that page's range sorts entirely before the key, positive if after, zero if
     *         the key is exactly that page's maximum
     */
    @Override
    public int compareSeparatorToKey(int tupleIndex, ITupleReference key) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        return cmp.compare(frameTuple, key);
    }

    @Override
    public int getDataPagePointer(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        return VTreeMetadataTupleAccessor.getDataPagePointer(frameTuple);
    }

    /**
     * Replaces the separator at {@code tupleIndex} with one carrying {@code key}, keeping its position,
     * and reports whether it fitted. A full-width separator is variable length, so a replacement can be
     * wider than what it replaces and a nearly-full page cannot always absorb it; on {@code false}
     * nothing has been touched and the caller must make room. Position is the caller's to justify: a
     * post-split lowering stays above its predecessor.
     */
    @Override
    public boolean replaceSeparator(int tupleIndex, ITupleReference key, int dataPageId) throws HyracksDataException {
        ITupleReference entry = VTreeMetadataTupleAccessor.createMetadataTuple(key, dataPageId);
        frameTuple.resetByTupleIndex(this, tupleIndex);
        int reclaimed = tupleWriter.bytesRequired(frameTuple);
        if (tupleWriter.bytesRequired(entry) > getTotalFreeSpace() + reclaimed) {
            return false;
        }
        delete(entry, tupleIndex);
        // delete() returns the bytes to the free-space total without moving FREE_SPACE_OFFSET back, so
        // the reclaimed run is fragmented; compact before writing a replacement that may be wider.
        if (hasSpaceInsert(entry) != FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE) {
            compact();
        }
        insert(entry, tupleIndex);
        return true;
    }

    /** Removes the separator at {@code tupleIndex}, freeing its bytes. */
    @Override
    public void deleteSeparator(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        delete(frameTuple, tupleIndex);
    }

    /** Binary-search the leftmost insertion index that keeps the entries key-ascending. */
    @Override
    public int findInsertPosition(ITupleReference key) throws HyracksDataException {
        int left = 0;
        int right = getTupleCount();
        while (left < right) {
            int mid = (left + right) >>> 1;
            if (compareSeparatorToKey(mid, key) < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    /**
     * Split a full metadata frame into two halves and insert {@code tuple} into whichever side preserves the
     * {@code max_distance}-ascending invariant. Uses a copy-and-reinitialize strategy (rather than in-place
     * deletes) to avoid page fragmentation.
     * <p>
     * Both halves are re-initialized, which resets their next-page pointers to the end-of-chain sentinel. Re-linking
     * the two halves into the directory chain — and, crucially, preserving this page's original successor as the
     * right half's successor — is the caller's responsibility (see
     * {@code VTree.handleMetadataPageOverflow}); {@link #getNextPage()} must therefore be read <em>before</em>
     * calling this method.
     */
    public void split(IVTreeMetadataFrame rightFrame, ITupleReference tuple) throws HyracksDataException {
        int tupleCount = getTupleCount();
        int splitIndex = tupleCount / 2;

        // Snapshot existing tuples (copyTuple returns an ArrayTupleReference compatible with tupleWriter.writeTuple).
        ITupleReference[] leftTuples = new ITupleReference[splitIndex];
        for (int i = 0; i < splitIndex; i++) {
            frameTuple.resetByTupleIndex(this, i);
            leftTuples[i] = TupleUtils.copyTuple(frameTuple);
        }
        ITupleReference[] rightTuples = new ITupleReference[tupleCount - splitIndex];
        for (int i = splitIndex; i < tupleCount; i++) {
            frameTuple.resetByTupleIndex(this, i);
            rightTuples[i - splitIndex] = TupleUtils.copyTuple(frameTuple);
        }

        // Preserve this frame's level in both halves rather than hardcoding 0: a directory page is not
        // necessarily at level 0, and initBuffer() is what stamps the level into the page header.
        byte level = getLevel();
        initBuffer(level);
        rightFrame.initBuffer(level);

        for (int i = 0; i < leftTuples.length; i++) {
            insert(leftTuples[i], i);
        }
        for (int i = 0; i < rightTuples.length; i++) {
            rightFrame.insert(rightTuples[i], i);
        }

        insertIntoHalf(rightFrame, tuple);
    }

    /**
     * Insert {@code tuple} into this frame or {@code rightFrame}, whichever half's key range covers it.
     * Valid only after {@link #split} while both halves are still latched.
     */
    public void insertIntoHalf(IVTreeMetadataFrame rightFrame, ITupleReference tuple) throws HyracksDataException {
        // Both sides are separators, so the key comparator reads their leading fields and ignores the
        // trailing pointers.
        if (getTupleCount() == 0 || cmp.compare(tuple, getSeparator(getTupleCount() - 1)) <= 0) {
            insert(tuple, findInsertPosition(tuple));
        } else {
            rightFrame.insert(tuple, ((VTreeMetadataFrame) rightFrame).findInsertPosition(tuple));
        }
    }

    /** The separator at {@code tupleIndex}; the view is valid until the next call on this frame. */
    private ITupleReference getSeparator(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        return frameTuple;
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder(super.printHeader());
        strBuilder.append("nextPage:          ").append(getNextPage()).append('\n');
        return strBuilder.toString();
    }
}
