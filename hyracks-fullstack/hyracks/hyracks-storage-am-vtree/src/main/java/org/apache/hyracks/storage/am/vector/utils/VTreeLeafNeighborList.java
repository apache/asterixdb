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
package org.apache.hyracks.storage.am.vector.utils;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.am.vector.api.VTreeStaticTupleConstants;

/**
 * Byte encoding for the graph-style neighbor list carried by a VTree leaf centroid.
 * <p>
 * The neighbor list is a variable-length byte field made of fixed {@link #ENTRY_SIZE}-byte entries.
 * An entry has the same width whether it is provisional or resolved — the invariant the two-pass
 * static-structure build depends on, so the resolution pass can overwrite each entry in place:
 * <ul>
 *   <li><b>provisional</b>: {@code [int centroidId][int SENTINEL]} — neighbor identified by centroid id,
 *       not yet resolved to a physical location.</li>
 *   <li><b>resolved</b>: {@code [int pageId][int slot]} — direct physical pointer into a leaf page.</li>
 * </ul>
 * The entry count is recoverable as {@code length / ENTRY_SIZE}; there is no count header and no cap.
 */
public final class VTreeLeafNeighborList {

    /** Bytes per neighbor entry: two 4-byte ints. */
    public static final int ENTRY_SIZE = Integer.BYTES * 2;

    /**
     * Marker stored in an entry's second int while it is provisional. A real {@code slot} is a
     * tuple index within a page and is always {@code >= 0}, so a negative marker can never collide.
     */
    public static final int SENTINEL = Integer.MIN_VALUE;

    /** Shared empty neighbor list (no neighbors). */
    public static final byte[] EMPTY = new byte[0];

    private VTreeLeafNeighborList() {
    }

    /**
     * Encode a provisional neighbor list from neighbor centroid ids. Each id becomes one
     * {@code [centroidId, SENTINEL]} entry. A {@code null} or empty input yields {@link #EMPTY}.
     */
    public static byte[] encodeProvisional(int[] neighborCentroidIds) {
        if (neighborCentroidIds == null || neighborCentroidIds.length == 0) {
            return EMPTY;
        }
        byte[] buf = new byte[neighborCentroidIds.length * ENTRY_SIZE];
        for (int i = 0; i < neighborCentroidIds.length; i++) {
            int off = i * ENTRY_SIZE;
            IntegerPointable.setInteger(buf, off, neighborCentroidIds[i]);
            IntegerPointable.setInteger(buf, off + 4, SENTINEL);
        }
        return buf;
    }

    /** Number of entries in a neighbor-list byte field. */
    public static int entryCount(byte[] buf, int start, int length) {
        return length / ENTRY_SIZE;
    }

    /** First int of an entry: the neighbor's centroid id while provisional (also the page id once resolved). */
    public static int readCentroidId(byte[] buf, int start, int entryIndex) {
        return IntegerPointable.getInteger(buf, start + entryIndex * ENTRY_SIZE);
    }

    /** First int of a resolved entry: the leaf page id. */
    public static int readPageId(byte[] buf, int start, int entryIndex) {
        return IntegerPointable.getInteger(buf, start + entryIndex * ENTRY_SIZE);
    }

    /** Second int of a resolved entry: the slot (tuple index) within the page. */
    public static int readSlot(byte[] buf, int start, int entryIndex) {
        return IntegerPointable.getInteger(buf, start + entryIndex * ENTRY_SIZE + 4);
    }

    /** True if the entry has been resolved (its second int is no longer the {@link #SENTINEL}). */
    public static boolean isResolved(byte[] buf, int start, int entryIndex) {
        return IntegerPointable.getInteger(buf, start + entryIndex * ENTRY_SIZE + 4) != SENTINEL;
    }

    /**
     * Overwrite one entry in place with its resolved physical pointer. Used by the resolution pass;
     * does not change the byte width, so surrounding tuple/slot layout is untouched.
     */
    public static void writeResolved(byte[] buf, int start, int entryIndex, int pageId, int slot) {
        int off = start + entryIndex * ENTRY_SIZE;
        IntegerPointable.setInteger(buf, off, pageId);
        IntegerPointable.setInteger(buf, off + 4, slot);
    }

    /**
     * Per-entry callback invoked by {@link #forEachLeafNeighborEntry} for every neighbor entry on a
     * leaf page. Receives the neighbor-list field bytes, the offset of the first entry within them,
     * and the entry index; the action reads/rewrites the entry in place via the {@code read*}/
     * {@link #writeResolved} accessors.
     */
    @FunctionalInterface
    public interface EntryAction {
        void apply(byte[] neighborList, int start, int entryIndex) throws HyracksDataException;
    }

    /**
     * Decode every tuple's neighbor list on the given leaf page (already set on {@code lfFrame}) and
     * invoke {@code action} once per neighbor entry. Tuples whose layout has no neighbor field
     * (field count &le; {@link VTreeStaticTupleConstants#LEAF_NEIGHBOR_LIST_FIELD}{@code + 1}) are
     * skipped. This is the shared decode prologue used by the static-structure builder and the two
     * loaders; only the per-entry action differs between them.
     * <p>
     * Drives the frame's own reusable tuple reference ({@link IVTreeLeafFrame#getFrameTuple()}) rather
     * than allocating one per call; the callers set the page and do not hold a cursor position across
     * this call, and the per-entry {@code action} operates on raw page bytes, so the shared reference
     * is safe here.
     */
    public static void forEachLeafNeighborEntry(IVTreeLeafFrame lfFrame, EntryAction action)
            throws HyracksDataException {
        int tupleCount = lfFrame.getTupleCount();
        ITreeIndexTupleReference frameTuple = lfFrame.getFrameTuple();
        for (int t = 0; t < tupleCount; t++) {
            frameTuple.resetByTupleIndex(lfFrame, t);
            // Neighbor list lives at LEAF_NEIGHBOR_LIST_FIELD, before the last (metadata-pointer) field;
            // absent in leaf layouts with fewer fields.
            if (frameTuple.getFieldCount() <= VTreeStaticTupleConstants.LEAF_NEIGHBOR_LIST_FIELD + 1) {
                continue;
            }
            byte[] neighborList = frameTuple.getFieldData(VTreeStaticTupleConstants.LEAF_NEIGHBOR_LIST_FIELD);
            int fieldStart = frameTuple.getFieldStart(VTreeStaticTupleConstants.LEAF_NEIGHBOR_LIST_FIELD);
            int contentLen = ByteArrayPointable.getContentLength(neighborList, fieldStart);
            int start = fieldStart + ByteArrayPointable.getNumberBytesToStoreMeta(contentLen);
            int numEntries = contentLen / ENTRY_SIZE;
            for (int e = 0; e < numEntries; e++) {
                action.apply(neighborList, start, e);
            }
        }
    }
}
