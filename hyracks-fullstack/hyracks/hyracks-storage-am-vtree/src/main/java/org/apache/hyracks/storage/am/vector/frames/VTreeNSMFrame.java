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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ISlotManager;
import org.apache.hyracks.storage.am.common.api.ISplitKey;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeFrame;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

/**
 * Common NSM (N-ary Storage Model) base for VTree page frames.
 * <p>
 * Page header layout extends {@link TreeIndexNSMFrame}: after the inherited reserved header bytes come a
 * 4-byte cluster ID and a 4-byte centroid ID. Subclasses append their own header fields after
 * {@link #CENTROID_ID_OFFSET} + {@link Integer#BYTES} and override {@link #getPageHeaderSize()} accordingly.
 * <p>
 * NOTE: these two <em>per-page header</em> fields are currently <em>reserved</em>: {@code cluster_id}
 * is written as the {@code -1} sentinel and read back only by the debug {@link #printHeader()}, and
 * {@code centroid_id} is never written (also read only by {@code printHeader}). They are retained (not
 * removed) because dropping them would shift every subclass header offset — an on-disk format change
 * that is out of scope here.
 * <p>
 * This header {@code centroid_id} slot is <em>distinct</em> from the per-tuple centroid id carried in
 * each interior/leaf/data tuple (read via {@code IVTreeLeafFrame#getCentroidId(int)} /
 * {@code VTreeBulkLoader.extractCentroidId}). That per-tuple id is live and load-bearing — e.g. the
 * bulk loader compares it against the current cluster to detect a cluster boundary and route records —
 * and is not affected by the reserved status of this header slot.
 */

public abstract class VTreeNSMFrame extends TreeIndexNSMFrame implements IVTreeFrame {

    // Offset of the 4-byte cluster ID field (sentinel -1 = unassigned).
    protected static final int CLUSTER_ID_OFFSET = TreeIndexNSMFrame.RESERVED_HEADER_SIZE;
    // Offset of the 4-byte centroid ID field. Subclass headers extend from CENTROID_ID_OFFSET + Integer.BYTES.
    protected static final int CENTROID_ID_OFFSET = CLUSTER_ID_OFFSET + Integer.BYTES;

    protected MultiComparator cmp;

    public VTreeNSMFrame(ITreeIndexTupleWriter tupleWriter, ISlotManager slotManager) {
        // frameTuple is declared and initialized (tupleWriter.createTupleReference()) by TreeIndexNSMFrame;
        // no need to redeclare/shadow it here.
        super(tupleWriter, slotManager);
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(CLUSTER_ID_OFFSET, -1);
    }

    @Override
    public int getPageHeaderSize() {
        return CENTROID_ID_OFFSET + Integer.BYTES;
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        return tupleWriter.createTupleReference();
    }

    @Override
    public void setMultiComparator(MultiComparator cmp) {
        this.cmp = cmp;
    }

    @Override
    public int getBytesRequiredToWriteTuple(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple) + slotManager.getSlotSize();
    }

    @Override
    public void insertSorted(ITupleReference tuple) {
        insert(tuple, getTupleCount());
    }

    @Override
    public FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple) throws HyracksDataException {
        // One correct definition shared by all VTree frames. The former override in VTreeDataFrame was
        // the only variant ever exercised (it is the sole frame type hasSpaceInsert is called on); this
        // base version had the checks in the wrong order and mislabeled results — total (possibly
        // fragmented) free space was reported as SUFFICIENT_CONTIGUOUS_SPACE, which would let a caller
        // insert without compacting and overrun the contiguous region. Interior/leaf/metadata frames
        // inherit this, so keep it correct here rather than relying on an override.
        int bytesRequired = tupleWriter.bytesRequired(tuple);
        int slotSize = slotManager.getSlotSize();
        // Contiguous space available without compaction: gap between the tuple-data high-water mark
        // (free-space offset) and the slot array growing down from the end of the page.
        int contiguousFree = buf.capacity() - getFreeSpaceOff() - getTupleCount() * slotSize;
        if (bytesRequired + slotSize <= contiguousFree) {
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        }
        // Space available only after compacting reclaimable (deleted-tuple) free space.
        if (bytesRequired + slotSize <= buf.getInt(TOTAL_FREE_SPACE_OFFSET)) {
            return FrameOpSpaceStatus.SUFFICIENT_SPACE;
        }
        return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
    }

    @Override
    public void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey,
            IExtraPageBlockHelper extraPageBlockHelper, IBufferCache bufferCache) throws HyracksDataException {
        // Generic split is not supported on VTree frames; subclasses that split (e.g. data/metadata frames)
        // expose their own split(...) entry points with the right argument types.
        throw new HyracksDataException("Split operation not implemented for " + this.getClass().getSimpleName());
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder(super.printHeader());
        strBuilder.append("clusterId:         ").append(buf.getInt(CLUSTER_ID_OFFSET)).append('\n');
        strBuilder.append("centroidId:        ").append(buf.getInt(CENTROID_ID_OFFSET)).append('\n');
        return strBuilder.toString();
    }
}
