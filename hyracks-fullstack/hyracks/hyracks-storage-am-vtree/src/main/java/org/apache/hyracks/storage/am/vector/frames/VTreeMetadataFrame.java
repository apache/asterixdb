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
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.frames.OrderedSlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.vector.api.IVTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.utils.VTreeMetadataTupleAccessor;

/**
 * VTree metadata frame.
 * <p>
 * Page layout: base header (from {@link VTreeNSMFrame}) followed by a 4-byte next-metadata-page pointer
 * (sentinel {@code -1}). Tuples have the format {@code <max_distance, data_page_pointer>} and are kept sorted
 * by {@code max_distance} ascending so that {@code findInsertPosition} and bounded scans can use binary search.
 */
public class VTreeMetadataFrame extends VTreeNSMFrame implements IVTreeMetadataFrame {

    // Offset (bytes from page start) of the 4-byte next-metadata-page pointer.
    private static final int NEXT_PAGE_OFFSET = CENTROID_ID_OFFSET + Integer.BYTES;

    public VTreeMetadataFrame(ITreeIndexTupleWriter tupleWriter) {
        super(tupleWriter, new OrderedSlotManager());
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
    public double getMaxDistance(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        return VTreeMetadataTupleAccessor.getMaxDistance(frameTuple);
    }

    @Override
    public int getDataPagePointer(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        return VTreeMetadataTupleAccessor.getDataPagePointer(frameTuple);
    }

    /**
     * Overwrite the {@code max_distance} field of an existing tuple in place. Caller must preserve the
     * sort invariant on {@code max_distance}.
     */
    public void updateMaxDistance(int tupleIndex, double newMaxDistance) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        int f = VTreeMetadataTupleAccessor.MAX_DISTANCE_FIELD;
        DoublePointable.setDouble(frameTuple.getFieldData(f), frameTuple.getFieldStart(f), newMaxDistance);
    }

    /**
     * Binary-search the leftmost insertion index that keeps {@code max_distance} sorted ascending.
     */
    public int findInsertPosition(double maxDistance) throws HyracksDataException {
        int left = 0;
        int right = getTupleCount();
        while (left < right) {
            int mid = (left + right) / 2;
            double midMaxDistance = getMaxDistance(mid);
            if (midMaxDistance < maxDistance) {
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

        initBuffer((byte) 0);
        rightFrame.initBuffer((byte) 0);

        for (int i = 0; i < leftTuples.length; i++) {
            insert(leftTuples[i], i);
        }
        for (int i = 0; i < rightTuples.length; i++) {
            rightFrame.insert(rightTuples[i], i);
        }

        // Route the new tuple to the side whose range covers its max_distance.
        double newMaxDistance = extractMaxDistanceFromTuple(tuple);
        if (getTupleCount() == 0 || newMaxDistance <= getMaxDistance(getTupleCount() - 1)) {
            insert(tuple, findInsertPosition(newMaxDistance));
        } else {
            rightFrame.insert(tuple, ((VTreeMetadataFrame) rightFrame).findInsertPosition(newMaxDistance));
        }
    }

    private double extractMaxDistanceFromTuple(ITupleReference tuple) {
        return VTreeMetadataTupleAccessor.getMaxDistance(tuple);
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder(super.printHeader());
        strBuilder.append("nextPage:          ").append(getNextPage()).append('\n');
        return strBuilder.toString();
    }
}
