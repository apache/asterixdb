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
import java.util.Arrays;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.frames.OrderedSlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;

/**
 * VTree data frame implementation.
 * <p>
 * Page layout: base header (from {@link VTreeNSMFrame}) followed by a 4-byte next-data-page pointer
 * (sentinel {@code -1}). Tuples are kept sorted by {@code distance_to_centroid} ascending. The exact
 * tuple shape depends on whether the index is quantized:
 * <ul>
 *   <li>Non-quantized: {@code <distance_to_centroid, centroid_id, PK, included_fields>}</li>
 *   <li>Quantized:     {@code <distance_to_centroid, centroid_id, quantized_distance,
 *       quantized_embedding, PK, included_fields>} (the default in this build, since
 *       quantization is enforced at index creation; pkStartField=4 vs 2)</li>
 * </ul>
 */
public class VTreeDataFrame extends VTreeNSMFrame implements IVTreeDataFrame {

    // Offset (in bytes from page start) of the 4-byte next-page pointer.
    private static final int NEXT_PAGE_OFFSET = CENTROID_ID_OFFSET + Integer.BYTES;

    public VTreeDataFrame(ITreeIndexTupleWriter tupleWriter) {
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
    public double getDistanceToCentroid(int tupleIndex) {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // First field of a data tuple is the raw double distance (no type tag).
        int distanceOff = frameTuple.getFieldStart(VTreeDataTupleAccessor.DISTANCE_FIELD);
        return buf.getDouble(distanceOff);
    }

    /**
     * Find the insertion position for a tuple based on distance to maintain sorted order.
     * Uses RIGHT boundary search: inserts AFTER all existing tuples with the same distance.
     * This preserves temporal ordering (FIFO) for tuples with equal distances.
     */
    public int findInsertPosition(double distance) {
        int tupleCount = getTupleCount();

        // Binary search for RIGHT boundary (first tuple with distance > target)
        int left = 0;
        int right = tupleCount;

        while (left < right) {
            int mid = (left + right) / 2;
            double midDistance = getDistanceToCentroid(mid);

            if (midDistance <= distance) {
                // Include equal distances: move past them
                left = mid + 1;
            } else {
                // midDistance > distance
                right = mid;
            }
        }

        return left;
    }

    /**
     * Find tuple matching both distance and primary key using RIGHT BOUND search.
     * Returns the rightmost (most recently inserted) tuple if multiple matches exist.
     * This is critical for finding matter tuples inserted after a delete-marker tuple during deletion.
     *
     * Uses binary comparison for primary key matching - no type assumption.
     *
     * @param distance Target distance to centroid
     * @param primaryKey Primary key bytes to match (binary format)
     * @return Tuple index if found, -1 if not found
     */
    public int findTupleByDistanceAndPrimaryKey(double distance, byte[] primaryKey, int pkFieldIndex)
            throws HyracksDataException {

        // Step 1: Use RIGHT BOUND search to find upper boundary
        int upperBound = findInsertPosition(distance);

        // Step 2: Search BACKWARD from upperBound-1 to find matching PK
        // This ensures we find the RIGHTMOST (last inserted) tuple with this distance+PK
        for (int i = upperBound - 1; i >= 0; i--) {
            double dist = getDistanceToCentroid(i);

            // Stop when we reach a different distance zone
            if (dist < distance) {
                break;
            }

            // Check primary key match using binary comparison (no type assumption)
            byte[] pk = getPrimaryKey(i, pkFieldIndex);
            if (Arrays.equals(pk, primaryKey)) {
                return i; // Found the rightmost matching tuple
            }
        }

        return -1; // Not found
    }

    /**
     * Split this data frame into {@code this} (left) and {@code rightFrame} (right) and insert {@code tuple}
     * into whichever side keeps the sorted-by-distance invariant. Follows the BTreeNSMLeafFrame.split() pattern:
     * mirror the buffer, shift right-half slots, fix tuple counts, compact, then insert.
     * The {@code insertIndex} parameter is kept for API compatibility; the real insert position is recomputed
     * from the new tuple's distance after compaction.
     */
    @Override
    public void split(IVTreeDataFrame rightFrameArg, ITupleReference tuple) throws HyracksDataException {
        VTreeDataFrame rightFrame = (VTreeDataFrame) rightFrameArg;
        int tupleCount = getTupleCount();
        int tuplesToLeft = tupleCount / 2;
        int tuplesToRight = tupleCount - tuplesToLeft;

        // Mirror entire page buffer into right frame, then shift its right-half slot range left.
        ByteBuffer rightBuffer = rightFrame.getBuffer();
        System.arraycopy(buf.array(), 0, rightBuffer.array(), 0, buf.capacity());

        int src = rightFrame.getSlotManager().getSlotEndOff();
        int dest =
                rightFrame.getSlotManager().getSlotEndOff() + tuplesToLeft * rightFrame.getSlotManager().getSlotSize();
        int length = rightFrame.getSlotManager().getSlotSize() * tuplesToRight;
        System.arraycopy(rightBuffer.array(), src, rightBuffer.array(), dest, length);

        rightBuffer.putInt(Constants.TUPLE_COUNT_OFFSET, tuplesToRight);
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, tuplesToLeft);

        rightFrame.compact();
        this.compact();

        // Pick the target frame based on the new tuple's distance vs. the left frame's last (split-point) tuple.
        double newTupleDistance = extractDistanceFromTuple(tuple);
        VTreeDataFrame targetFrame;
        if (tuplesToLeft > 0) {
            double splitPointDistance = getDistanceToCentroid(tuplesToLeft - 1);
            targetFrame = (newTupleDistance <= splitPointDistance) ? this : rightFrame;
        } else {
            // Edge case: left frame is empty after split.
            targetFrame = rightFrame;
        }

        // Recompute insertion index in the target frame to honor RIGHT-boundary semantics.
        int targetTupleIndex = targetFrame.findInsertPosition(newTupleDistance);
        targetFrame.insert(tuple, targetTupleIndex);
    }

    /**
     * Extract distance from tuple (first field).
     */
    private double extractDistanceFromTuple(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(VTreeDataTupleAccessor.DISTANCE_FIELD);
        int offset = tuple.getFieldStart(VTreeDataTupleAccessor.DISTANCE_FIELD);
        return DoublePointable.getDouble(data, offset);
    }

    /**
     * Get primary key from data tuple.
     *
     * @param tupleIndex the tuple index in this frame
     * @param pkFieldIndex the field index of the primary key in the data tuple
     * @return the primary key bytes
     */
    public byte[] getPrimaryKey(int tupleIndex, int pkFieldIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);

        byte[] data = frameTuple.getFieldData(pkFieldIndex);
        int offset = frameTuple.getFieldStart(pkFieldIndex);
        int length = frameTuple.getFieldLength(pkFieldIndex);

        return Arrays.copyOfRange(data, offset, offset + length);
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder(super.printHeader());
        strBuilder.append("nextPage:          ").append(getNextPage()).append('\n');
        return strBuilder.toString();
    }
}
