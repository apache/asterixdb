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
package org.apache.asterix.column.bytes.stream.in;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.asterix.column.zero.writers.DefaultColumnPageZeroWriter;
import org.apache.asterix.column.zero.writers.SparseColumnPageZeroWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.IntPairUtil;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.tuples.ColumnMultiPageZeroBufferProvider;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public final class MultiPageZeroByteBuffersReader {
    private static final ByteBuffer EMPTY;
    private final IntList notRequiredSegmentsIndexes;
    private ColumnMultiPageZeroBufferProvider bufferProvider;
    private final Int2IntMap segmentDir; // should I just create a buffer[numberOfSegments] instead?
    private int maxBuffersSize;

    static {
        EMPTY = ByteBuffer.allocate(0);
        EMPTY.limit(0);
    }

    private final List<ByteBuffer> buffers;

    public MultiPageZeroByteBuffersReader() {
        this.buffers = new ArrayList<>();
        segmentDir = new Int2IntOpenHashMap();
        notRequiredSegmentsIndexes = new IntArrayList();
        segmentDir.defaultReturnValue(-1);
    }

    public void reset(IColumnBufferProvider pageZeroBufferProvider) throws HyracksDataException {
        reset();
        this.bufferProvider = (ColumnMultiPageZeroBufferProvider) pageZeroBufferProvider;
        maxBuffersSize = bufferProvider.getNumberOfRemainingPages();
        bufferProvider.readAll(buffers, segmentDir);
    }

    public void read(int segmentIndex, IPointable pointable, int position, int length)
            throws EOFException, HyracksDataException {
        if (segmentIndex < 0 || segmentIndex >= maxBuffersSize) {
            throw new IndexOutOfBoundsException("Buffer index out of bounds: " + segmentIndex);
        }

        int bufferIndex = segmentDir.get(segmentIndex);
        if (bufferIndex == -1) {
            //Fill up the buffer
            // this page was not pinned, because of the DefaultReadContext, as the pages were expected to be in disk.
            // so read the required segment, and fill the buffer.
            ByteBuffer buffer = bufferProvider.read(segmentIndex);
            segmentDir.put(segmentIndex, buffers.size());
            bufferIndex = buffers.size();
            buffers.add(buffer);
        }
        ByteBuffer buffer = buffers.get(bufferIndex);
        pointable.set(buffer.array(), position, length);
    }

    public int readOffset(long[] offsetColumnIndexPairs, int maxColumnsInZerothSegment, int numberOfColumnsInAPage,
            int currentColumnIndex) {
        int numberOfColumns = offsetColumnIndexPairs.length - 1;
        for (Int2IntMap.Entry pair : segmentDir.int2IntEntrySet()) {
            int segmentIndex = pair.getIntKey();
            int bufferIndex = pair.getIntValue();
            ByteBuffer buffer = buffers.get(bufferIndex);
            int columnIndex = maxColumnsInZerothSegment + segmentIndex * numberOfColumnsInAPage;
            int segmentOffset = 0;
            for (int j = 0; j < numberOfColumnsInAPage; j++) {
                int columnOffset = buffer.getInt(segmentOffset);
                offsetColumnIndexPairs[currentColumnIndex] = IntPairUtil.of(columnOffset, columnIndex);
                segmentOffset += DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
                currentColumnIndex++;
                columnIndex++;
                if (columnIndex == numberOfColumns) {
                    break; // No need to read more columns from this buffer.
                }
            }
        }
        return currentColumnIndex;
    }

    public int readSparseOffset(long[] offsetColumnIndexPairs, int numberOfPageSegments, int numberOfColumnsInAPage,
            int numberOfColumnsInLastSegment, int currentColumnIndex) {
        for (Int2IntMap.Entry pair : segmentDir.int2IntEntrySet()) {
            int segmentIndex = pair.getIntKey();
            int bufferIndex = pair.getIntValue();
            ByteBuffer buffer = buffers.get(bufferIndex);
            int segmentOffset = 0;
            int numberOfColumnsInSegment =
                    segmentIndex == numberOfPageSegments - 2 ? numberOfColumnsInLastSegment : numberOfColumnsInAPage;
            for (int j = 0; j < numberOfColumnsInSegment; j++) {
                int columnIndex = buffer.getInt(segmentOffset);
                int columnOffset = buffer.getInt(segmentOffset + Integer.BYTES);
                offsetColumnIndexPairs[currentColumnIndex++] = IntPairUtil.of(columnOffset, columnIndex);
                segmentOffset += SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
            }
        }
        return currentColumnIndex;
    }

    public void readAllColumns(BitSet presentColumns, int numberOfPageSegments, int numberOfColumnsInAPage,
            int numberOfColumnsInLastSegment) {
        final int stride = SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
        final int lastSegmentIndex = numberOfPageSegments - 2;

        for (Int2IntMap.Entry entry : segmentDir.int2IntEntrySet()) {
            final int segmentIndex = entry.getIntKey();
            final int bufferIndex = entry.getIntValue();
            final ByteBuffer buffer = buffers.get(bufferIndex);

            final int columnsInSegment =
                    (segmentIndex == lastSegmentIndex) ? numberOfColumnsInLastSegment : numberOfColumnsInAPage;

            int offset = 0;
            int limit = columnsInSegment * stride;

            while (offset < limit) {
                presentColumns.set(buffer.getInt(offset));
                offset += stride;
            }
        }
    }

    public void unPinNotRequiredSegments(BitSet pageZeroSegmentsPages, int numberOfPageZeroSegments)
            throws HyracksDataException {
        if (numberOfPageZeroSegments <= 1) {
            // If there is only one segment, it is always pinned.
            // So no need to unpin the segments.
            return;
        }
        notRequiredSegmentsIndexes.clear();
        // Start checking from index 1 (0th segment is always pinned)
        int i = pageZeroSegmentsPages.nextClearBit(1);
        while (i >= 1 && i < numberOfPageZeroSegments) {
            int segmentIndex = i - 1; // Adjusted index for segmentDir

            int bufferIndex = segmentDir.get(segmentIndex);
            if (bufferIndex != -1) {
                buffers.set(bufferIndex, EMPTY);
                notRequiredSegmentsIndexes.add(bufferIndex);
                segmentDir.remove(segmentIndex);
            }

            i = pageZeroSegmentsPages.nextClearBit(i + 1);
        }
        // Unpin the buffers that are not required anymore.
        bufferProvider.releasePages(notRequiredSegmentsIndexes);
    }

    public int findColumnIndexInSegment(int segmentIndex, int columnIndex, int numberOfColumnsInSegment)
            throws HyracksDataException {
        if (segmentIndex < 0 || segmentIndex >= maxBuffersSize) {
            throw new IndexOutOfBoundsException("Buffer index out of bounds: " + segmentIndex);
        }
        int bufferIndex = segmentDir.get(segmentIndex);
        if (bufferIndex == -1) {
            //Fill up the buffer
            // this page was not pinned, because of the DefaultReadContext, as the pages were expected to be in disk.
            // so read the required segment, and fill the buffer.
            ByteBuffer buffer = bufferProvider.read(segmentIndex);
            segmentDir.put(segmentIndex, buffers.size());
            bufferIndex = buffers.size();
            buffers.add(buffer);
        }
        ByteBuffer buffer = buffers.get(bufferIndex);
        int start = 0;
        int end = numberOfColumnsInSegment - 1;
        while (start <= end) {
            int mid = start + (end - start) / 2;
            int midColumnIndex = buffer.getInt(mid * SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE);
            if (midColumnIndex == columnIndex) {
                return mid; // found the column index
            } else if (midColumnIndex < columnIndex) {
                start = mid + 1;
            } else {
                end = mid - 1;
            }
        }

        return -1;
    }

    public void reset() {
        buffers.clear();
        maxBuffersSize = 0;
        segmentDir.clear();
    }
}
