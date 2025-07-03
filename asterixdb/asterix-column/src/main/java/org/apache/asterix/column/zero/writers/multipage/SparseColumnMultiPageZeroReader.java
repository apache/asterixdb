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
package org.apache.asterix.column.zero.writers.multipage;

import static org.apache.asterix.column.zero.writers.multipage.DefaultColumnMultiPageZeroWriter.MAX_COLUMNS_IN_ZEROTH_SEGMENT;
import static org.apache.asterix.column.zero.writers.multipage.DefaultColumnMultiPageZeroWriter.NUMBER_OF_PAGE_ZERO_SEGMENTS_OFFSET;
import static org.apache.asterix.column.zero.writers.multipage.SparseColumnMultiPageZeroWriter.MAX_COLUMNS_INDEX_IN_ZEROTH_SEGMENT_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.LEFT_MOST_KEY_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.MEGA_LEAF_NODE_LENGTH;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.NEXT_LEAF_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.NUMBER_OF_COLUMNS_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.RIGHT_MOST_KEY_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.TUPLE_COUNT_OFFSET;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.asterix.column.zero.readers.SparseColumnPageZeroReader;
import org.apache.asterix.column.zero.writers.SparseColumnPageZeroWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.IntPairUtil;
import org.apache.hyracks.storage.am.lsm.btree.column.error.ColumnarValueException;

import com.fasterxml.jackson.databind.node.ObjectNode;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class SparseColumnMultiPageZeroReader extends AbstractColumnMultiPageZeroReader {
    private final SparseColumnPageZeroReader zerothSegmentReader;
    private final int maxNumberOfColumnsInAPage;
    private final BitSet pageZeroSegmentsPages;
    private final Int2IntOpenHashMap columnIndexToRelativeColumnIndex;

    private int maxColumnIndexInZerothSegment;
    private int numberOfColumnInZerothSegment;
    private int numberOfPageZeroSegments;
    private int headerSize;
    private ByteBuffer pageZeroBuf;

    private final VoidPointable offsetPointable;

    public SparseColumnMultiPageZeroReader(int bufferCapacity) {
        super();
        zerothSegmentReader = new SparseColumnPageZeroReader();
        this.pageZeroSegmentsPages = new BitSet();
        this.maxNumberOfColumnsInAPage =
                SparseColumnMultiPageZeroWriter.getMaximumNumberOfColumnsInAPage(bufferCapacity);
        this.offsetPointable = new VoidPointable();
        this.columnIndexToRelativeColumnIndex = new Int2IntOpenHashMap();
        columnIndexToRelativeColumnIndex.defaultReturnValue(-1);
    }

    @Override
    public void resetStream(IColumnBufferProvider pageZeroSegmentBufferProvider) throws HyracksDataException {
        segmentBuffers.reset(pageZeroSegmentBufferProvider);
    }

    @Override
    public void reset(ByteBuffer pageZeroBuf) {
        this.pageZeroBuf = pageZeroBuf;
        numberOfPageZeroSegments = pageZeroBuf.getInt(NUMBER_OF_PAGE_ZERO_SEGMENTS_OFFSET);
        numberOfColumnInZerothSegment = pageZeroBuf.getInt(MAX_COLUMNS_IN_ZEROTH_SEGMENT);
        maxColumnIndexInZerothSegment = pageZeroBuf.getInt(MAX_COLUMNS_INDEX_IN_ZEROTH_SEGMENT_OFFSET);
        headerSize = MAX_COLUMNS_INDEX_IN_ZEROTH_SEGMENT_OFFSET + numberOfPageZeroSegments * Integer.BYTES;
        zerothSegmentReader.reset(pageZeroBuf, Math.min(numberOfColumnInZerothSegment, getNumberOfPresentColumns()),
                headerSize);
        columnIndexToRelativeColumnIndex.clear();
    }

    @Override
    public void reset(ByteBuffer pageZeroBuf, int headerSize) {
        throw new UnsupportedOperationException("This method is not supported for multi-page zero readers.");
    }

    @Override
    public int getColumnOffset(int columnIndex) throws HyracksDataException {
        try {
            if (columnIndex <= maxColumnIndexInZerothSegment) {
                return zerothSegmentReader.getColumnOffset(columnIndex);
            } else {
                int segmentIndex = findSegment(columnIndex) - 1;
                int relativeColumnIndex = findRelativeColumnIndex(columnIndex);
                int columnIndexInRequiredSegment =
                        (relativeColumnIndex - numberOfColumnInZerothSegment) % maxNumberOfColumnsInAPage;
                int segmentOffset =
                        columnIndexInRequiredSegment * SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE + Integer.BYTES; // skipping 4 bytes of columnIndex
                segmentBuffers.read(segmentIndex, offsetPointable, segmentOffset,
                        SparseColumnPageZeroWriter.COLUMN_INDEX_SIZE);
                return IntegerPointable.getInteger(offsetPointable.getByteArray(), offsetPointable.getStartOffset());
            }
        } catch (EOFException e) {
            throw HyracksDataException.create(e);
        }
    }

    private int findSegment(int columnIndex) {
        // This method finds the segment index (except for 0th segment) for the given columnIndex.
        if (numberOfPageZeroSegments == 1) {
            // only zeroth segment is present
            return 0;
        }
        // gives 0 based segment index (0 for zeroth segment, 1 for first segment, etc.)
        int start = 1;
        int end = numberOfPageZeroSegments - 1;
        int resultSegment = -1;
        while (start <= end) {
            int mid = (start + end) / 2;
            int segmentColumnIndex =
                    pageZeroBuf.getInt(MAX_COLUMNS_INDEX_IN_ZEROTH_SEGMENT_OFFSET + mid * Integer.BYTES);
            if (segmentColumnIndex >= columnIndex) {
                resultSegment = mid;
                end = mid - 1; // continue searching in the left half
            } else {
                start = mid + 1;
            }
        }
        return resultSegment;
    }

    private int findRelativeColumnIndex(int columnIndex) throws HyracksDataException {
        if (columnIndexToRelativeColumnIndex.get(columnIndex) != -1) {
            return columnIndexToRelativeColumnIndex.get(columnIndex);
        }
        if (columnIndex <= maxColumnIndexInZerothSegment) {
            return zerothSegmentReader.getRelativeColumnIndex(columnIndex);
        } else {
            int segmentIndex = findSegment(columnIndex);
            if (segmentIndex <= 0) {
                return -1;
            }
            segmentIndex -= 1; // Adjusting to get the segment index for the segment stream
            // Oth based segment index, hence need to check in segmentIndex - 1 th buffer
            int numberOfColumnsInSegment =
                    segmentIndex == numberOfPageZeroSegments - 2
                            ? getNumberOfPresentColumns() - numberOfColumnInZerothSegment
                                    - (numberOfPageZeroSegments - 2) * maxNumberOfColumnsInAPage
                            : maxNumberOfColumnsInAPage;
            int segmentColumnIndex =
                    segmentBuffers.findColumnIndexInSegment(segmentIndex, columnIndex, numberOfColumnsInSegment);
            if (segmentColumnIndex == -1) {
                return -1;
            }
            int relativeIndex =
                    numberOfColumnInZerothSegment + segmentIndex * maxNumberOfColumnsInAPage + segmentColumnIndex;
            columnIndexToRelativeColumnIndex.put(columnIndex, relativeIndex);
            return relativeIndex;
        }
    }

    private int findNumberOfColumnsInSegment(int segmentIndex) {
        // starts from 1st segment, not from 0th segment
        if (segmentIndex == numberOfPageZeroSegments - 2) {
            return getNumberOfPresentColumns() - numberOfColumnInZerothSegment
                    - (numberOfPageZeroSegments - 2) * maxNumberOfColumnsInAPage;
        }
        // For segments beyond the zeroth segment, we can have maximum number of columns in a page, except the last segment.
        return maxNumberOfColumnsInAPage;
    }

    @Override
    public long getColumnFilterMin(int columnIndex) throws HyracksDataException {
        try {
            if (columnIndex <= maxColumnIndexInZerothSegment) {
                return zerothSegmentReader.getColumnFilterMin(columnIndex);
            } else {
                int segmentIndex = findSegment(columnIndex) - 1;
                int relativeColumnIndex = findRelativeColumnIndex(columnIndex);
                int columnIndexInRequiredSegment =
                        (relativeColumnIndex - numberOfColumnInZerothSegment) % maxNumberOfColumnsInAPage;
                int segmentOffset =
                        findNumberOfColumnsInSegment(segmentIndex) * SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
                segmentOffset += columnIndexInRequiredSegment * SparseColumnPageZeroWriter.FILTER_SIZE;
                segmentBuffers.read(segmentIndex, offsetPointable, segmentOffset, Long.BYTES);
                return LongPointable.getLong(offsetPointable.getByteArray(), offsetPointable.getStartOffset());
            }
        } catch (EOFException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public long getColumnFilterMax(int columnIndex) throws HyracksDataException {
        try {
            if (columnIndex <= maxColumnIndexInZerothSegment) {
                return zerothSegmentReader.getColumnFilterMax(columnIndex);
            } else {
                int segmentIndex = findSegment(columnIndex) - 1;
                int relativeColumnIndex = findRelativeColumnIndex(columnIndex);
                int columnIndexInRequiredSegment =
                        (relativeColumnIndex - numberOfColumnInZerothSegment) % maxNumberOfColumnsInAPage;
                int segmentOffset =
                        findNumberOfColumnsInSegment(segmentIndex) * SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
                segmentOffset += columnIndexInRequiredSegment * SparseColumnPageZeroWriter.FILTER_SIZE;
                segmentOffset += Long.BYTES; // skip min filter
                segmentBuffers.read(segmentIndex, offsetPointable, segmentOffset, Long.BYTES);
                return LongPointable.getLong(offsetPointable.getByteArray(), offsetPointable.getStartOffset());
            }
        } catch (EOFException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void skipFilters() {
        zerothSegmentReader.skipFilters();
    }

    @Override
    public void skipColumnOffsets() {
        zerothSegmentReader.skipColumnOffsets();
    }

    @Override
    public int getTupleCount() {
        return pageZeroBuf.getInt(TUPLE_COUNT_OFFSET);
    }

    @Override
    public int getLeftMostKeyOffset() {
        return pageZeroBuf.getInt(LEFT_MOST_KEY_OFFSET);
    }

    @Override
    public int getRightMostKeyOffset() {
        return pageZeroBuf.getInt(RIGHT_MOST_KEY_OFFSET);
    }

    @Override
    public int getNumberOfPresentColumns() {
        return pageZeroBuf.getInt(NUMBER_OF_COLUMNS_OFFSET);
    }

    @Override
    public int getRelativeColumnIndex(int columnIndex) throws HyracksDataException {
        return findRelativeColumnIndex(columnIndex);
    }

    @Override
    public int getNextLeaf() {
        return pageZeroBuf.getInt(NEXT_LEAF_OFFSET);
    }

    @Override
    public int getMegaLeafNodeLengthInBytes() {
        return pageZeroBuf.getInt(MEGA_LEAF_NODE_LENGTH);
    }

    @Override
    public int getPageZeroCapacity() {
        return pageZeroBuf.capacity();
    }

    @Override
    public boolean isValidColumn(int columnIndex) throws HyracksDataException {
        return findRelativeColumnIndex(columnIndex) != -1;
    }

    @Override
    public void getAllColumns(BitSet presentColumns) {
        int columnOffsetStart = headerSize;
        for (int i = 0; i < Math.min(getNumberOfPresentColumns(), numberOfColumnInZerothSegment); i++) {
            int columnIndex = pageZeroBuf.getInt(columnOffsetStart);
            presentColumns.set(columnIndex);
            columnOffsetStart += SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
        }
        if (getNumberOfPresentColumns() > numberOfColumnInZerothSegment) {
            // read the rest of the columns from the segment stream
            int columnsInLastSegment = getNumberOfPresentColumns() - numberOfColumnInZerothSegment
                    - (numberOfPageZeroSegments - 2) * maxNumberOfColumnsInAPage;
            segmentBuffers.readAllColumns(presentColumns, numberOfPageZeroSegments, maxNumberOfColumnsInAPage,
                    columnsInLastSegment);
        }
    }

    @Override
    public ByteBuffer getPageZeroBuf() {
        throw new UnsupportedOperationException("This method is not supported for multi-page zero readers.");
    }

    @Override
    public int populateOffsetColumnIndexPairs(long[] offsetColumnIndexPairs) {
        // offsetColumnIndexPairs >= getNumberOfPresentColumns() + 1 (maybe because of the previous MegaLeaf).
        // Do not rely on offsetColumnIndexPairs.length, as it may be larger than the number of present columns.
        // This is because the same array is reused for multiple leaf segments, and previous leaves may have more columns.
        int columnOffsetStart = headerSize;
        int currentColumnIndex = 0;
        int numberOfColumns = getNumberOfPresentColumns();
        while (currentColumnIndex < Math.min(numberOfColumns, numberOfColumnInZerothSegment)) {
            int columnIndex = pageZeroBuf.getInt(columnOffsetStart);
            int columnOffset = pageZeroBuf.getInt(columnOffsetStart + SparseColumnPageZeroWriter.COLUMN_INDEX_SIZE);
            offsetColumnIndexPairs[currentColumnIndex++] = IntPairUtil.of(columnOffset, columnIndex);
            columnOffsetStart += SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
        }

        // If the pages are not pinned, we will not read any columnIndex, but the old stuffs will already be present in the offsetColumnIndexPairs.
        if (numberOfColumns > numberOfColumnInZerothSegment) {
            // read the rest of the columns from the segment stream
            int columnsInLastSegment = getNumberOfPresentColumns() - numberOfColumnInZerothSegment
                    - (numberOfPageZeroSegments - 2) * maxNumberOfColumnsInAPage;
            currentColumnIndex = segmentBuffers.readSparseOffset(offsetColumnIndexPairs, numberOfPageZeroSegments,
                    maxNumberOfColumnsInAPage, columnsInLastSegment, currentColumnIndex);
        }

        return currentColumnIndex;
    }

    @Override
    public int getNumberOfPageZeroSegments() {
        return numberOfPageZeroSegments;
    }

    @Override
    public BitSet getPageZeroSegmentsPages() {
        return pageZeroSegmentsPages;
    }

    @Override
    public int getHeaderSize() {
        return headerSize;
    }

    @Override
    public BitSet markRequiredPageSegments(BitSet projectedColumns, int pageZeroId, boolean markAll) {
        pageZeroSegmentsPages.clear();
        // Not marking the zeroth segment
        if (numberOfPageZeroSegments == 1 || markAll) {
            // mark all segments as required
            pageZeroSegmentsPages.set(1, numberOfPageZeroSegments);
        } else {
            // Iterate over the projected columns and mark the segments that contain them
            int currentIndex = projectedColumns.nextSetBit(maxColumnIndexInZerothSegment + 1);
            while (currentIndex >= 0) {
                int rangeEnd = projectedColumns.nextClearBit(currentIndex); // exclusive
                int startSegmentIndex = findSegment(currentIndex);
                if (startSegmentIndex == -1) {
                    //This indicates that the currentIndex > MaxColumnIndex in the last segment
                    //Hence this leaf doesn't need to pin the segment for requested column ranges.

                    //We can return early as next projectedColumns next set bit will also be out of bounds.
                    break;
                }
                int endSegmentIndex = findSegment(rangeEnd - 1);
                if (endSegmentIndex == -1) {
                    //This indicates that the rangeEnd - 1 > MaxColumnIndex in the last segment
                    //but the startSegmentIndex is valid, hence we may pin to the last segment.
                    endSegmentIndex = numberOfPageZeroSegments - 1; // Last segment index
                }

                if (startSegmentIndex <= endSegmentIndex) {
                    pageZeroSegmentsPages.set(startSegmentIndex, endSegmentIndex + 1);
                }

                currentIndex = projectedColumns.nextSetBit(rangeEnd);
            }
        }
        return pageZeroSegmentsPages;
    }

    @Override
    public void unPinNotRequiredPageZeroSegments() throws HyracksDataException {
        segmentBuffers.unPinNotRequiredSegments(pageZeroSegmentsPages, numberOfPageZeroSegments);
    }

    @Override
    public void printPageZeroReaderInfo() {
        ColumnarValueException ex = new ColumnarValueException();
        ObjectNode readerNode = ex.createNode(getClass().getSimpleName());
        readerNode.put("headerSize", headerSize);
        readerNode.put("maxColumnIndexInZerothSegment", maxColumnIndexInZerothSegment);
        readerNode.put("numberOfColumnInZerothSegment", numberOfColumnInZerothSegment);
        readerNode.put("maxNumberOfColumnsInAPage", maxNumberOfColumnsInAPage);
        readerNode.put("numberOfPageZeroSegments", numberOfPageZeroSegments);
        LOGGER.debug("SparseColumnMultiPageZeroReader Info: {}", readerNode.toPrettyString());
    }
}
