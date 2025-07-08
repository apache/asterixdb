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

import static org.apache.asterix.column.zero.writers.multipage.DefaultColumnMultiPageZeroWriter.EXTENDED_HEADER_SIZE;
import static org.apache.asterix.column.zero.writers.multipage.DefaultColumnMultiPageZeroWriter.MAX_COLUMNS_IN_ZEROTH_SEGMENT;
import static org.apache.asterix.column.zero.writers.multipage.DefaultColumnMultiPageZeroWriter.NUMBER_OF_PAGE_ZERO_SEGMENTS_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.LEFT_MOST_KEY_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.MEGA_LEAF_NODE_LENGTH;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.NEXT_LEAF_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.NUMBER_OF_COLUMNS_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.RIGHT_MOST_KEY_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.TUPLE_COUNT_OFFSET;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.asterix.column.zero.readers.DefaultColumnPageZeroReader;
import org.apache.asterix.column.zero.writers.DefaultColumnPageZeroWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.IntPairUtil;
import org.apache.hyracks.storage.am.lsm.btree.column.error.ColumnarValueException;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class DefaultColumnMultiPageZeroReader extends AbstractColumnMultiPageZeroReader {
    public static final int headerSize = DefaultColumnMultiPageZeroWriter.EXTENDED_HEADER_SIZE;
    private final DefaultColumnPageZeroReader zerothSegmentReader;

    private final int maxNumberOfColumnsInAPage;
    private final BitSet pageZeroSegmentsPages;
    private int zerothSegmentMaxColumns;
    private int numberOfPageZeroSegments; // includes the zeroth segment
    private ByteBuffer pageZeroBuf;

    private final VoidPointable offsetPointable;

    public DefaultColumnMultiPageZeroReader(int bufferCapacity) {
        super();
        zerothSegmentReader = new DefaultColumnPageZeroReader();
        this.pageZeroSegmentsPages = new BitSet();
        this.maxNumberOfColumnsInAPage =
                DefaultColumnMultiPageZeroWriter.getMaximumNumberOfColumnsInAPage(bufferCapacity);
        this.offsetPointable = new VoidPointable();
    }

    @Override
    public void resetStream(IColumnBufferProvider pageZeroSegmentBufferProvider) throws HyracksDataException {
        segmentBuffers.reset(pageZeroSegmentBufferProvider);
    }

    @Override
    public void reset(ByteBuffer pageZeroBuf) {
        this.pageZeroBuf = pageZeroBuf;
        zerothSegmentMaxColumns = pageZeroBuf.getInt(MAX_COLUMNS_IN_ZEROTH_SEGMENT);
        zerothSegmentReader.reset(pageZeroBuf, Math.min(zerothSegmentMaxColumns, getNumberOfPresentColumns()),
                headerSize);
        numberOfPageZeroSegments = pageZeroBuf.getInt(NUMBER_OF_PAGE_ZERO_SEGMENTS_OFFSET);
    }

    @Override
    public void reset(ByteBuffer pageZeroBuf, int headerSize) {
        throw new UnsupportedOperationException("This method should not be called for multi-page readers.");
    }

    @Override
    public int getColumnOffset(int columnIndex) throws HyracksDataException {
        try {
            if (columnIndex < zerothSegmentMaxColumns) {
                return zerothSegmentReader.getColumnOffset(columnIndex);
            } else {
                int segmentIndex = (columnIndex - zerothSegmentMaxColumns) / maxNumberOfColumnsInAPage;
                int columnIndexInRequiredSegment = (columnIndex - zerothSegmentMaxColumns) % maxNumberOfColumnsInAPage;
                int segmentOffset = columnIndexInRequiredSegment * DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
                segmentBuffers.read(segmentIndex, offsetPointable, segmentOffset,
                        DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE);
                return IntegerPointable.getInteger(offsetPointable.getByteArray(), offsetPointable.getStartOffset());
            }
        } catch (EOFException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public int getNumberOfPageZeroSegments() {
        return numberOfPageZeroSegments;
    }

    @Override
    public BitSet getPageZeroSegmentsPages() {
        //If pageZeroSegmentsPages is null, it means that the CloudReadContext is not being used.
        // which indicates all the segments are being read.
        return pageZeroSegmentsPages;
    }

    @Override
    public long getColumnFilterMin(int columnIndex) throws HyracksDataException {
        try {
            if (columnIndex < zerothSegmentMaxColumns) {
                return zerothSegmentReader.getColumnFilterMin(columnIndex);
            } else {
                int segmentIndex = (columnIndex - zerothSegmentMaxColumns) / maxNumberOfColumnsInAPage;
                int columnIndexInRequiredSegment = (columnIndex - zerothSegmentMaxColumns) % maxNumberOfColumnsInAPage;
                int segmentOffset =
                        findNumberOfColumnsInSegment(segmentIndex) * DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE
                                + columnIndexInRequiredSegment * DefaultColumnPageZeroWriter.FILTER_SIZE;
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
            if (columnIndex < zerothSegmentMaxColumns) {
                return zerothSegmentReader.getColumnFilterMax(columnIndex);
            } else {
                int segmentIndex = (columnIndex - zerothSegmentMaxColumns) / maxNumberOfColumnsInAPage;
                int columnIndexInRequiredSegment = (columnIndex - zerothSegmentMaxColumns) % maxNumberOfColumnsInAPage;
                int segmentOffset =
                        findNumberOfColumnsInSegment(segmentIndex) * DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE
                                + columnIndexInRequiredSegment * DefaultColumnPageZeroWriter.FILTER_SIZE;
                segmentOffset += Long.BYTES; // Move to the max value in the filter
                segmentBuffers.read(segmentIndex, offsetPointable, segmentOffset, Long.BYTES);
                return LongPointable.getLong(offsetPointable.getByteArray(), offsetPointable.getStartOffset());
            }
        } catch (EOFException e) {
            throw HyracksDataException.create(e);
        }
    }

    private int findNumberOfColumnsInSegment(int segmentIndex) {
        // starts from 1st segment, not from 0th segment
        if (segmentIndex == numberOfPageZeroSegments - 2) {
            return getNumberOfPresentColumns() - zerothSegmentMaxColumns
                    - (numberOfPageZeroSegments - 2) * maxNumberOfColumnsInAPage;
        }
        // For segments beyond the zeroth segment, we can have maximum number of columns in a page, except the last segment.
        return maxNumberOfColumnsInAPage;
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
    public int getRelativeColumnIndex(int columnIndex) {
        return columnIndex;
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
    public boolean isValidColumn(int columnIndex) {
        return columnIndex < getNumberOfPresentColumns();
    }

    @Override
    public void getAllColumns(BitSet presentColumns) {
        int numberOfColumns = getNumberOfPresentColumns();
        presentColumns.set(0, numberOfColumns);
    }

    @Override
    public ByteBuffer getPageZeroBuf() {
        return pageZeroBuf;
    }

    @Override
    public int populateOffsetColumnIndexPairs(long[] offsetColumnIndexPairs) {
        int columnOffsetStart = headerSize;
        int numberOfColumns = getNumberOfPresentColumns();
        int currentColumnIndex = 0;
        while (currentColumnIndex < Math.min(numberOfColumns, zerothSegmentMaxColumns)) {
            // search in the 0th segment
            int offset = pageZeroBuf.getInt(columnOffsetStart);
            offsetColumnIndexPairs[currentColumnIndex] = IntPairUtil.of(offset, currentColumnIndex);
            columnOffsetStart += DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
            currentColumnIndex++;
        }

        if (numberOfColumns > zerothSegmentMaxColumns) {
            // read the rest of the columns from the segment stream
            currentColumnIndex = segmentBuffers.readOffset(offsetColumnIndexPairs, zerothSegmentMaxColumns,
                    maxNumberOfColumnsInAPage, currentColumnIndex);
        }
        return currentColumnIndex;
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
            int currentIndex = projectedColumns.nextSetBit(zerothSegmentMaxColumns);
            int totalNumberOfColumns = getNumberOfPresentColumns();
            while (currentIndex >= 0 && currentIndex < totalNumberOfColumns) {
                int rangeEnd = projectedColumns.nextClearBit(currentIndex); // exclusive

                int fromSegmentIndex = (currentIndex - zerothSegmentMaxColumns) / maxNumberOfColumnsInAPage + 1;
                int toSegmentIndex = (rangeEnd - 1 - zerothSegmentMaxColumns) / maxNumberOfColumnsInAPage + 1;

                if (fromSegmentIndex <= toSegmentIndex) {
                    pageZeroSegmentsPages.set(fromSegmentIndex, toSegmentIndex + 1); // inclusive range
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
    public int getHeaderSize() {
        return EXTENDED_HEADER_SIZE;
    }

    @Override
    public void printPageZeroReaderInfo() {
        ColumnarValueException ex = new ColumnarValueException();
        ObjectNode readerNode = ex.createNode(getClass().getSimpleName());
        readerNode.put("headerSize", headerSize);
        readerNode.put("maxColumnsInZerothSegment", zerothSegmentMaxColumns);
        readerNode.put("maxNumberOfColumnsInAPage", maxNumberOfColumnsInAPage);
        readerNode.put("numberOfPageZeroSegments", numberOfPageZeroSegments);
        LOGGER.debug("DefaultColumnMultiPageZeroReader Info: {}", readerNode.toPrettyString());
    }
}
