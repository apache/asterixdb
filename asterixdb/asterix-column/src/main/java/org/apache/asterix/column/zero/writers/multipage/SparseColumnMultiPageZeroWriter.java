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

import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.FLAG_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.HEADER_SIZE;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.LEFT_MOST_KEY_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.MEGA_LEAF_NODE_LENGTH;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.NUMBER_OF_COLUMNS_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.RIGHT_MOST_KEY_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.SIZE_OF_COLUMNS_OFFSETS_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.TUPLE_COUNT_OFFSET;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.asterix.column.bytes.stream.out.MultiPersistentPageZeroBufferBytesOutputStream;
import org.apache.asterix.column.zero.writers.SparseColumnPageZeroWriter;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IValuesWriter;

/*
[ PageZero Segment 0 ]
──────────────────────────────────────────────────────────────────────────────
| Headers                                                                    |
| ───────────────────────────────────────────────────────────────────────── |
| TupleCountOffset                                                           |
| MaxColumnsInZerothSegment                                                  |
| LevelOffset                                                                |
| NumberOfColumnsOffset                                                      |
| LeftMostKeyOffset                                                          |
| RightMostKeyOffset                                                         |
| SizeOfColumnsOffsetsOffset                                                 |
| MegaLeafNodeLength                                                         |
| FlagOffset                                                                 |
| NextLeafOffset                                                             |
| NumberOfPageSegments                                                       |
| MaxColumnIndexInZerothSegment                                              |
| MaxColumnIndexInFirstSegment                                               |
| MaxColumnIndexInThirdSegment                                               |

| Min Primary Key                                                            |
| Max Primary Key                                                            |
| Primary Key Values                                                         |
| [ offset₁, min₁, max₁ ]                                                   |
| [ offset₂, min₂, max₂ ]                                                   |
| [ offset₃, min₃, max₃ ]                                                   |
| ...                                                                        |

[ PageZero Segment 1..N ]
──────────────────────────────────────────────────────────────────────────────
| Additional column metadata (same format)                                   |
*/
public class SparseColumnMultiPageZeroWriter implements IColumnPageZeroWriter {
    //For storing the last columnIndex in the ith segment
    public static final int NUMBER_OF_PAGE_ZERO_SEGMENTS_OFFSET = HEADER_SIZE;
    public static final int MAX_COLUMNS_IN_ZEROTH_SEGMENT_OFFSET = NUMBER_OF_PAGE_ZERO_SEGMENTS_OFFSET + 4;
    public static final int MAX_COLUMNS_INDEX_IN_ZEROTH_SEGMENT_OFFSET = MAX_COLUMNS_IN_ZEROTH_SEGMENT_OFFSET + 4;

    private final MultiPersistentPageZeroBufferBytesOutputStream segments;
    private final SparseColumnPageZeroWriter zerothSegmentWriter;
    private final int maximumNumberOfColumnsInAPage;
    private final int zerothSegmentMaxColumns;
    private int[] presentColumns;
    private int numberOfPresentColumns;
    private int numberOfPageZeroSegments;
    private int numberOfColumnInZerothSegment;

    public SparseColumnMultiPageZeroWriter(IColumnWriteMultiPageOp multiPageOp, int zerothSegmentMaxColumns,
            int bufferCachePageSize) {
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        multiPageOpRef.setValue(multiPageOp);
        segments = new MultiPersistentPageZeroBufferBytesOutputStream(multiPageOpRef);
        this.zerothSegmentMaxColumns = zerothSegmentMaxColumns;
        this.zerothSegmentWriter = new SparseColumnPageZeroWriter();
        this.maximumNumberOfColumnsInAPage = getMaximumNumberOfColumnsInAPage(bufferCachePageSize);
    }

    @Override
    public void resetBasedOnColumns(int[] presentColumns, int numberOfColumns) throws HyracksDataException {
        this.presentColumns = presentColumns;
        this.numberOfPresentColumns = presentColumns.length;
        this.numberOfColumnInZerothSegment = Math.min(numberOfPresentColumns, zerothSegmentMaxColumns);
        this.numberOfPageZeroSegments = calculateNumberOfPageZeroSegments(numberOfPresentColumns,
                numberOfColumnInZerothSegment, maximumNumberOfColumnsInAPage);
        int headerSize = MAX_COLUMNS_INDEX_IN_ZEROTH_SEGMENT_OFFSET + numberOfPageZeroSegments * Integer.BYTES;
        zerothSegmentWriter.resetInnerBasedOnColumns(presentColumns, numberOfColumnInZerothSegment, headerSize);
        if (numberOfPageZeroSegments > 1) {
            segments.reset(numberOfPageZeroSegments - 1);
        }
    }

    @Override
    public void resetBasedOnColumns(int[] presentColumns, int numberOfColumns, int headerSize)
            throws HyracksDataException {
        throw new UnsupportedOperationException(
                "resetBasedOnColumns with headerSize is not supported in multi-page zero writer");
    }

    @Override
    public byte flagCode() {
        return MULTI_PAGE_SPARSE_WRITER_FLAG;
    }

    private int calculateNumberOfPageZeroSegments(int numberOfColumns, int numberOfColumnInZerothSegment,
            int maximumNumberOfColumnsInAPage) {
        // calculate the number of segments required to store the columns
        int numberOfColumnsBeyondZerothSegment = numberOfColumns - numberOfColumnInZerothSegment;
        if (numberOfColumnsBeyondZerothSegment <= 0) {
            return 1; // only zeroth segment is needed
        }
        return 1 + (int) Math.ceil((double) numberOfColumnsBeyondZerothSegment / maximumNumberOfColumnsInAPage);
    }

    @Override
    public void allocateColumns() {
        // allocate the zeroth segment columns
        zerothSegmentWriter.allocateColumns();
    }

    @Override
    public void putColumnOffset(int absoluteColumnIndex, int relativeColumnIndex, int offset)
            throws HyracksDataException {
        // for sparse writer, we need to find the relative column index in the present columns.
        try {
            if (relativeColumnIndex < zerothSegmentMaxColumns) {
                // Write to the zeroth segment
                zerothSegmentWriter.putColumnOffset(absoluteColumnIndex, relativeColumnIndex, offset);
            } else {
                int columnIndexInSegment = relativeColumnIndex - numberOfColumnInZerothSegment;
                int requiredSegment = columnIndexInSegment / maximumNumberOfColumnsInAPage;
                int columnIndexInRequiredSegment = columnIndexInSegment % maximumNumberOfColumnsInAPage;
                int offsetInSegment = columnIndexInRequiredSegment * SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
                segments.writeInSegment(requiredSegment, offsetInSegment, absoluteColumnIndex, offset);
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void putColumnFilter(int relativeColumnIndex, long normalizedMinValue, long normalizedMaxValue)
            throws HyracksDataException {
        try {
            if (relativeColumnIndex < zerothSegmentMaxColumns) {
                zerothSegmentWriter.putColumnFilter(relativeColumnIndex, normalizedMinValue, normalizedMaxValue);
            } else {
                // For columns beyond the zeroth segment, we need to write to the segments
                int columnIndexInSegment = relativeColumnIndex - numberOfColumnInZerothSegment;
                int requiredSegment = columnIndexInSegment / maximumNumberOfColumnsInAPage;
                int columnIndexInRequiredSegment = columnIndexInSegment % maximumNumberOfColumnsInAPage;
                int numberOfColumnsInSegment = findNumberOfColumnsInSegment(requiredSegment);
                int segmentFilterOffset = numberOfColumnsInSegment * SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
                int offsetInSegment =
                        segmentFilterOffset + columnIndexInRequiredSegment * SparseColumnPageZeroWriter.FILTER_SIZE;
                segments.writeInSegment(requiredSegment, offsetInSegment, normalizedMinValue);
                segments.writeInSegment(requiredSegment, offsetInSegment + Long.BYTES, normalizedMaxValue);
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private int findNumberOfColumnsInSegment(int segmentIndex) {
        // starts from 1st segment, not from 0th segment
        if (segmentIndex == numberOfPageZeroSegments - 2) {
            return numberOfPresentColumns - numberOfColumnInZerothSegment
                    - (numberOfPageZeroSegments - 2) * maximumNumberOfColumnsInAPage;
        }
        // For segments beyond the zeroth segment, we can have maximum number of columns in a page, except the last segment.
        return maximumNumberOfColumnsInAPage;
    }

    @Override
    public void writePrimaryKeyColumns(IValuesWriter[] primaryKeyWriters) throws HyracksDataException {
        zerothSegmentWriter.writePrimaryKeyColumns(primaryKeyWriters);
    }

    @Override
    public int getNumberOfColumns() {
        return numberOfPresentColumns;
    }

    @Override
    public boolean includeOrderedColumn(BitSet presentColumns, int columnIndex, boolean includeChildrenColumns) {
        return zerothSegmentWriter.includeOrderedColumn(presentColumns, columnIndex, includeChildrenColumns);
    }

    @Override
    public int getPageZeroBufferCapacity() {
        int pageSize = zerothSegmentWriter.getPageZeroBufferCapacity();
        return pageSize * numberOfPageZeroSegments;
    }

    @Override
    public int getRelativeColumnIndex(int columnIndex) {
        int relativeColumnIndex =
                zerothSegmentWriter.findColumnIndex(presentColumns, numberOfPresentColumns, columnIndex);
        if (relativeColumnIndex == -1) {
            throw new IllegalStateException("Column index " + relativeColumnIndex + " is out of bounds");
        }
        return relativeColumnIndex;
    }

    @Override
    public int getColumnOffsetsSize() {
        return numberOfPresentColumns * SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
    }

    @Override
    public void setPageZero(ByteBuffer pageZero) {
        throw new IllegalStateException("setPageZero is not supported in multi-page zero writer");
    }

    @Override
    public void flush(ByteBuffer buf, int numberOfTuples, ITupleReference minKey, ITupleReference maxKey,
            AbstractColumnTupleWriter columnWriter, ITreeIndexTupleWriter rowTupleWriter) throws HyracksDataException {
        zerothSegmentWriter.setPageZero(buf);
        buf.putInt(MEGA_LEAF_NODE_LENGTH, columnWriter.flush(this));
        // Write min and max keys
        int offset = buf.position();
        buf.putInt(LEFT_MOST_KEY_OFFSET, offset);
        offset += rowTupleWriter.writeTuple(minKey, buf.array(), offset);
        buf.putInt(RIGHT_MOST_KEY_OFFSET, offset);
        rowTupleWriter.writeTuple(maxKey, buf.array(), offset);

        // Write page information
        buf.putInt(TUPLE_COUNT_OFFSET, numberOfTuples);
        buf.put(FLAG_OFFSET, flagCode());
        buf.putInt(NUMBER_OF_COLUMNS_OFFSET, getNumberOfColumns());
        buf.putInt(SIZE_OF_COLUMNS_OFFSETS_OFFSET, getColumnOffsetsSize());
        // write the number of segments
        buf.putInt(NUMBER_OF_PAGE_ZERO_SEGMENTS_OFFSET, numberOfPageZeroSegments);
        // write the max column count in the zeroth segment
        buf.putInt(MAX_COLUMNS_IN_ZEROTH_SEGMENT_OFFSET, numberOfColumnInZerothSegment);

        // write the max columnIndex in headers.
        for (int i = 0; i < numberOfPageZeroSegments; i++) {
            int columnIndexOffset = MAX_COLUMNS_INDEX_IN_ZEROTH_SEGMENT_OFFSET + i * Integer.BYTES;
            if (i == 0) {
                int presentColumnIndex = numberOfColumnInZerothSegment - 1;
                buf.putInt(columnIndexOffset, presentColumns[presentColumnIndex]);
            } else if (i == numberOfPageZeroSegments - 1) {
                buf.putInt(columnIndexOffset, presentColumns[numberOfPresentColumns - 1]);
            } else {
                int presentColumnIndex = numberOfColumnInZerothSegment + i * maximumNumberOfColumnsInAPage;
                buf.putInt(columnIndexOffset, presentColumns[presentColumnIndex - 1]);
            }
        }

        // reset the collected meta info
        segments.finish();
        columnWriter.reset();
    }

    @Override
    public int getHeaderSize() {
        return MAX_COLUMNS_INDEX_IN_ZEROTH_SEGMENT_OFFSET + numberOfPageZeroSegments * Integer.BYTES;
    }

    public static int getHeaderSpace(int numberOfExtraPagesRequired) {
        return MAX_COLUMNS_INDEX_IN_ZEROTH_SEGMENT_OFFSET + numberOfExtraPagesRequired * Integer.BYTES;
    }

    public static int getMaximumNumberOfColumnsInAPage(int bufferCachePageSize) {
        return bufferCachePageSize
                / (SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE + SparseColumnPageZeroWriter.FILTER_SIZE);
    }
}
