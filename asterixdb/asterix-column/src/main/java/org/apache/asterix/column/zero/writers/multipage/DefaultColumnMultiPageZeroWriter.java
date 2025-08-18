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
import org.apache.asterix.column.zero.writers.DefaultColumnPageZeroWriter;
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
| MaxColumnsInPageZeroSegment                                                |

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
public class DefaultColumnMultiPageZeroWriter implements IColumnPageZeroWriter {
    // for storing max columns allowed in zeroth segment
    public static final int NUMBER_OF_PAGE_ZERO_SEGMENTS_OFFSET = HEADER_SIZE;
    public static final int MAX_COLUMNS_IN_ZEROTH_SEGMENT = HEADER_SIZE + Integer.BYTES;
    public static final int EXTENDED_HEADER_SIZE = MAX_COLUMNS_IN_ZEROTH_SEGMENT + Integer.BYTES;

    private final MultiPersistentPageZeroBufferBytesOutputStream segments;
    private final DefaultColumnPageZeroWriter zerothSegmentWriter;
    // maximum number of columns that can be laid out in the zeroth segments
    private final int zerothSegmentMaxColumns;
    private final int maximumNumberOfColumnsInAPage; // this is the maximum number of columns that can be laid out in a page

    private int numberOfColumns;
    private int numberOfColumnInZerothSegment;
    private int numberOfPageZeroSegments; // this includes the zeroth segment

    public DefaultColumnMultiPageZeroWriter(IColumnWriteMultiPageOp multiPageOp, int zerothSegmentMaxColumns,
            int bufferCapacity) {
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        multiPageOpRef.setValue(multiPageOp);
        segments = new MultiPersistentPageZeroBufferBytesOutputStream(multiPageOpRef); // should this be populated at reset?
        this.zerothSegmentWriter = new DefaultColumnPageZeroWriter();
        this.zerothSegmentMaxColumns = zerothSegmentMaxColumns;
        this.maximumNumberOfColumnsInAPage = getMaximumNumberOfColumnsInAPage(bufferCapacity);
    }

    @Override
    public void resetBasedOnColumns(int[] presentColumns, int numberOfColumns) throws HyracksDataException {
        this.numberOfColumns = numberOfColumns;
        this.numberOfColumnInZerothSegment = Math.min(numberOfColumns, zerothSegmentMaxColumns);
        this.numberOfPageZeroSegments = calculateNumberOfPageZeroSegments(numberOfColumns,
                numberOfColumnInZerothSegment, maximumNumberOfColumnsInAPage);
        zerothSegmentWriter.resetBasedOnColumns(presentColumns, numberOfColumnInZerothSegment, EXTENDED_HEADER_SIZE);
        if (numberOfPageZeroSegments > 1) {
            // these many buffers need to be allocated, to get contiguous pageIds
            segments.reset(numberOfPageZeroSegments - 1);
        }
    }

    @Override
    public void resetBasedOnColumns(int[] presentColumns, int numberOfColumns, int headerSize)
            throws HyracksDataException {
        throw new UnsupportedOperationException(
                "resetBasedOnColumns with headerSize is not supported in multi-page zero writer");
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
        // rest of the segments need not need to be allocated
        // as those are full of columns
    }

    @Override
    public void putColumnOffset(int columnIndex, int relativeColumnIndex, int offset) throws HyracksDataException {
        try {
            // for default writer, both columnIndex and relativeColumnIndex are the same
            if (columnIndex < zerothSegmentMaxColumns) {
                zerothSegmentWriter.putColumnOffset(columnIndex, relativeColumnIndex, offset);
            } else {
                // For columns beyond the zeroth segment, we need to write to the segments
                int columnIndexInSegment = columnIndex - numberOfColumnInZerothSegment;
                int requiredSegment = columnIndexInSegment / maximumNumberOfColumnsInAPage;
                int columnIndexInRequiredSegment = columnIndexInSegment % maximumNumberOfColumnsInAPage;
                int offsetInSegment = columnIndexInRequiredSegment * DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
                segments.writeInSegment(requiredSegment, offsetInSegment, offset);
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void putColumnFilter(int columnIndex, long normalizedMinValue, long normalizedMaxValue)
            throws HyracksDataException {
        try {
            if (columnIndex < zerothSegmentMaxColumns) {
                zerothSegmentWriter.putColumnFilter(columnIndex, normalizedMinValue, normalizedMaxValue);
            } else {
                // For columns beyond the zeroth segment, we need to write to the segments
                int columnIndexInSegment = columnIndex - numberOfColumnInZerothSegment;
                int requiredSegment = columnIndexInSegment / maximumNumberOfColumnsInAPage;
                int columnIndexInRequiredSegment = columnIndexInSegment % maximumNumberOfColumnsInAPage;
                int numberOfColumnsInSegment = findNumberOfColumnsInSegment(requiredSegment);
                int segmentFilterOffset = numberOfColumnsInSegment * DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
                int offsetInSegment =
                        segmentFilterOffset + columnIndexInRequiredSegment * DefaultColumnPageZeroWriter.FILTER_SIZE;
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
            return numberOfColumns - numberOfColumnInZerothSegment
                    - (numberOfPageZeroSegments - 2) * maximumNumberOfColumnsInAPage;
        }
        // For segments beyond the zeroth segment, we can have maximum number of columns in a page, except the last segment.
        return maximumNumberOfColumnsInAPage;
    }

    @Override
    public void writePrimaryKeyColumns(IValuesWriter[] primaryKeyWriters) throws HyracksDataException {
        // primary key columns are always written to the zeroth segment
        zerothSegmentWriter.writePrimaryKeyColumns(primaryKeyWriters);
    }

    @Override
    public byte flagCode() {
        return MULTI_PAGE_DEFAULT_WRITER_FLAG;
    }

    @Override
    public void flush(ByteBuffer buf, int numberOfTuples, ITupleReference minKey, ITupleReference maxKey,
            AbstractColumnTupleWriter columnWriter, ITreeIndexTupleWriter rowTupleWriter) throws HyracksDataException {
        buf.position(EXTENDED_HEADER_SIZE);
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
        // write the number of columns in the zeroth segment
        buf.putInt(MAX_COLUMNS_IN_ZEROTH_SEGMENT, zerothSegmentMaxColumns);

        // reset the collected meta info
        segments.finish();
        columnWriter.reset();
    }

    @Override
    public int getNumberOfColumns() {
        return numberOfColumns;
    }

    @Override
    public boolean includeOrderedColumn(BitSet presentColumns, int columnIndex, boolean includeChildrenColumns) {
        return true;
    }

    @Override
    public int getPageZeroBufferCapacity() {
        int pageSize = zerothSegmentWriter.getPageZeroBufferCapacity();
        return pageSize * numberOfPageZeroSegments;
    }

    @Override
    public int getRelativeColumnIndex(int columnIndex) {
        return columnIndex;
    }

    @Override
    public int getColumnOffsetsSize() {
        return numberOfColumns * DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
    }

    @Override
    public void setPageZero(ByteBuffer pageZero) {
        throw new IllegalStateException("setPageZero is not supported in multi-page zero writer");
    }

    @Override
    public int getHeaderSize() {
        return EXTENDED_HEADER_SIZE;
    }

    public static int getMaximumNumberOfColumnsInAPage(int bufferCapacity) {
        return bufferCapacity
                / (DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE + DefaultColumnPageZeroWriter.FILTER_SIZE);
    }
}
