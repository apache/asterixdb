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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.btree;

import java.util.BitSet;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReader;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public final class ColumnBTreeReadLeafFrame extends AbstractColumnBTreeLeafFrame {
    private final AbstractColumnTupleReader columnarTupleReader;
    private final ITreeIndexTupleReference leftMostTuple;
    private final ITreeIndexTupleReference rightMostTuple;
    private IColumnPageZeroReader columnPageZeroReader;

    public ColumnBTreeReadLeafFrame(ITreeIndexTupleWriter rowTupleWriter,
            AbstractColumnTupleReader columnarTupleReader) {
        super(rowTupleWriter, columnarTupleReader.getPageZeroWriterFlavorSelector());
        this.columnarTupleReader = columnarTupleReader;
        leftMostTuple = rowTupleWriter.createTupleReference();
        rightMostTuple = rowTupleWriter.createTupleReference();
    }

    @Override
    protected void resetPageZeroReader() {
        columnPageZeroReader = pageZeroWriterFlavorSelector.createPageZeroReader(getFlagByte(), buf.capacity());
        columnPageZeroReader.reset(buf);
        buf.position(columnPageZeroReader.getHeaderSize());
    }

    public void pinPageZeroSegments(int fileId, int pageZeroId, List<ICachedPage> segmentPages,
            IBufferCache bufferCache, IBufferCacheReadContext bcOpCtx) throws HyracksDataException {
        // pins all the segments, used by the column planner and sweeper
        int numberOfPageSegments = getNumberOfPageZeroSegments();
        for (int i = 1; i < numberOfPageSegments; i++) {
            long dpid = BufferedFileHandle.getDiskPageId(fileId, pageZeroId + i);
            if (bcOpCtx != null) {
                segmentPages.add(bufferCache.pin(dpid, bcOpCtx));
            } else {
                segmentPages.add(bufferCache.pin(dpid));
            }
        }
    }

    @Override
    public ITupleReference getLeftmostTuple() {
        if (getTupleCount() == 0) {
            return null;
        }

        leftMostTuple.setFieldCount(cmp.getKeyFieldCount());
        leftMostTuple.resetByTupleOffset(buf.array(), columnPageZeroReader.getLeftMostKeyOffset());
        return leftMostTuple;
    }

    @Override
    public ITupleReference getRightmostTuple() {
        if (getTupleCount() == 0) {
            return null;
        }

        rightMostTuple.setFieldCount(cmp.getKeyFieldCount());
        rightMostTuple.resetByTupleOffset(buf.array(), columnPageZeroReader.getRightMostKeyOffset());
        return rightMostTuple;
    }

    public void getAllColumns(BitSet presentColumns) {
        columnPageZeroReader.getAllColumns(presentColumns);
    }

    public IColumnTupleIterator createTupleReference(int index, IColumnReadMultiPageOp multiPageOp) {
        return columnarTupleReader.createTupleIterator(this, index, multiPageOp);
    }

    @Override
    public int getTupleCount() {
        return columnPageZeroReader.getTupleCount();
    }

    public int getPageId() {
        return BufferedFileHandle.getPageId(((CachedPage) page).getDiskPageId());
    }

    public int getNumberOfPageZeroSegments() {
        return columnPageZeroReader.getNumberOfPageZeroSegments();
    }

    public int getNumberOfColumns() {
        return columnPageZeroReader.getNumberOfPresentColumns();
    }

    public int getColumnOffset(int columnIndex) throws HyracksDataException {
        // update the exception message.
        if (!columnPageZeroReader.isValidColumn(columnIndex)) {
            printPageZeroReaderInfo();
            throw new IndexOutOfBoundsException(columnIndex + " >= " + getNumberOfColumns());
        }
        return columnPageZeroReader.getColumnOffset(columnIndex);
    }

    public boolean isValidColumn(int columnIndex) throws HyracksDataException {
        return columnPageZeroReader.isValidColumn(columnIndex);
    }

    public int getNextLeaf() {
        return columnPageZeroReader.getNextLeaf();
    }

    public int getMegaLeafNodeLengthInBytes() {
        return columnPageZeroReader.getMegaLeafNodeLengthInBytes();
    }

    public int getHeaderSize() {
        return columnPageZeroReader.getHeaderSize();
    }

    public int getPageZeroSegmentCount() {
        return columnPageZeroReader.getNumberOfPageZeroSegments();
    }

    // flag needs to be directly accessed from the buffer, as this will be used to choose the pageReader
    public byte getFlagByte() {
        return buf.get(FLAG_OFFSET);
    }

    public void skipFilters() {
        columnPageZeroReader.skipFilters();
    }

    public void skipColumnOffsets() {
        columnPageZeroReader.skipColumnOffsets();
    }

    public IColumnPageZeroReader getColumnPageZeroReader() {
        return columnPageZeroReader;
    }

    public int getMegaLeafNodeNumberOfPages() {
        // the denominator should ideally be the bufferCache pageSize, but
        // in the current way, the pageZeroCapacity = bufferCache's pageSize.
        // May be, needs to be changed in the future, to point to the bufferCache's pageSize.
        return (int) Math.ceil((double) getMegaLeafNodeLengthInBytes() / columnPageZeroReader.getPageZeroCapacity());
    }

    public ColumnBTreeReadLeafFrame createCopy() {
        return new ColumnBTreeReadLeafFrame(rowTupleWriter, columnarTupleReader);
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        throw new IllegalArgumentException("Use createTupleReference(int)");
    }

    public int populateOffsetColumnIndexPairs(long[] offsetColumnIndexPairs) {
        return columnPageZeroReader.populateOffsetColumnIndexPairs(offsetColumnIndexPairs);
    }

    public BitSet getPageZeroSegmentsPages() {
        return columnPageZeroReader.getPageZeroSegmentsPages();
    }

    public BitSet markRequiredPageZeroSegments(BitSet projectedColumns, int pageZeroId, boolean markAll) {
        return columnPageZeroReader.markRequiredPageSegments(projectedColumns, pageZeroId, markAll);
    }

    public void unPinNotRequiredPageZeroSegments() throws HyracksDataException {
        columnPageZeroReader.unPinNotRequiredPageZeroSegments();
    }

    public void printPageZeroReaderInfo() {
        columnPageZeroReader.printPageZeroReaderInfo();
    }
}
