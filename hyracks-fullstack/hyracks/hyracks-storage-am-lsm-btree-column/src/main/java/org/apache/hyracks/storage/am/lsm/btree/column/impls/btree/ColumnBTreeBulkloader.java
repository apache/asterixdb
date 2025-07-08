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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTreeNSMBulkLoader;
import org.apache.hyracks.storage.am.btree.impls.BTreeSplitKey;
import org.apache.hyracks.storage.am.common.api.ISplitKey;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.impls.NodeFrontier;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnWriteContext;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.JSONUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ObjectNode;

public final class ColumnBTreeBulkloader extends BTreeNSMBulkLoader implements IColumnWriteMultiPageOp {
    private static final Logger LOGGER = LogManager.getLogger();
    private final List<CachedPage> columnsPages;
    private final List<ICachedPage> pageZeroSegments; // contains from 1st segment to the last segment of page0
    private final List<CachedPage> tempConfiscatedPages;
    private final ColumnBTreeWriteLeafFrame columnarFrame;
    private final AbstractColumnTupleWriter columnWriter;
    private final ISplitKey lowKey;
    private final IColumnWriteContext columnWriteContext;
    private final int maxColumnsInPageZerothSegment;
    private final IColumnPageZeroWriter.ColumnPageZeroWriterType pageZeroWriterType;
    private boolean setLowKey;
    private int tupleCount;

    // For logging
    private int numberOfLeafNodes;
    private int numberOfPagesInCurrentLeafNode;
    private int maxNumberOfPagesForAColumn;
    private int maxNumberOfPageZeroSegments; // Exclude the zeroth segment
    private int maxNumberOfPagesInALeafNode;
    private int maxTupleCount;
    private int lastRequiredFreeSpace;

    public ColumnBTreeBulkloader(NCConfig storageConfig, float fillFactor, boolean verifyInput,
            IPageWriteCallback callback, ITreeIndex index, ITreeIndexFrame leafFrame,
            IBufferCacheWriteContext writeContext) throws HyracksDataException {
        super(fillFactor, verifyInput, callback, index, leafFrame, writeContext);
        columnsPages = new ArrayList<>();
        pageZeroSegments = new ArrayList<>();
        tempConfiscatedPages = new ArrayList<>();
        columnWriteContext = (IColumnWriteContext) writeContext;
        columnarFrame = (ColumnBTreeWriteLeafFrame) leafFrame;
        columnWriter = columnarFrame.getColumnTupleWriter();
        columnWriter.init(this);
        lowKey = new BTreeSplitKey(tupleWriter.createTupleReference());
        lowKey.getTuple().setFieldCount(cmp.getKeyFieldCount());
        setLowKey = true;

        // Writer config
        maxColumnsInPageZerothSegment = storageConfig.getStorageMaxColumnsInZerothSegment();
        pageZeroWriterType = IColumnPageZeroWriter.ColumnPageZeroWriterType
                .valueOf(storageConfig.getStoragePageZeroWriter().toUpperCase());

        // For logging. Starts with 1 for page0
        numberOfPagesInCurrentLeafNode = 1;
        maxNumberOfPagesForAColumn = 0;
        maxNumberOfPagesInALeafNode = 0;
        maxNumberOfPageZeroSegments = 0;
        numberOfLeafNodes = 1;
        maxTupleCount = 0;
        lastRequiredFreeSpace = 0;
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        if (isFull(tuple)) {
            writeFullLeafPage();
            confiscateNewLeafPage();
        }
        if (tupleCount == 0) {
            //Since we are writing the first tuple, we need to estimate the number of columns.
            columnWriter.updateColumnMetadataForCurrentTuple(tuple);
        }
        //Save the key of the last inserted tuple
        setMinMaxKeys(tuple);
        columnWriter.writeTuple(tuple);
        tupleCount++;
    }

    @Override
    protected ITreeIndexTupleReference createTupleReference() {
        return tupleWriter.createTupleReference();
    }

    private boolean isFull(ITupleReference tuple) throws HyracksDataException {
        if (tupleCount == 0) {
            columnWriter.updateColumnMetadataForCurrentTuple(tuple);
            // this is for non-adaptive case.
            columnWriter.setWriterType(pageZeroWriterType);
            return false;
        } else if (tupleCount >= columnWriter.getMaxNumberOfTuples()) {
            //We reached the maximum number of tuples
            return true;
        }
        //Columns' Offsets
        columnWriter.updateColumnMetadataForCurrentTuple(tuple);
        int requiredFreeSpace = columnWriter.getPageZeroWriterOccupiedSpace(maxColumnsInPageZerothSegment,
                columnarFrame.getBuffer().capacity(), true, pageZeroWriterType);
        //Occupied space from previous writes
        requiredFreeSpace += columnWriter.getPrimaryKeysEstimatedSize();
        //min and max tuples' sizes
        requiredFreeSpace += lowKey.getTuple().getTupleSize() + getSplitKeySize(tuple);
        //New tuple required space
        requiredFreeSpace += columnWriter.bytesRequired(tuple);
        lastRequiredFreeSpace = requiredFreeSpace;
        return bufferCache.getPageSize() <= requiredFreeSpace;
    }

    private void setMinMaxKeys(ITupleReference tuple) {
        //Set max key
        setSplitKey(splitKey, tuple);
        if (setLowKey) {
            setSplitKey(lowKey, tuple);
            setLowKey = false;
        }
    }

    @Override
    public void end() throws HyracksDataException {
        if (tupleCount > 0) {
            splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer().array(), 0);
            try {
                columnarFrame.flush(columnWriter, tupleCount, maxColumnsInPageZerothSegment, lowKey.getTuple(),
                        splitKey.getTuple(), this);
            } catch (Exception e) {
                logState(e);
                throw e;
            }
        }
        columnWriter.close();
        //We are done, return any temporary confiscated pages
        for (ICachedPage page : tempConfiscatedPages) {
            bufferCache.returnPage(page, false);
        }

        // For logging
        int numberOfTempConfiscatedPages = tempConfiscatedPages.size();
        tempConfiscatedPages.clear();
        //Where Page0 and columns pages will be written
        super.end();

        log("Finished", numberOfTempConfiscatedPages);
    }

    @Override
    protected void writeFullLeafPage() throws HyracksDataException {
        NodeFrontier leafFrontier = nodeFrontiers.get(0);
        splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer().array(), 0);
        splitKey.setLeftPage(leafFrontier.pageId);
        if (tupleCount > 0) {
            //We need to flush columns to confiscate all columns pages first before calling propagateBulk
            try {
                columnarFrame.flush(columnWriter, tupleCount, maxColumnsInPageZerothSegment, lowKey.getTuple(),
                        splitKey.getTuple(), this);
            } catch (Exception e) {
                logState(e);
                throw e;
            }
        }

        propagateBulk(1, pagesToWrite);

        //Take a page for the next leaf
        leafFrontier.pageId = freePageManager.takePage(metaFrame);
        columnarFrame.setNextLeaf(leafFrontier.pageId);

        /*
         * Write columns' pages first to ensure they (columns' pages) are written before pageZero.
         * It ensures pageZero does not land in between columns' pages if compression is enabled
         */
        writeColumnAndSegmentPages();
        //Then write page0
        write(leafFrontier.page);

        //Write interior nodes after writing columns pages
        for (ICachedPage c : pagesToWrite) {
            write(c);
        }

        // For logging
        maxNumberOfPagesInALeafNode = Math.max(maxNumberOfPagesInALeafNode, numberOfPagesInCurrentLeafNode);
        maxTupleCount = Math.max(maxTupleCount, tupleCount);
        // Starts with 1 for page0
        numberOfPagesInCurrentLeafNode = 1;
        numberOfLeafNodes++;

        // Clear for next page
        pagesToWrite.clear();
        splitKey.setRightPage(leafFrontier.pageId);
        setLowKey = true;
        tupleCount = 0;
    }

    @Override
    protected void writeLastLeaf(ICachedPage page) throws HyracksDataException {
        /*
         * Write columns' pages first to ensure they (columns' pages) are written before pageZero.
         * It ensures pageZero does not land in between columns' pages if compression is enabled
         */
        writeColumnAndSegmentPages();
        super.writeLastLeaf(page);
    }

    private void writeColumnAndSegmentPages() throws HyracksDataException {
        for (ICachedPage c : columnsPages) {
            write(c);
        }

        // For logging
        int numberOfPagesInPersistedColumn = columnsPages.size();
        maxNumberOfPagesForAColumn = Math.max(maxNumberOfPagesForAColumn, numberOfPagesInPersistedColumn);
        numberOfPagesInCurrentLeafNode += numberOfPagesInPersistedColumn;
        columnsPages.clear();

        // persist page zero segments from 1 to the last segment
        for (ICachedPage page : pageZeroSegments) {
            write(page);
        }

        int numberOfPageZeroSegments = pageZeroSegments.size();
        maxNumberOfPageZeroSegments = Math.max(maxNumberOfPageZeroSegments, numberOfPageZeroSegments);
        pageZeroSegments.clear();

        // Indicate to the columnWriteContext that all columns were persisted
        columnWriteContext.columnsPersisted();
    }

    private void writeColumnsPages() throws HyracksDataException {
        for (ICachedPage c : columnsPages) {
            write(c);
        }

        // For logging
        int numberOfPagesInPersistedColumn = columnsPages.size();
        maxNumberOfPagesForAColumn = Math.max(maxNumberOfPagesForAColumn, numberOfPagesInPersistedColumn);
        numberOfPagesInCurrentLeafNode += numberOfPagesInPersistedColumn;

        columnsPages.clear();
        // Indicate to the columnWriteContext that all columns were persisted
        columnWriteContext.columnsPersisted();
    }

    @Override
    public void abort() throws HyracksDataException {
        for (ICachedPage page : columnsPages) {
            bufferCache.returnPage(page, false);
        }

        for (ICachedPage page : pageZeroSegments) {
            bufferCache.returnPage(page, false);
        }

        for (ICachedPage page : tempConfiscatedPages) {
            bufferCache.returnPage(page, false);
        }
        super.abort();

        // For logging
        log("Aborted", tempConfiscatedPages.size());
    }

    private void setSplitKey(ISplitKey splitKey, ITupleReference tuple) {
        int splitKeySize = getSplitKeySize(tuple);
        splitKey.initData(splitKeySize);
        tupleWriter.writeTupleFields(tuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer().array(), 0);
        splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer().array(), 0);
    }

    private int getSplitKeySize(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple, 0, cmp.getKeyFieldCount());
    }

    private void log(String status, int numberOfTempConfiscatedPages) {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }

        LOGGER.debug(
                "{} columnar bulkloader wrote maximum {} and last {} and used leafNodes: {}, tempPagesAllocated: {}, maxPagesPerColumn: {}, and maxLeafNodePages: {}",
                status, maxTupleCount, tupleCount, numberOfLeafNodes, numberOfTempConfiscatedPages,
                maxNumberOfPagesForAColumn, maxNumberOfPagesInALeafNode);
    }

    /*
     * ***********************************************************
     * IColumnWriteMultiPageOp
     * ***********************************************************
     */

    @Override
    public ByteBuffer confiscatePersistent() throws HyracksDataException {
        int pageId = freePageManager.takePage(metaFrame);
        long dpid = BufferedFileHandle.getDiskPageId(fileId, pageId);
        CachedPage page = (CachedPage) bufferCache.confiscatePage(dpid);
        columnsPages.add(page);
        return page.getBuffer();
    }

    @Override
    public ByteBuffer confiscatePageZeroPersistent() throws HyracksDataException {
        int pageId = freePageManager.takePage(metaFrame);
        long dpid = BufferedFileHandle.getDiskPageId(fileId, pageId);
        CachedPage page = (CachedPage) bufferCache.confiscatePage(dpid);
        pageZeroSegments.add(page);
        return page.getBuffer();
    }

    @Override
    public void persist() throws HyracksDataException {
        writeColumnsPages();
    }

    @Override
    public int getNumberOfPersistentBuffers() {
        return columnsPages.size();
    }

    @Override
    public ByteBuffer confiscateTemporary() throws HyracksDataException {
        CachedPage page = (CachedPage) bufferCache.confiscatePage(IBufferCache.INVALID_DPID);
        tempConfiscatedPages.add(page);
        return page.getBuffer();
    }

    private void logState(Exception e) {
        try {
            ObjectNode state = JSONUtil.createObject();
            // number of tuples processed for the current leaf
            state.put("currentLeafTupleCount", tupleCount);
            // number of columns
            state.put("currentLeafColumnCount", columnWriter.getAbsoluteNumberOfColumns(false));
            // number of columns including current tuple
            state.put("currentColumnCount", columnWriter.getAbsoluteNumberOfColumns(true));
            state.put("lastRequiredFreeSpace", lastRequiredFreeSpace);
            state.put("splitKeyTupleSize", splitKey.getTuple().getTupleSize());
            state.put("splitKeyTupleSizeByTupleWriter", tupleWriter.bytesRequired(splitKey.getTuple()));
            state.put("lowKeyTupleSize", lowKey.getTuple().getTupleSize());
            ObjectNode bufNode = state.putObject("leafBufferDetails");
            columnarFrame.dumpBuffer(bufNode);
            LOGGER.error("pageZero flush failed {}", state, e);
        } catch (Exception ex) {
            e.addSuppressed(ex);
        }
    }

}
