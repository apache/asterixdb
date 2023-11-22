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
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ColumnBTreeBulkloader extends BTreeNSMBulkLoader implements IColumnWriteMultiPageOp {
    private static final Logger LOGGER = LogManager.getLogger();
    private final List<CachedPage> columnsPages;
    private final List<CachedPage> tempConfiscatedPages;
    private final ColumnBTreeWriteLeafFrame columnarFrame;
    private final AbstractColumnTupleWriter columnWriter;
    private final ISplitKey lowKey;
    private boolean setLowKey;
    private int tupleCount;

    // For logging
    private int numberOfLeafNodes;
    private int numberOfPagesInCurrentLeafNode;
    private int maxNumberOfPagesForAColumn;
    private int maxNumberOfPagesInALeafNode;
    private int maxTupleCount;

    public ColumnBTreeBulkloader(float fillFactor, boolean verifyInput, IPageWriteCallback callback, ITreeIndex index,
            ITreeIndexFrame leafFrame) throws HyracksDataException {
        super(fillFactor, verifyInput, callback, index, leafFrame);
        columnsPages = new ArrayList<>();
        tempConfiscatedPages = new ArrayList<>();
        columnarFrame = (ColumnBTreeWriteLeafFrame) leafFrame;
        columnWriter = columnarFrame.getColumnTupleWriter();
        columnWriter.init(this);
        lowKey = new BTreeSplitKey(tupleWriter.createTupleReference());
        lowKey.getTuple().setFieldCount(cmp.getKeyFieldCount());
        setLowKey = true;

        // For logging. Starts with 1 for page0
        numberOfPagesInCurrentLeafNode = 1;
        maxNumberOfPagesForAColumn = 0;
        maxNumberOfPagesInALeafNode = 0;
        numberOfLeafNodes = 1;
        maxTupleCount = 0;
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        if (isFull(tuple)) {
            writeFullLeafPage();
            confiscateNewLeafPage();
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

    private boolean isFull(ITupleReference tuple) {
        if (tupleCount == 0) {
            return false;
        } else if (tupleCount >= columnWriter.getMaxNumberOfTuples()) {
            //We reached the maximum number of tuples
            return true;
        }
        int requiredFreeSpace = AbstractColumnBTreeLeafFrame.HEADER_SIZE;
        //Columns' Offsets
        requiredFreeSpace += columnWriter.getColumnOffsetsSize();
        //Occupied space from previous writes
        requiredFreeSpace += columnWriter.getOccupiedSpace();
        //min and max tuples' sizes
        requiredFreeSpace += lowKey.getTuple().getTupleSize() + splitKey.getTuple().getTupleSize();
        //New tuple required space
        requiredFreeSpace += columnWriter.bytesRequired(tuple);
        return bufferCache.getPageSize() <= requiredFreeSpace;
    }

    private void setMinMaxKeys(ITupleReference tuple) {
        //Set max key
        setSplitKey(splitKey, tuple);
        if (setLowKey) {
            setSplitKey(lowKey, tuple);
            lowKey.getTuple().resetByTupleOffset(lowKey.getBuffer().array(), 0);
            setLowKey = false;
        }
    }

    @Override
    public void end() throws HyracksDataException {
        if (tupleCount > 0) {
            splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer().array(), 0);
            columnarFrame.flush(columnWriter, tupleCount, this, lowKey.getTuple(), splitKey.getTuple());
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
            columnarFrame.flush(columnWriter, tupleCount, this, lowKey.getTuple(), splitKey.getTuple());
        }

        propagateBulk(1, pagesToWrite);

        //Take a page for the next leaf
        leafFrontier.pageId = freePageManager.takePage(metaFrame);
        columnarFrame.setNextLeaf(leafFrontier.pageId);

        /*
         * Write columns' pages first to ensure they (columns' pages) are written before pageZero.
         * It ensures pageZero does not land in between columns' pages if compression is enabled
         */
        writeColumnsPages();
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
        writeColumnsPages();
        super.writeLastLeaf(page);
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
    }

    @Override
    public void abort() throws HyracksDataException {
        for (ICachedPage page : columnsPages) {
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
        int splitKeySize = tupleWriter.bytesRequired(tuple, 0, cmp.getKeyFieldCount());
        splitKey.initData(splitKeySize);
        tupleWriter.writeTupleFields(tuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer().array(), 0);
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
}
