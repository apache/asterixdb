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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.InvertedListCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * A cursor class that traverse an inverted list that consists of fixed-size elements on disk
 *
 */
public class FixedSizeElementInvertedListCursor extends InvertedListCursor {

    private final IBufferCache bufferCache;
    private final int fileId;
    private final int elementSize;
    // for sequential scan
    private int currentElementIxForScan;
    private int currentOffsetForScan;
    private int currentPageIxForScan;
    // the whole range of the given inverted list
    private int startPageId;
    private int endPageId;
    private int startOff;
    private int numElements;
    private int numPages;
    // the current range of the loaded pages in memory
    private int bufferStartPageId;
    private int bufferEndPageId;
    private int bufferStartElementIx;
    private int bufferEndElementIx;
    private int bufferNumLoadedPages;

    private final FixedSizeTupleReference tuple;
    // The last element in the current range in memory
    private final FixedSizeTupleReference bufferEndElementTuple;
    private ICachedPage page;
    // The last element index per page
    private int[] elementIndexes = new int[10];
    // buffer manager to conform to the memory budget
    private final ISimpleFrameBufferManager bufferManagerForSearch;
    private ArrayList<ByteBuffer> buffers;
    private boolean moreBlocksToRead = true;
    // The last searched element index (used for random traversal)
    private int lastRandomSearchedElementIx;
    private final IIndexCursorStats stats;

    public FixedSizeElementInvertedListCursor(IBufferCache bufferCache, int fileId, ITypeTraits[] invListFields,
            IHyracksTaskContext ctx, IIndexCursorStats stats) throws HyracksDataException {
        this.bufferCache = bufferCache;
        this.fileId = fileId;
        int tmpSize = 0;
        for (int i = 0; i < invListFields.length; i++) {
            tmpSize += invListFields[i].getFixedLength();
        }
        elementSize = tmpSize;
        this.currentOffsetForScan = -elementSize;
        this.currentElementIxForScan = 0;
        this.currentPageIxForScan = 0;
        this.bufferStartPageId = 0;
        this.bufferEndPageId = 0;
        this.bufferStartElementIx = 0;
        this.bufferEndElementIx = 0;
        this.bufferNumLoadedPages = 0;
        this.lastRandomSearchedElementIx = 0;
        this.moreBlocksToRead = true;
        this.tuple = new FixedSizeTupleReference(invListFields);
        this.bufferEndElementTuple = new FixedSizeTupleReference(invListFields);
        this.buffers = new ArrayList<ByteBuffer>();
        if (ctx == null) {
            throw HyracksDataException.create(ErrorCode.CANNOT_CONTINUE_TEXT_SEARCH_HYRACKS_TASK_IS_NULL);
        }
        this.bufferManagerForSearch = TaskUtil.get(HyracksConstants.INVERTED_INDEX_SEARCH_FRAME_MANAGER, ctx);
        if (bufferManagerForSearch == null) {
            throw HyracksDataException.create(ErrorCode.CANNOT_CONTINUE_TEXT_SEARCH_BUFFER_MANAGER_IS_NULL);
        }
        this.stats = stats;
    }

    /**
     * Tries to allocate enough buffers to read the inverted list at once. If memory budget is not enough, this method
     * stops allocating buffers.
     */
    private void allocateBuffers() throws HyracksDataException {
        do {
            ByteBuffer tmpBuffer = bufferManagerForSearch.acquireFrame(bufferCache.getPageSize());
            if (tmpBuffer == null) {
                // Budget exhausted
                break;
            }
            Arrays.fill(tmpBuffer.array(), (byte) 0);
            buffers.add(tmpBuffer);
        } while (buffers.size() < numPages);
        // At least there should be one frame to load a page from disk.
        if (buffers.isEmpty()) {
            throw HyracksDataException.create(ErrorCode.NOT_ENOUGH_BUDGET_FOR_TEXTSEARCH,
                    FixedSizeElementInvertedListCursor.class.getName());
        }
    }

    /**
     * Deallocates all buffers. i.e. releases all buffers to the buffer manager.
     */
    private void deallocateBuffers() throws HyracksDataException {
        for (int i = 0; i < buffers.size(); i++) {
            bufferManagerForSearch.releaseFrame(buffers.get(i));
            buffers.set(i, null);
        }
        buffers.clear();
    }

    /**
     * Clears the contents of the buffers.
     */
    private void clearBuffers() throws HyracksDataException {
        for (int i = 0; i < buffers.size(); i++) {
            Arrays.fill(buffers.get(i).array(), (byte) 0);
            buffers.get(i).clear();
        }
    }

    /**
     * Checks whether there are more elements to return. This is usually used for a sequential scan.
     */
    @Override
    public boolean doHasNext() {
        return currentElementIxForScan < numElements;
    }

    /**
     * Returns the next element.
     */
    @Override
    public void doNext() throws HyracksDataException {
        if (currentOffsetForScan + 2 * elementSize > bufferCache.getPageSize()) {
            currentPageIxForScan++;
            currentOffsetForScan = 0;
        } else {
            currentOffsetForScan += elementSize;
        }

        // Needs to read the next block?
        if (currentElementIxForScan > bufferEndElementIx && endPageId > bufferEndPageId) {
            loadPages();
            currentOffsetForScan = 0;
        }

        currentElementIxForScan++;

        tuple.reset(buffers.get(currentPageIxForScan).array(), currentOffsetForScan);
    }

    /**
     * Prepares buffers to load pages. This method should not be called during the open()
     * since it tries to allocate all available frames. If there are multiple concurrently opened
     * cursors (e.g., a partitioned inverted index), this will cause an issue. An assumption of this cursor is
     * that no two cursors are accessed at the same time even though they can be opened together.
     */
    @Override
    public void prepareLoadPages() throws HyracksDataException {
        // Resets the buffers if there is any.
        clearBuffers();
        if (numPages > buffers.size()) {
            allocateBuffers();
        }
    }

    /**
     * Reads a part of the inverted list into the working memory via the buffer cache.
     * This method reads the inverted list until it fills the current buffers.
     */
    @Override
    public void loadPages() throws HyracksDataException {
        // Conducts a load. Based on the size of the buffers, it may be possible to read the entire list.
        // Resets the start page ID to load. At this moment, the variable bufferEndPageId holds
        // the last page ID where the previous loadPages() stopped.
        bufferStartPageId = bufferEndPageId + 1;
        if (bufferStartPageId > endPageId) {
            return;
        }
        int currentBufferIdx = 0;
        ByteBuffer tmpBuffer;
        for (int i = bufferStartPageId; i <= endPageId; i++) {
            page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, i), false);
            stats.getPageCounter().update(1);
            // Copies the content to the buffer (working memory).
            // Assumption: processing inverted list takes time; so, we don't want to keep them on the buffer cache.
            // Rather, we utilize the assigned working memory (buffers).
            tmpBuffer = page.getBuffer();

            // Copies the entire content of the page to the current buffer in the working memory.
            System.arraycopy(tmpBuffer.array(), 0, buffers.get(currentBufferIdx).array(), 0,
                    buffers.get(currentBufferIdx).capacity());
            buffers.get(currentBufferIdx).position(buffers.get(currentBufferIdx).capacity());

            currentBufferIdx++;
            bufferCache.unpin(page);
            bufferEndPageId = i;

            // Buffer full?
            if (currentBufferIdx >= buffers.size()) {
                break;
            }
        }
        setBlockInfo();
    }

    /**
     * Updates the information about this block.
     */
    private void setBlockInfo() {
        bufferNumLoadedPages = bufferEndPageId - bufferStartPageId + 1;
        bufferStartElementIx =
                bufferStartPageId == startPageId ? 0 : elementIndexes[bufferStartPageId - startPageId - 1] + 1;
        lastRandomSearchedElementIx = bufferStartElementIx;
        bufferEndElementIx = elementIndexes[bufferEndPageId - startPageId];
        // Gets the final element tuple in this block.
        getElementAtIndex(bufferEndElementIx, bufferEndElementTuple);
        currentPageIxForScan = 0;
        currentOffsetForScan = bufferStartElementIx == 0 ? startOff - elementSize : -elementSize;
        if (bufferEndPageId == endPageId) {
            moreBlocksToRead = false;
        }
    }

    /**
     * Unloads the pages from the buffers (working memory). This will release all buffers.
     */
    @Override
    public void unloadPages() throws HyracksDataException {
        // Deallocates the buffer pages
        deallocateBuffers();
    }

    /**
     * Checks whether the search tuple is greater than the last element in the current block of the cursor.
     * If so, the cursor needs to load next block of the inverted list.
     *
     * @param searchTuple
     * @param invListCmp
     * @return true if the search tuple is greater than the last element in the current block of the cursor
     *         false if the search tuple is equal to or less than the last element in the current block of the cursor
     * @throws HyracksDataException
     */
    private boolean needToReadNextBlock(ITupleReference searchTuple, MultiComparator invListCmp)
            throws HyracksDataException {
        if (moreBlocksToRead && invListCmp.compare(searchTuple, bufferEndElementTuple) > 0) {
            return true;
        }
        return false;
    }

    /**
     * Gets the tuple for the given element index.
     */
    private void getElementAtIndex(int elementIx, FixedSizeTupleReference tuple) {
        int currentPageIx =
                binarySearch(elementIndexes, bufferStartPageId - startPageId, bufferNumLoadedPages, elementIx);
        if (currentPageIx < 0) {
            throw new IndexOutOfBoundsException(
                    "Requested index: " + elementIx + " from array with numElements: " + numElements);
        }

        int currentOff;
        if (currentPageIx == 0) {
            currentOff = startOff + elementIx * elementSize;
        } else {
            int relativeElementIx = elementIx - elementIndexes[currentPageIx - 1] - 1;
            currentOff = relativeElementIx * elementSize;
        }
        // Gets the actual index in the buffers since buffers.size() can be smaller than the total number of pages.
        int bufferIdx = currentPageIx % buffers.size();
        tuple.reset(buffers.get(bufferIdx).array(), currentOff);
    }

    /**
     * Checks whether the given tuple exists on this inverted list. This method is used when doing a random traversal.
     */
    @Override
    public boolean containsKey(ITupleReference searchTuple, MultiComparator invListCmp) throws HyracksDataException {
        // If the given element is greater than the last element in the current buffer, reads the next block.
        if (needToReadNextBlock(searchTuple, invListCmp)) {
            loadPages();
        }
        int mid = -1;
        int begin = lastRandomSearchedElementIx;
        int end = bufferEndElementIx;

        while (begin <= end) {
            mid = (begin + end) / 2;
            getElementAtIndex(mid, tuple);
            int cmp = invListCmp.compare(searchTuple, tuple);
            if (cmp < 0) {
                end = mid - 1;
            } else if (cmp > 0) {
                begin = mid + 1;
            } else {
                lastRandomSearchedElementIx = mid;
                return true;
            }
        }

        lastRandomSearchedElementIx = mid;
        return false;
    }

    /**
     * Opens the cursor for the given inverted list. After this open() call, prepreLoadPages() should be called
     * before loadPages() are called. For more details, check prepapreLoadPages().
     */
    @Override
    protected void setInvListInfo(int startPageId, int endPageId, int startOff, int numElements)
            throws HyracksDataException {
        this.startPageId = startPageId;
        this.endPageId = endPageId;
        this.startOff = startOff;
        this.numElements = numElements;
        this.currentElementIxForScan = 0;
        this.currentPageIxForScan = 0;
        this.currentOffsetForScan = startOff - elementSize;
        this.bufferStartPageId = startPageId;
        // Deducts 1 since the startPage would be set to bufferEndPageId + 1 in loadPages().
        this.bufferEndPageId = startPageId - 1;
        this.moreBlocksToRead = true;
        this.numPages = endPageId - startPageId + 1;

        if (numPages > elementIndexes.length) {
            elementIndexes = new int[numPages];
        }

        for (ByteBuffer buffer : buffers) {
            buffer.clear();
        }

        // Fills the last element index per page.
        // first page
        int cumulElements = (bufferCache.getPageSize() - startOff) / elementSize;
        // Deducts 1 because this is the index, not the number of elements.
        elementIndexes[0] = cumulElements - 1;

        // middle, full pages
        for (int i = 1; i < numPages - 1; i++) {
            elementIndexes[i] = elementIndexes[i - 1] + (bufferCache.getPageSize() / elementSize);
        }

        // last page
        // Deducts 1 because this is the index, not the number of elements.
        elementIndexes[numPages - 1] = numElements - 1;
    }

    /**
     * Prints the contents of the current inverted list (a debugging method).
     */
    @SuppressWarnings("rawtypes")
    @Override
    public String printInvList(ISerializerDeserializer[] serdes) throws HyracksDataException {
        int oldCurrentOff = currentOffsetForScan;
        int oldCurrentPageId = currentPageIxForScan;
        int oldCurrentElementIx = currentElementIxForScan;

        currentOffsetForScan = startOff - elementSize;
        currentPageIxForScan = 0;
        currentElementIxForScan = 0;

        StringBuilder strBuilder = new StringBuilder();

        while (hasNext()) {
            next();
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                ByteArrayInputStream inStream = new ByteArrayInputStream(tuple.getFieldData(i), tuple.getFieldStart(i),
                        tuple.getFieldLength(i));
                DataInput dataIn = new DataInputStream(inStream);
                Object o = serdes[i].deserialize(dataIn);
                strBuilder.append(o.toString());
                if (i + 1 < tuple.getFieldCount()) {
                    strBuilder.append(",");
                }
            }
            strBuilder.append(" ");
        }

        // reset previous state
        currentOffsetForScan = oldCurrentOff;
        currentPageIxForScan = oldCurrentPageId;
        currentElementIxForScan = oldCurrentElementIx;

        return strBuilder.toString();
    }

    /**
     * Prints the current element (a debugging method).
     */
    @Override
    @SuppressWarnings("rawtypes")
    public String printCurrentElement(ISerializerDeserializer[] serdes) throws HyracksDataException {
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            ByteArrayInputStream inStream =
                    new ByteArrayInputStream(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            DataInput dataIn = new DataInputStream(inStream);
            Object o = serdes[i].deserialize(dataIn);
            strBuilder.append(o.toString());
            if (i + 1 < tuple.getFieldCount()) {
                strBuilder.append(",");
            }
        }
        return strBuilder.toString();
    }

    /**
     * Conducts a binary search to get the index of the given key.
     */
    private int binarySearch(int[] arr, int arrStart, int arrLength, int key) {
        int mid;
        int begin = arrStart;
        int end = arrStart + arrLength - 1;

        while (begin <= end) {
            mid = (begin + end) / 2;
            int cmp = (key - arr[mid]);
            if (cmp < 0) {
                end = mid - 1;
            } else if (cmp > 0) {
                begin = mid + 1;
            } else {
                return mid;
            }
        }

        if (begin > arr.length - 1) {
            return -1;
        }
        if (key < arr[begin]) {
            return begin;
        } else {
            return -1;
        }
    }

    /**
     * A compare function that is used to sort inverted list cursors
     */
    @Override
    public int compareTo(InvertedListCursor invListCursor) {
        try {
            return numElements - invListCursor.size();
        } catch (HyracksDataException hde) {
            throw new IllegalStateException(hde);
        }
    }

    /**
     * Gets the cardinality of the current inverted list.
     */
    @Override
    public int size() {
        return numElements;
    }

    /**
     * Gets the current tuple.
     */
    @Override
    public ITupleReference doGetTuple() {
        return tuple;
    }

    /**
     * Closes the cursor.
     */
    @Override
    public void doClose() throws HyracksDataException {
        if (!buffers.isEmpty()) {
            unloadPages();
        }
    }

    /**
     * Destroys the cursor.
     */
    @Override
    public void doDestroy() throws HyracksDataException {
        if (!buffers.isEmpty()) {
            unloadPages();
        }
    }

}
