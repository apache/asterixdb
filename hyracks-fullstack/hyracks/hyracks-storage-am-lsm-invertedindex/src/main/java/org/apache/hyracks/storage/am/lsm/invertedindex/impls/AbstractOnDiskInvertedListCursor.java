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

package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

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
import org.apache.hyracks.dataflow.std.buffermanager.SingleFrameBufferManager;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * A cursor that reads an inverted list.
 */
public abstract class AbstractOnDiskInvertedListCursor extends AbstractInvertedListCursor {

    protected final IBufferCache bufferCache;
    protected final int fileId;
    // for sequential scan
    public int currentElementIxForScan;
    protected int currentOffsetForScan;
    protected int currentPageIxForScan;
    // the whole range of the given inverted list
    protected int startPageId;
    protected int endPageId;
    protected int startOff;
    protected int numElements;
    protected int numPages;
    // the current range of the loaded pages in memory
    protected int bufferStartPageId;
    protected int bufferEndPageId;
    protected int bufferStartElementIx;
    protected int bufferNumLoadedPages;

    protected final IInvertedListTupleReference tuple;
    protected final ITypeTraits[] invListFields;
    protected ICachedPage page;
    // buffer manager to conform to the memory budget
    protected final ISimpleFrameBufferManager bufferManagerForSearch;
    protected ArrayList<ByteBuffer> buffers;
    protected boolean moreBlocksToRead = true;
    // The last searched element index (used for random traversal for containsKey())
    protected int lastRandomSearchedElementIx;
    protected final IIndexCursorStats stats;

    protected AbstractOnDiskInvertedListCursor(IBufferCache bufferCache, int fileId, ITypeTraits[] invListFields,
            IHyracksTaskContext ctx, IIndexCursorStats stats) throws HyracksDataException {
        this(bufferCache, fileId, invListFields, ctx, stats, false);
    }

    protected AbstractOnDiskInvertedListCursor(IBufferCache bufferCache, int fileId, ITypeTraits[] invListFields,
            IIndexCursorStats stats) throws HyracksDataException {
        this(bufferCache, fileId, invListFields, null, stats, true);
    }

    // If isScan, use the SingleFrameBufferManager to minimize memory cost by allocating only one memory frame,
    // elsewhere use a regular buffer manager
    protected AbstractOnDiskInvertedListCursor(IBufferCache bufferCache, int fileId, ITypeTraits[] invListFields,
            IHyracksTaskContext ctx, IIndexCursorStats stats, boolean isScan) throws HyracksDataException {
        this.bufferCache = bufferCache;
        this.fileId = fileId;

        this.currentElementIxForScan = 0;
        this.currentPageIxForScan = 0;
        this.bufferStartPageId = 0;
        this.bufferEndPageId = 0;
        this.bufferStartElementIx = 0;
        this.bufferNumLoadedPages = 0;
        this.lastRandomSearchedElementIx = 0;
        this.moreBlocksToRead = true;
        this.invListFields = invListFields;
        this.tuple = InvertedIndexUtils.createInvertedListTupleReference(invListFields);
        this.buffers = new ArrayList<ByteBuffer>();
        if (ctx == null && !isScan) {
            throw HyracksDataException.create(ErrorCode.CANNOT_CONTINUE_TEXT_SEARCH_HYRACKS_TASK_IS_NULL);
        }
        if (!isScan) {
            this.bufferManagerForSearch = TaskUtil.get(HyracksConstants.INVERTED_INDEX_SEARCH_FRAME_MANAGER, ctx);
            if (bufferManagerForSearch == null) {
                throw HyracksDataException.create(ErrorCode.CANNOT_CONTINUE_TEXT_SEARCH_BUFFER_MANAGER_IS_NULL);
            }
        } else {
            this.bufferManagerForSearch = new SingleFrameBufferManager();
        }
        this.stats = stats;
    }

    /**
     * Tries to allocate enough buffers to read the inverted list at once. If memory budget is not enough, this method
     * stops allocating buffers. */
    protected void allocateBuffers() throws HyracksDataException {
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
                    AbstractOnDiskInvertedListCursor.class.getName());
        }
    }

    /**
     * Deallocates all buffers. i.e. releases all buffers to the buffer manager.
     */
    protected void deallocateBuffers() throws HyracksDataException {
        for (int i = 0; i < buffers.size(); i++) {
            bufferManagerForSearch.releaseFrame(buffers.get(i));
            buffers.set(i, null);
        }
        buffers.clear();
    }

    /**
     * Clears the contents of the buffers.
     */
    protected void clearBuffers() throws HyracksDataException {
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
     * Unloads the pages from the buffers (working memory). This will release all buffers.
     */
    @Override
    public void unloadPages() throws HyracksDataException {
        // Deallocates the buffer pages
        deallocateBuffers();
    }

    /**
     * Sets the disk-based inverted list information such as page ids and the number of elements
     * for the given inverted list.
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
        this.bufferStartPageId = startPageId;
        // Deducts 1 since the startPage would be set to bufferEndPageId + 1 in loadPages().
        this.bufferEndPageId = startPageId - 1;
        this.moreBlocksToRead = true;
        this.numPages = endPageId - startPageId + 1;

        for (ByteBuffer buffer : buffers) {
            buffer.clear();
        }
    }

    /**
     * Updates the information about this block.
     */
    protected void setBlockInfo() {
        bufferNumLoadedPages = bufferEndPageId - bufferStartPageId + 1;
        lastRandomSearchedElementIx = bufferStartElementIx;
        currentPageIxForScan = 0;
        if (bufferEndPageId == endPageId) {
            moreBlocksToRead = false;
        }
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
     * A compare function that is used to sort inverted list cursors
     */
    @Override
    public int compareTo(IInvertedListCursor invListCursor) {
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
    public int size() throws HyracksDataException {
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
