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

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.InvertedListCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * A simple scan cursor that only reads a frame by frame from the inverted list on disk. This cursor does not
 * conduct a binary search. It only supports the scan operation. The main purpose of this cursor is
 * doing a full-scan of an inverted list during a storage-component-merge process.
 */
public class FixedSizeElementInvertedListScanCursor extends InvertedListCursor {

    protected final IBufferCache bufferCache;
    protected final int fileId;
    protected final int elementSize;
    protected int currentElementIxForScan;
    protected int currentOffsetForScan;
    protected int currentPageId;

    protected int startPageId;
    protected int endPageId;
    protected int startOff;
    protected int numElements;
    protected int numPages;

    protected final FixedSizeTupleReference tuple;
    protected ICachedPage page;

    protected boolean pinned;
    protected int pinnedPageId = -1;
    protected final IIndexCursorStats stats;

    public FixedSizeElementInvertedListScanCursor(IBufferCache bufferCache, int fileId, ITypeTraits[] invListFields,
            IIndexCursorStats stats) throws HyracksDataException {
        this.bufferCache = bufferCache;
        this.fileId = fileId;
        int tmpSize = 0;
        for (int i = 0; i < invListFields.length; i++) {
            tmpSize += invListFields[i].getFixedLength();
        }
        elementSize = tmpSize;
        this.currentElementIxForScan = 0;
        this.currentOffsetForScan = -elementSize;
        this.currentPageId = 0;
        this.startPageId = 0;
        this.endPageId = 0;
        this.startOff = 0;
        this.numElements = 0;
        this.numPages = 0;
        this.tuple = new FixedSizeTupleReference(invListFields);
        this.pinned = false;
        this.stats = stats;
    }

    @Override
    public boolean doHasNext() {
        return currentElementIxForScan < numElements;
    }

    @Override
    public void doNext() throws HyracksDataException {
        if (currentOffsetForScan + 2 * elementSize > bufferCache.getPageSize()) {
            // Read the next page.
            currentOffsetForScan = 0;
            loadPages();
        } else {
            currentOffsetForScan += elementSize;
        }
        currentElementIxForScan++;
        tuple.reset(page.getBuffer().array(), currentOffsetForScan);
    }

    @Override
    public void prepareLoadPages() throws HyracksDataException {
        // No-op for this cursor since it only loads one page to the buffer cache at a time.
    }

    /**
     * Loads one page from the inverted list into the buffer cache.
     */
    @Override
    public void loadPages() throws HyracksDataException {
        if (currentPageId == endPageId) {
            // inverted list exhausted, return
            return;
        }
        currentPageId++;
        if (pinned && pinnedPageId == currentPageId) {
            // already pinned, return
            return;
        }
        unloadPages();
        page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), false);
        pinnedPageId = currentPageId;
        pinned = true;
        stats.getPageCounter().update(1);

    }

    @Override
    public void unloadPages() throws HyracksDataException {
        if (pinned) {
            bufferCache.unpin(page);
            pinned = false;
        }
    }

    @Override
    protected void setInvListInfo(int startPageId, int endPageId, int startOff, int numElements)
            throws HyracksDataException {
        this.startPageId = startPageId;
        this.endPageId = endPageId;
        this.startOff = startOff;
        this.numElements = numElements;
        this.currentElementIxForScan = 0;
        this.currentOffsetForScan = startOff - elementSize;
        // Deducts 1 since the startPage would be set to bufferCurrentPageId + 1 in loadPages().
        this.currentPageId = startPageId - 1;
        this.numPages = endPageId - startPageId + 1;
    }

    @Override
    public int compareTo(InvertedListCursor invListCursor) {
        try {
            return numElements - invListCursor.size();
        } catch (HyracksDataException hde) {
            throw new IllegalStateException(hde);
        }
    }

    @Override
    public int size() {
        return numElements;
    }

    @Override
    public ITupleReference doGetTuple() {
        return tuple;
    }

    @Override
    public void doClose() throws HyracksDataException {
        // No op
        // We allow the inverted list cursor to hold at most one page to avoid
        // unnecessary pins
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        unloadPages();
    }

    @Override
    public boolean containsKey(ITupleReference searchTuple, MultiComparator invListCmp) throws HyracksDataException {
        // This method is designed for a random search.
        return false;
    }

    @Override
    public String printInvList(ISerializerDeserializer[] serdes) throws HyracksDataException {
        return null;
    }

    @Override
    public String printCurrentElement(ISerializerDeserializer[] serdes) throws HyracksDataException {
        return null;
    }

}
