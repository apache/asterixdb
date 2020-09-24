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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.fixedsize;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.AbstractOnDiskInvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * A cursor class that traverse an inverted list that consists of fixed-size elements on disk
 *
 */
public class FixedSizeElementOnDiskInvertedListCursor extends AbstractOnDiskInvertedListCursor {

    private final int elementSize;
    private int bufferEndElementIx;
    // The last element in the current range in memory
    protected final IInvertedListTupleReference bufferEndElementTuple;
    // The last element index per page
    private int[] elementIndexes = new int[10];

    public FixedSizeElementOnDiskInvertedListCursor(IBufferCache bufferCache, int fileId, ITypeTraits[] invListFields,
            IHyracksTaskContext ctx, IIndexCursorStats stats) throws HyracksDataException {
        super(bufferCache, fileId, invListFields, ctx, stats);

        this.bufferEndElementIx = 0;
        this.bufferEndElementTuple = InvertedIndexUtils.createInvertedListTupleReference(invListFields);

        int tmpSize = 0;
        for (int i = 0; i < invListFields.length; i++) {
            tmpSize += invListFields[i].getFixedLength();
        }
        elementSize = tmpSize;
        this.currentOffsetForScan = -elementSize;
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
     * Updates the information about this block.
     */
    @Override
    protected void setBlockInfo() {
        super.setBlockInfo();

        bufferStartElementIx =
                bufferStartPageId == startPageId ? 0 : elementIndexes[bufferStartPageId - startPageId - 1] + 1;
        bufferEndElementIx = elementIndexes[bufferEndPageId - startPageId];
        // Gets the final element tuple in this block.
        getElementAtIndex(bufferEndElementIx, bufferEndElementTuple);
    }

    /**
     * Gets the tuple for the given element index.
     */
    private void getElementAtIndex(int elementIx, IInvertedListTupleReference tuple) {
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
     * Opens the cursor for the given inverted list. After this open() call, prepreLoadPages() should be called
     * before loadPages() are called. For more details, check prepapreLoadPages().
     */
    @Override
    protected void setInvListInfo(int startPageId, int endPageId, int startOff, int numElements)
            throws HyracksDataException {
        super.setInvListInfo(startPageId, endPageId, startOff, numElements);

        if (numPages > elementIndexes.length) {
            elementIndexes = new int[numPages];
        }
        this.currentOffsetForScan = startOff - elementSize;
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
}
