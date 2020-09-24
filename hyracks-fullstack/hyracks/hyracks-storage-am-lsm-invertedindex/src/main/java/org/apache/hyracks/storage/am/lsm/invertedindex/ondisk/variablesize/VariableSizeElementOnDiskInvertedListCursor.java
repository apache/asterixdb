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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.variablesize;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.AbstractOnDiskInvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * A cursor class that traverse an inverted list that consists of variable-size elements on disk
 *
 */

public class VariableSizeElementOnDiskInvertedListCursor extends AbstractOnDiskInvertedListCursor {

    // The scan offset is set to 0 when initialized, and we need such an isInit flag
    // to avoid increasing the offset for the first element in the list when calling next()
    private boolean isInit;
    private IInvertedListTupleReference tupleReference;
    private ITreeIndexTupleWriter tupleWriter;

    public VariableSizeElementOnDiskInvertedListCursor(IBufferCache bufferCache, int fileId,
            ITypeTraits[] invListFields, IIndexCursorStats stats) throws HyracksDataException {
        super(bufferCache, fileId, invListFields, stats);
        this.isInit = true;
        this.tupleReference = new VariableSizeInvertedListTupleReference(invListFields);
        this.tupleWriter = new TypeAwareTupleWriter(invListFields);
    }

    public VariableSizeElementOnDiskInvertedListCursor(IBufferCache bufferCache, int fileId,
            ITypeTraits[] invListFields, IHyracksTaskContext ctx, IIndexCursorStats stats) throws HyracksDataException {
        super(bufferCache, fileId, invListFields, ctx, stats);
        isInit = true;
    }

    @Override
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        super.doOpen(initialState, searchPred);

        // Note that the cursors can be re-used in the upper-layer callers so we need to reset the state variables when open()
        currentElementIxForScan = 0;
        isInit = true;
        this.tupleReference = new VariableSizeInvertedListTupleReference(invListFields);
        this.tupleWriter = new TypeAwareTupleWriter(invListFields);
    }

    /**
     * Returns the next element.
     */
    @Override
    public void doNext() throws HyracksDataException {
        // init state for the first element: keep the currentOffsetForScan at 0
        if (isInit) {
            isInit = false;
        } else {
            tupleReference.reset(buffers.get(currentPageIxForScan).array(), currentOffsetForScan);
            currentOffsetForScan += tupleWriter.bytesRequired(tupleReference);
        }

        int currentPageEndOffset =
                InvertedIndexUtils.getInvertedListFrameEndOffset(buffers.get(currentPageIxForScan).array());
        assert currentOffsetForScan <= currentPageEndOffset;
        // We reach the end of the current frame, turn to the next frame
        if (currentOffsetForScan >= currentPageEndOffset) {
            currentPageIxForScan++;
            currentOffsetForScan = 0;
        }

        // Needs to read the next block?
        if (currentPageIxForScan >= buffers.size() && endPageId > bufferEndPageId) {
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
        currentOffsetForScan = bufferStartElementIx == 0 ? startOff : 0;
    }

    /**
     * Checks whether the given tuple exists on this inverted list. This method is used when doing a random traversal.
     */
    @Override
    public boolean containsKey(ITupleReference searchTuple, MultiComparator invListCmp) throws HyracksDataException {
        if (isInit) {
            // when isInit, the tuple is null, call next to fetch one tuple
            next();
        }
        while (hasNext()) {
            int cmp = invListCmp.compare(searchTuple, tuple);
            if (cmp < 0) {
                return false;
            } else if (cmp == 0) {
                return true;
            }
            // ToDo: here we get the tuple first and then call next() later because the upper-layer caller in InvertedListMerger already called next()
            // However, this is not consistent with other use cases of next() in AsterixDB
            // Maybe we need to fix the upper layer InvertedListMerger part to call next() first then getTuple()
            // to follow the convention to use cursor
            next();
        }

        if (tuple != null) {
            int cmp = invListCmp.compare(searchTuple, tuple);
            if (cmp < 0) {
                return false;
            } else if (cmp == 0) {
                return true;
            }
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

        this.currentOffsetForScan = startOff;
    }

    /**
     * Prints the contents of the current inverted list (a debugging method).
     */
    @SuppressWarnings("rawtypes")
    @Override
    public String printInvList(ISerializerDeserializer[] serdes) throws HyracksDataException {
        // Will implement later if necessary
        return "";
    }
}
