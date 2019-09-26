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

import java.util.List;

import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class LSMInvertedIndexSearchCursorInitialState implements ICursorInitialState {

    public static final int INVALID_VALUE = -1;

    private final boolean includeMemComponent;
    private final ILSMHarness lsmHarness;
    private final LSMInvertedIndexOpContext ctx;
    private ISearchOperationCallback searchCallback;
    private MultiComparator originalCmp;
    private final MultiComparator keyCmp;
    private final PermutingTupleReference keysOnlyTuple;
    private final ITreeIndexFrameFactory deletedKeysBtreeLeafFrameFactory;

    private final List<ILSMComponent> operationalComponents;

    // For disk-based inverted list cursors
    private int invListStartPageId = INVALID_VALUE;
    private int invListEndPageId = INVALID_VALUE;
    private int invListStartOffset = INVALID_VALUE;
    private int invListNumElements = INVALID_VALUE;

    public LSMInvertedIndexSearchCursorInitialState() {
        this(null, null, null, null, false, null, null);
        resetInvertedListInfo();
    }

    public LSMInvertedIndexSearchCursorInitialState(final MultiComparator keyCmp, PermutingTupleReference keysOnlyTuple,
            ITreeIndexFrameFactory deletedKeysBtreeLeafFrameFactory, IIndexOperationContext ctx,
            boolean includeMemComponent, ILSMHarness lsmHarness, List<ILSMComponent> operationalComponents) {
        this.keyCmp = keyCmp;
        this.keysOnlyTuple = keysOnlyTuple;
        this.deletedKeysBtreeLeafFrameFactory = deletedKeysBtreeLeafFrameFactory;
        this.includeMemComponent = includeMemComponent;
        this.operationalComponents = operationalComponents;
        this.lsmHarness = lsmHarness;
        this.ctx = (LSMInvertedIndexOpContext) ctx;
        this.searchCallback = ctx != null ? this.ctx.getSearchOperationCallback() : null;
        resetInvertedListInfo();
    }

    @Override
    public ICachedPage getPage() {
        return null;
    }

    @Override
    public void setPage(ICachedPage page) {
    }

    public List<ILSMComponent> getOperationalComponents() {
        return operationalComponents;
    }

    public boolean getIncludeMemComponent() {
        return includeMemComponent;
    }

    public ILSMHarness getLSMHarness() {
        return lsmHarness;
    }

    public ILSMIndexOperationContext getOpContext() {
        return ctx;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return searchCallback;
    }

    @Override
    public void setSearchOperationCallback(ISearchOperationCallback searchCallback) {
        this.searchCallback = searchCallback;
    }

    @Override
    public MultiComparator getOriginalKeyComparator() {
        return originalCmp;
    }

    @Override
    public void setOriginialKeyComparator(MultiComparator originalCmp) {
        this.originalCmp = originalCmp;
    }

    public MultiComparator getKeyComparator() {
        return keyCmp;
    }

    public ITreeIndexFrameFactory getgetDeletedKeysBTreeLeafFrameFactory() {
        return deletedKeysBtreeLeafFrameFactory;
    }

    public PermutingTupleReference getKeysOnlyTuple() {
        return keysOnlyTuple;
    }

    public void setInvertedListInfo(int invListStartPageId, int invListEndPageId, int invListStartOffset,
            int invListNumElements) {
        this.invListStartPageId = invListStartPageId;
        this.invListEndPageId = invListEndPageId;
        this.invListStartOffset = invListStartOffset;
        this.invListNumElements = invListNumElements;
    }

    public int getInvListStartPageId() {
        return invListStartPageId;
    }

    public int getInvListEndPageId() {
        return invListEndPageId;
    }

    public int getInvListStartOffset() {
        return invListStartOffset;
    }

    public int getInvListNumElements() {
        return invListNumElements;
    }

    private void resetInvertedListInfo() {
        invListStartPageId = INVALID_VALUE;
        invListEndPageId = INVALID_VALUE;
        invListStartOffset = INVALID_VALUE;
        invListNumElements = INVALID_VALUE;
    }
}
