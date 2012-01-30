/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMRTreeCursorInitialState implements ICursorInitialState {

    private int numberOfTrees;
    private ITreeIndexFrameFactory rtreeInteriorFrameFactory;
    private ITreeIndexFrameFactory rtreeLeafFrameFactory;
    private ITreeIndexFrameFactory btreeLeafFrameFactory;
    private MultiComparator btreeCmp;
    private LSMRTree lsmRTree;
    private ITreeIndexAccessor[] bTreeAccessors;
    private final boolean includeMemRTree;
    private final AtomicInteger searcherRefCount;

    public LSMRTreeCursorInitialState(int numberOfTrees, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            MultiComparator btreeCmp, ITreeIndexAccessor[] bTreeAccessors, LSMRTree lsmRTree, boolean includeMemRTree,
            AtomicInteger searcherRefCount) {
        this.numberOfTrees = numberOfTrees;
        this.rtreeLeafFrameFactory = rtreeLeafFrameFactory;
        this.rtreeInteriorFrameFactory = rtreeInteriorFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;
        this.btreeCmp = btreeCmp;
        this.lsmRTree = lsmRTree;
        this.bTreeAccessors = bTreeAccessors;
        this.includeMemRTree = includeMemRTree;
        this.searcherRefCount = searcherRefCount;
    }

    public int getNumberOfTrees() {
        return numberOfTrees;
    }

    public ITreeIndexFrameFactory getRTreeInteriorFrameFactory() {
        return rtreeInteriorFrameFactory;
    }

    public ITreeIndexFrameFactory getRTreeLeafFrameFactory() {
        return rtreeLeafFrameFactory;
    }

    public ITreeIndexFrameFactory getBTreeLeafFrameFactory() {
        return btreeLeafFrameFactory;
    }

    public MultiComparator getBTreeCmp() {
        return btreeCmp;
    }

    @Override
    public ICachedPage getPage() {
        return null;
    }

    @Override
    public void setPage(ICachedPage page) {
    }

    public ITreeIndexAccessor[] getBTreeAccessors() {
        return bTreeAccessors;
    }

    public LSMRTree getLsmRTree() {
        return lsmRTree;
    }

    public boolean getIncludeMemRTree() {
        return includeMemRTree;
    }

    public AtomicInteger getSearcherRefCount() {
        return searcherRefCount;
    }

}
