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

import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeOpContext;

public final class LSMRTreeOpContext implements ILSMIndexOperationContext {

    public RTreeOpContext rtreeOpContext;
    public BTreeOpContext btreeOpContext;
    public final RTree.RTreeAccessor memRTreeAccessor;
    public final BTree.BTreeAccessor memBTreeAccessor;
    private IndexOperation op;
    public final List<ILSMComponent> componentHolder;
    public final IModificationOperationCallback modificationCallback;
    public final ISearchOperationCallback searchCallback;

    public LSMRTreeOpContext(RTree.RTreeAccessor memRtreeAccessor, IRTreeLeafFrame rtreeLeafFrame,
            IRTreeInteriorFrame rtreeInteriorFrame, ITreeIndexMetaDataFrame rtreeMetaFrame, int rTreeHeightHint,
            BTree.BTreeAccessor memBtreeAccessor, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexMetaDataFrame btreeMetaFrame,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            IModificationOperationCallback modificationCallback, ISearchOperationCallback searchCallback) {
        this.memRTreeAccessor = memRtreeAccessor;
        this.memBTreeAccessor = memBtreeAccessor;
        this.componentHolder = new LinkedList<ILSMComponent>();
        this.modificationCallback = modificationCallback;
        this.searchCallback = searchCallback;
        this.rtreeOpContext = new RTreeOpContext(rtreeLeafFrame, rtreeInteriorFrame, rtreeMetaFrame, rtreeCmpFactories,
                rTreeHeightHint, NoOpOperationCallback.INSTANCE);
        this.btreeOpContext = new BTreeOpContext(memBtreeAccessor, btreeLeafFrameFactory, btreeInteriorFrameFactory,
                btreeMetaFrame, btreeCmpFactories, modificationCallback, NoOpOperationCallback.INSTANCE);
    }

    public void setOperation(IndexOperation newOp) {
        reset();
        if (newOp == IndexOperation.INSERT) {
            rtreeOpContext.setOperation(newOp);
        } else if (newOp == IndexOperation.DELETE) {
            btreeOpContext.setOperation(IndexOperation.INSERT);
        }
        this.op = newOp;
    }

    @Override
    public void reset() {
        componentHolder.clear();
    }

    @Override
    public IndexOperation getOperation() {
        return op;
    }

    public MultiComparator getBTreeMultiComparator() {
        return btreeOpContext.cmp;
    }

    @Override
    public List<ILSMComponent> getComponentHolder() {
        return componentHolder;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return searchCallback;
    }

    @Override
    public IModificationOperationCallback getModificationCallback() {
        return modificationCallback;
    }
}