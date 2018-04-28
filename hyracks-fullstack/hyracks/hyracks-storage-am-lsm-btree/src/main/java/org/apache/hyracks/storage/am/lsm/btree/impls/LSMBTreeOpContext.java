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

package org.apache.hyracks.storage.am.lsm.btree.impls;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.util.trace.ITracer;

public final class LSMBTreeOpContext extends AbstractLSMIndexOperationContext {

    /*
     * Finals
     */
    private final ITreeIndexFrameFactory insertLeafFrameFactory;
    private final ITreeIndexFrameFactory deleteLeafFrameFactory;
    private final IBTreeLeafFrame insertLeafFrame;
    private final IBTreeLeafFrame deleteLeafFrame;
    private final BTree[] mutableBTrees;
    private final BTree.BTreeAccessor[] mutableBTreeAccessors;
    private final BTreeOpContext[] mutableBTreeOpCtxs;
    private final MultiComparator cmp;
    private final MultiComparator bloomFilterCmp;
    private final BTreeRangeSearchCursor memCursor;
    private final LSMBTreeCursorInitialState searchInitialState;
    private final LSMBTreePointSearchCursor insertSearchCursor;
    /*
     * Mutables
     */
    private BTree.BTreeAccessor currentMutableBTreeAccessor;
    private BTreeOpContext currentMutableBTreeOpCtx;
    private boolean destroyed = false;

    public LSMBTreeOpContext(ILSMIndex index, List<ILSMMemoryComponent> mutableComponents,
            ITreeIndexFrameFactory insertLeafFrameFactory, ITreeIndexFrameFactory deleteLeafFrameFactory,
            IExtendedModificationOperationCallback modificationCallback, ISearchOperationCallback searchCallback,
            int numBloomFilterKeyFields, int[] btreeFields, int[] filterFields, ILSMHarness lsmHarness,
            IBinaryComparatorFactory[] filterCmpFactories, ITracer tracer) {
        super(index, btreeFields, filterFields, filterCmpFactories, searchCallback, modificationCallback, tracer);
        LSMBTreeMemoryComponent c = (LSMBTreeMemoryComponent) mutableComponents.get(0);
        IBinaryComparatorFactory cmpFactories[] = c.getIndex().getComparatorFactories();
        if (cmpFactories[0] != null) {
            this.cmp = MultiComparator.create(c.getIndex().getComparatorFactories());
        } else {
            this.cmp = null;
        }

        bloomFilterCmp = numBloomFilterKeyFields == 0 ? null
                : MultiComparator.create(c.getIndex().getComparatorFactories(), 0, numBloomFilterKeyFields);

        mutableBTrees = new BTree[mutableComponents.size()];
        mutableBTreeAccessors = new BTree.BTreeAccessor[mutableComponents.size()];
        mutableBTreeOpCtxs = new BTreeOpContext[mutableComponents.size()];
        for (int i = 0; i < mutableComponents.size(); i++) {
            LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) mutableComponents.get(i);
            mutableBTrees[i] = mutableComponent.getIndex();
            IIndexAccessParameters iap =
                    new IndexAccessParameters(modificationCallback, NoOpOperationCallback.INSTANCE);
            mutableBTreeAccessors[i] = mutableBTrees[i].createAccessor(iap);
            mutableBTreeOpCtxs[i] = mutableBTreeAccessors[i].getOpContext();
        }
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.insertLeafFrame = (IBTreeLeafFrame) insertLeafFrameFactory.createFrame();
        this.deleteLeafFrame = (IBTreeLeafFrame) deleteLeafFrameFactory.createFrame();
        if (insertLeafFrame != null && this.getCmp() != null) {
            insertLeafFrame.setMultiComparator(getCmp());
        }
        if (deleteLeafFrame != null && this.getCmp() != null) {
            deleteLeafFrame.setMultiComparator(getCmp());
        }
        searchPredicate = new RangePredicate(null, null, true, true, getCmp(), getCmp());
        memCursor = (insertLeafFrame != null) ? new BTreeRangeSearchCursor(insertLeafFrame, false) : null;
        searchInitialState = new LSMBTreeCursorInitialState(insertLeafFrameFactory, getCmp(), bloomFilterCmp,
                lsmHarness, null, searchCallback, null);
        insertSearchCursor = new LSMBTreePointSearchCursor(this);
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        reset();
        this.op = newOp;
    }

    public void setInsertMode() {
        currentMutableBTreeOpCtx.setLeafFrame(insertLeafFrame);
        currentMutableBTreeOpCtx.setLeafFrameFactory(insertLeafFrameFactory);
    }

    public void setDeleteMode() {
        currentMutableBTreeOpCtx.setLeafFrame(deleteLeafFrame);
        currentMutableBTreeOpCtx.setLeafFrameFactory(deleteLeafFrameFactory);
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        setCurrentMutableBTreeAccessor(mutableBTreeAccessors[currentMutableComponentId]);
        currentMutableBTreeOpCtx = mutableBTreeOpCtxs[currentMutableComponentId];
        switch (op) {
            case SEARCH:
            case DISKORDERSCAN:
            case UPDATE:
                // Attention: It is important to leave the leafFrame and leafFrameFactory of the mutableBTree as is
                // when doing an update. Update will only be set if a previous attempt to delete or insert failed,
                // so we must preserve the semantics of the previously requested operation.
                break;
            case UPSERT:
            case INSERT:
                setInsertMode();
                break;
            case PHYSICALDELETE:
            case DELETE:
                setDeleteMode();
                break;
        }
    }

    public BTree.BTreeAccessor getCurrentMutableBTreeAccessor() {
        return currentMutableBTreeAccessor;
    }

    public void setCurrentMutableBTreeAccessor(BTree.BTreeAccessor currentMutableBTreeAccessor) {
        this.currentMutableBTreeAccessor = currentMutableBTreeAccessor;
    }

    public LSMBTreePointSearchCursor getInsertSearchCursor() {
        return insertSearchCursor;
    }

    public BTreeRangeSearchCursor getMemCursor() {
        return memCursor;
    }

    public LSMBTreeCursorInitialState getSearchInitialState() {
        return searchInitialState;
    }

    public MultiComparator getCmp() {
        return cmp;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (destroyed) {
            return;
        }
        destroyed = true;
        Throwable failure = CleanupUtils.destroy(null, mutableBTreeAccessors);
        failure = CleanupUtils.destroy(failure, mutableBTreeOpCtxs);
        failure = CleanupUtils.destroy(failure, insertSearchCursor, memCursor);
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }
}
