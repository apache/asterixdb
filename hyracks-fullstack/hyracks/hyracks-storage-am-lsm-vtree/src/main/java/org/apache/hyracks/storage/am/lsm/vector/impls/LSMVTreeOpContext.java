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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.impls.VTreeOpContext;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.util.trace.ITracer;

/**
 * Operation context for LSM Vector Clustering Tree operations.
 * Holds pre-created accessors and op contexts for every mutable component, plus the per-operation
 * mutable cursor state. Mirrors the LSM B+Tree op-context pattern.
 */
public class LSMVTreeOpContext extends AbstractLSMIndexOperationContext {

    // Immutable: pre-created resources, reused across operations
    private final ITreeIndexFrameFactory insertDataFrameFactory;
    private final ITreeIndexFrameFactory deleteDataFrameFactory;
    private final IVTreeDataFrame insertDataFrame;
    private final IVTreeDataFrame deleteDataFrame;
    private final VTree[] mutableVTrees;
    private final VTree.VTreeAccessor[] mutableVTreeAccessors;
    private final VTreeOpContext[] mutableVTreeOpCtxs;
    private final MultiComparator cmp;
    private final LSMVTreeCursorInitialState searchInitialState;
    private final IIndexAccessParameters iap;

    // Mutable: switched per operation
    private VTree.VTreeAccessor currentMutableVTreeAccessor;
    private VTreeOpContext currentMutableVTreeOpCtx;
    private boolean destroyed = false;

    public LSMVTreeOpContext(LSMVTree lsmTree, List<ILSMMemoryComponent> mutableComponents, int[] treeFields,
            int[] filterFields, IBinaryComparatorFactory[] filterCmpFactories,
            IExtendedModificationOperationCallback modificationCallback, ISearchOperationCallback searchCallback,
            ITracer tracer, IIndexAccessParameters iap) {
        super(lsmTree, treeFields, filterFields, filterCmpFactories, searchCallback, modificationCallback, tracer);

        this.iap = iap;
        this.insertDataFrameFactory = lsmTree.getInsertDataFrameFactory();
        this.deleteDataFrameFactory = lsmTree.getDeleteDataFrameFactory();

        // ADM-aware comparators: handle the 1-byte type tag prefix for ADOUBLE, AINT32, etc.
        IBinaryComparatorFactory[] cmpFactories = lsmTree.getCmpFactories();
        IBinaryComparator[] comparators = new IBinaryComparator[cmpFactories.length];
        for (int i = 0; i < comparators.length; i++) {
            comparators[i] = cmpFactories[i].createBinaryComparator();
        }
        this.cmp = new MultiComparator(comparators);

        this.insertDataFrame = (IVTreeDataFrame) insertDataFrameFactory.createFrame();
        this.deleteDataFrame = (IVTreeDataFrame) deleteDataFrameFactory.createFrame();

        // Pre-create accessors and op contexts for ALL mutable components
        mutableVTrees = new VTree[mutableComponents.size()];
        mutableVTreeAccessors = new VTree.VTreeAccessor[mutableComponents.size()];
        mutableVTreeOpCtxs = new VTreeOpContext[mutableComponents.size()];

        for (int i = 0; i < mutableComponents.size(); i++) {
            LSMVTreeMemoryComponent mutableComponent = (LSMVTreeMemoryComponent) mutableComponents.get(i);
            mutableVTrees[i] = mutableComponent.getIndex();
            IIndexAccessParameters componentIap =
                    new IndexAccessParameters(modificationCallback, NoOpOperationCallback.INSTANCE);
            mutableVTreeAccessors[i] = (VTree.VTreeAccessor) mutableVTrees[i].createAccessor(componentIap);
            mutableVTreeOpCtxs[i] = mutableVTreeAccessors[i].getOpContext();
        }

        // Search state defaults to insert-mode data frame; switched on delete/upsert path
        this.searchInitialState = new LSMVTreeCursorInitialState(lsmTree.getInteriorFrameFactory(),
                lsmTree.getLeafFrameFactory(), lsmTree.getMetadataFrameFactory(), insertDataFrameFactory, cmp,
                lsmTree.getHarness(), null, searchCallback, componentHolder);
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        reset();
        this.op = newOp;
    }

    public void setInsertMode() {
        currentMutableVTreeOpCtx.setDataFrame(insertDataFrame);
        currentMutableVTreeOpCtx.setDataFrameFactory(insertDataFrameFactory);
    }

    public void setDeleteMode() {
        currentMutableVTreeOpCtx.setDataFrame(deleteDataFrame);
        currentMutableVTreeOpCtx.setDataFrameFactory(deleteDataFrameFactory);
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        setCurrentMutableVTreeAccessor(mutableVTreeAccessors[currentMutableComponentId]);
        currentMutableVTreeOpCtx = mutableVTreeOpCtxs[currentMutableComponentId];
        switch (op) {
            case SEARCH:
            case DISKORDERSCAN:
            case UPDATE:
                // Data frame is set by the calling path; nothing to do here.
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

    public VTree.VTreeAccessor getCurrentMutableVTreeAccessor() {
        return currentMutableVTreeAccessor;
    }

    public void setCurrentMutableVTreeAccessor(VTree.VTreeAccessor accessor) {
        this.currentMutableVTreeAccessor = accessor;
    }

    public LSMVTreeCursorInitialState getSearchInitialState() {
        return searchInitialState;
    }

    public MultiComparator getCmp() {
        return cmp;
    }

    public IIndexAccessParameters getIndexAccessParameters() {
        return iap;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (destroyed) {
            return;
        }
        destroyed = true;
        Throwable failure = CleanupUtils.destroy(null, mutableVTreeAccessors);
        failure = CleanupUtils.destroy(failure, mutableVTreeOpCtxs);
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }
}
