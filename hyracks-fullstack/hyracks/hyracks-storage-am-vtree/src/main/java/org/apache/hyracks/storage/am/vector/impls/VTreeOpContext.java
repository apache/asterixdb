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

package org.apache.hyracks.storage.am.vector.impls;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleBuilder;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleBuilderFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

/**
 * Operation context for vector clustering tree operations.
 * <p>
 * Not thread-safe: holds mutable per-operation frames/state and is confined to a single operation
 * context (one accessor / one thread at a time).
 */
public class VTreeOpContext implements IIndexOperationContext, IExtraPageBlockHelper {

    private static final int INIT_ARRAYLIST_SIZE = 6;

    private final IIndexAccessor accessor;
    private final MultiComparator cmp;
    private final ITreeIndexFrameFactory interiorFrameFactory;
    private final ITreeIndexFrameFactory leafFrameFactory;
    private final ITreeIndexFrameFactory metadataFrameFactory;
    private ITreeIndexFrameFactory dataFrameFactory; // Mutable for LSMBTree-style frame switching
    private final IPageManager freePageManager;
    private final ITreeIndexMetadataFrame metaFrame;
    private final int vectorDimensions;
    private final IVTreeDataTupleBuilder dataTupleBuilder;

    private IVTreeInteriorFrame interiorFrame;
    private IVTreeLeafFrame leafFrame;
    private IVTreeMetadataFrame metadataFrame;
    private IVTreeDataFrame dataFrame;

    private IndexOperation op;
    private IModificationOperationCallback modificationCallback;
    private ISearchOperationCallback searchCallback;

    private Deque<Long> pageLsns;
    private List<Integer> smPages;
    private int opRestarts = 0;
    private boolean destroyed = false;
    private long metadataPageId = -1;

    public VTreeOpContext(IIndexAccessor accessor, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, ITreeIndexFrameFactory metadataFrameFactory,
            ITreeIndexFrameFactory dataFrameFactory, IPageManager freePageManager,
            IBinaryComparatorFactory[] cmpFactories, int vectorDimensions,
            IModificationOperationCallback modificationCallback, ISearchOperationCallback searchCallback,
            IVTreeDataTupleBuilderFactory dataTupleBuilderFactory, VTreeQuantizationParams quantizationParams) {
        this.accessor = accessor;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.metadataFrameFactory = metadataFrameFactory;
        this.dataFrameFactory = dataFrameFactory;
        this.freePageManager = freePageManager;
        this.vectorDimensions = vectorDimensions;
        this.modificationCallback = modificationCallback;
        this.searchCallback = searchCallback;
        this.dataTupleBuilder = dataTupleBuilderFactory.createDataTupleBuilder(quantizationParams);

        // A null / empty cmpFactories array (or a null first entry) means "no comparator" for this
        // context; guard the [0] access so a missing array does not surface as an obscure NPE/AIOOBE.
        if (cmpFactories != null && cmpFactories.length > 0 && cmpFactories[0] != null) {
            this.cmp = MultiComparator.create(cmpFactories);
        } else {
            this.cmp = null;
        }

        this.interiorFrame = (IVTreeInteriorFrame) interiorFrameFactory.createFrame();
        this.leafFrame = (IVTreeLeafFrame) leafFrameFactory.createFrame();
        this.metadataFrame = (IVTreeMetadataFrame) metadataFrameFactory.createFrame();
        this.dataFrame = (IVTreeDataFrame) dataFrameFactory.createFrame();
        this.metaFrame = freePageManager.createMetadataFrame();

        this.pageLsns = new ArrayDeque<>(INIT_ARRAYLIST_SIZE);
        this.smPages = new ArrayList<>(INIT_ARRAYLIST_SIZE);
    }

    @Override
    public void reset() {
        pageLsns.clear();
        smPages.clear();
        opRestarts = 0;
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        this.op = newOp;
    }

    @Override
    public IndexOperation getOperation() {
        return op;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (destroyed) {
            return;
        }
        destroyed = true;
        // Cleanup resources if needed
    }

    // Getters
    public IIndexAccessor getAccessor() {
        return accessor;
    }

    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    public ITreeIndexFrameFactory getMetadataFrameFactory() {
        return metadataFrameFactory;
    }

    public ITreeIndexFrameFactory getDataFrameFactory() {
        return dataFrameFactory;
    }

    public IPageManager getFreePageManager() {
        return freePageManager;
    }

    public IVTreeInteriorFrame getInteriorFrame() {
        return interiorFrame;
    }

    public IVTreeLeafFrame getLeafFrame() {
        return leafFrame;
    }

    public IVTreeMetadataFrame getMetadataFrame() {
        return metadataFrame;
    }

    public IVTreeDataFrame getDataFrame() {
        return dataFrame;
    }

    public ITreeIndexMetadataFrame getMetaFrame() {
        return metaFrame;
    }

    public MultiComparator getCmp() {
        return cmp;
    }

    public int getVectorDimensions() {
        return vectorDimensions;
    }

    public IVTreeDataTupleBuilder getDataTupleBuilder() {
        return dataTupleBuilder;
    }

    public IModificationOperationCallback getModificationCallback() {
        return modificationCallback;
    }

    public ISearchOperationCallback getSearchCallback() {
        return searchCallback;
    }

    public Deque<Long> getPageLsns() {
        return pageLsns;
    }

    public List<Integer> getSmPages() {
        return smPages;
    }

    public int getOpRestarts() {
        return opRestarts;
    }

    public void setOpRestarts(int opRestarts) {
        this.opRestarts = opRestarts;
    }

    // IExtraPageBlockHelper methods
    @Override
    public int getFreeBlock(int size) throws HyracksDataException {
        return freePageManager.takeBlock(metaFrame, size);
    }

    @Override
    public void returnFreePageBlock(int blockPageId, int size) throws HyracksDataException {
        freePageManager.releaseBlock(metaFrame, blockPageId, size);
    }

    public void setCallbacks(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        this.modificationCallback = modificationCallback;
        this.searchCallback = searchCallback;
    }

    public long getMetadataPageId() {
        return metadataPageId;
    }

    public void setMetadataPageId(long metadataPageId) {
        this.metadataPageId = metadataPageId;
    }

    public void setDataFrame(IVTreeDataFrame dataFrame) {
        this.dataFrame = dataFrame;
    }

    public void setDataFrameFactory(ITreeIndexFrameFactory dataFrameFactory) {
        this.dataFrameFactory = dataFrameFactory;
    }
}
