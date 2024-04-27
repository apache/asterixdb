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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManager;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnMetadata;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.IColumnIndexDiskCacheManager;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeBatchPointSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor.ICursorFactory;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.trace.ITracer;

public class LSMColumnBTree extends LSMBTree {
    private static final ICursorFactory CURSOR_FACTORY = LSMColumnBTreeSearchCursor::new;
    private final IColumnManager columnManager;
    private final IColumnIndexDiskCacheManager diskCacheManager;
    private final ILSMDiskComponentFactory mergeComponentFactory;
    /**
     * This column metadata only used during flush and dataset bulkload operations. We cannot have more than one
     * thread to do a flush/dataset bulkload. Do not use it for search/scan. Instead, use the latest component
     * metadata of the operational disk components.
     *
     * @see LSMColumnBTreeOpContext#createProjectionInfo()
     */
    private IColumnMetadata columnMetadata;

    public LSMColumnBTree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, IBufferCache diskBufferCache,
            ILSMIndexFileManager fileManager, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory mergeComponentFactory, ILSMDiskComponentFactory bulkloadComponentFactory,
            double bloomFilterFalsePositiveRate, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            int[] btreeFields, ITracer tracer, IColumnManager columnManager, boolean atomic,
            IColumnIndexDiskCacheManager diskCacheManager) throws HyracksDataException {
        super(ioManager, virtualBufferCaches, interiorFrameFactory, insertLeafFrameFactory, deleteLeafFrameFactory,
                diskBufferCache, fileManager, componentFactory, bulkloadComponentFactory, null, null, null,
                bloomFilterFalsePositiveRate, fieldCount, cmpFactories, mergePolicy, opTracker, ioScheduler,
                ioOpCallbackFactory, pageWriteCallbackFactory, true, true, btreeFields, null, true, false, tracer,
                atomic);
        this.columnManager = columnManager;
        this.mergeComponentFactory = mergeComponentFactory;
        this.diskCacheManager = diskCacheManager;
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        super.activate();
        if (diskComponents.isEmpty()) {
            columnMetadata = columnManager.activate();
        } else {
            IComponentMetadata componentMetadata = diskComponents.get(0).getMetadata();
            columnMetadata = columnManager.activate(ColumnUtil.getColumnMetadataCopy(componentMetadata));
        }

        diskCacheManager.activate(columnMetadata.getNumberOfColumns(), diskComponents, diskBufferCache);
    }

    @Override
    public LSMColumnBTreeOpContext createOpContext(IIndexAccessParameters iap) {
        int numBloomFilterKeyFields =
                ((LSMColumnBTreeWithBloomFilterDiskComponentFactory) componentFactory).getBloomFilterKeyFields().length;
        IColumnTupleProjector tupleProjector =
                ColumnUtil.getTupleProjector(iap, columnManager.getMergeColumnProjector());
        return new LSMColumnBTreeOpContext(this, memoryComponents, insertLeafFrameFactory, deleteLeafFrameFactory,
                (IExtendedModificationOperationCallback) iap.getModificationCallback(),
                iap.getSearchOperationCallback(), numBloomFilterKeyFields, getTreeFields(), getFilterFields(),
                getHarness(), getFilterCmpFactories(), tracer, tupleProjector);
    }

    protected IColumnManager getColumnManager() {
        return columnManager;
    }

    protected IColumnMetadata getColumnMetadata() {
        return columnMetadata;
    }

    @Override
    protected LSMBTreeRangeSearchCursor createCursor(AbstractLSMIndexOperationContext opCtx,
            boolean returnDeletedTuples, IIndexCursorStats stats) {
        return new LSMColumnBTreeRangeSearchCursor(opCtx, returnDeletedTuples, stats);
    }

    @Override
    public LSMBTreeBatchPointSearchCursor createBatchPointSearchCursor(ILSMIndexOperationContext opCtx) {
        return new LSMColumnBatchPointSearchCursor(opCtx);
    }

    @Override
    public ILSMDiskComponentFactory getMergeComponentFactory() {
        return mergeComponentFactory;
    }

    @Override
    public ICursorFactory getCursorFactory() {
        return CURSOR_FACTORY;
    }

    @Override
    public IColumnIndexDiskCacheManager getDiskCacheManager() {
        return diskCacheManager;
    }
}
