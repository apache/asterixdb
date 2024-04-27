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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.btree;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.storage.am.btree.impls.DiskBTree;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnMetadata;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnWriteContext;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;

public class ColumnBTree extends DiskBTree {
    public ColumnBTree(IBufferCache bufferCache, IPageManager freePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            IBinaryComparatorFactory[] cmpFactories, int fieldCount, FileReference file) {
        super(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories, fieldCount, file);
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, IPageWriteCallback callback) {
        throw new IllegalAccessError("Missing write column metadata");
    }

    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, IPageWriteCallback callback,
            IColumnMetadata columnMetadata, IColumnWriteContext writeContext) throws HyracksDataException {
        ColumnBTreeLeafFrameFactory columnLeafFrameFactory = (ColumnBTreeLeafFrameFactory) leafFrameFactory;
        ColumnBTreeWriteLeafFrame writeLeafFrame =
                columnLeafFrameFactory.createWriterFrame(columnMetadata, writeContext);
        return new ColumnBTreeBulkloader(fillFactor, verifyInput, callback, this, writeLeafFrame, writeContext);
    }

    @Override
    public BTreeAccessor createAccessor(IIndexAccessParameters iap) {
        throw new IllegalArgumentException(
                "Use createAccessor(IIndexAccessParameters, int, IColumnProjectionInfo, IColumnReadContext)");
    }

    public BTreeAccessor createAccessor(IIndexAccessParameters iap, int index, IColumnProjectionInfo projectionInfo,
            IColumnReadContext context) {
        return new ColumnBTreeAccessor(this, iap, index, projectionInfo, context);
    }

    public class ColumnBTreeAccessor extends DiskBTreeAccessor {
        private final int index;
        private final IColumnProjectionInfo projectionInfo;
        private final IColumnReadContext context;

        public ColumnBTreeAccessor(ColumnBTree btree, IIndexAccessParameters iap, int index,
                IColumnProjectionInfo projectionInfo, IColumnReadContext context) {
            super(btree, iap);
            this.index = index;
            this.projectionInfo = projectionInfo;
            this.context = context;
        }

        @Override
        public ITreeIndexCursor createSearchCursor(boolean exclusive) {
            ColumnBTreeLeafFrameFactory columnLeafFrameFactory = (ColumnBTreeLeafFrameFactory) leafFrameFactory;
            ColumnBTreeReadLeafFrame readLeafFrame = columnLeafFrameFactory.createReadFrame(projectionInfo);
            return new ColumnBTreeRangeSearchCursor(
                    readLeafFrame, (IIndexCursorStats) iap.getParameters()
                            .getOrDefault(HyracksConstants.INDEX_CURSOR_STATS, NoOpIndexCursorStats.INSTANCE),
                    index, context);
        }

        @Override
        public ITreeIndexCursor createPointCursor(boolean exclusive, boolean stateful) {
            ColumnBTreeLeafFrameFactory columnLeafFrameFactory = (ColumnBTreeLeafFrameFactory) leafFrameFactory;
            ColumnBTreeReadLeafFrame readLeafFrame = columnLeafFrameFactory.createReadFrame(projectionInfo);
            return new ColumnBTreePointSearchCursor(
                    readLeafFrame, (IIndexCursorStats) iap.getParameters()
                            .getOrDefault(HyracksConstants.INDEX_CURSOR_STATS, NoOpIndexCursorStats.INSTANCE),
                    index, context);
        }

        @Override
        protected IBufferCacheReadContext getBufferCacheOperationContext() {
            return context;
        }
    }
}