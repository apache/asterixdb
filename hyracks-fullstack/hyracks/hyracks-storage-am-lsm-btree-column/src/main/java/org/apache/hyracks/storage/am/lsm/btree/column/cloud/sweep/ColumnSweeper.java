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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud.sweep;

import static org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState.READABLE_UNWRITABLE;
import static org.apache.hyracks.storage.am.lsm.common.util.LSMComponentIdUtils.isMergedComponent;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.cloud.buffercache.page.CloudCachedPage;
import org.apache.hyracks.cloud.cache.unit.SweepableIndexUnit;
import org.apache.hyracks.cloud.sweeper.SweepContext;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.ColumnRanges;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.annotations.CriticalPath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ColumnSweeper {
    private static final Logger LOGGER = LogManager.getLogger();
    private final ColumnSweepLockInfo lockedColumns;
    private final ColumnRanges ranges;
    private final List<ILSMDiskComponent> sweepableComponents;

    public ColumnSweeper(int numberOfPrimaryKeys) {
        lockedColumns = new ColumnSweepLockInfo();
        ranges = new ColumnRanges(numberOfPrimaryKeys);
        sweepableComponents = new ArrayList<>();
    }

    public long sweep(BitSet plan, SweepContext context, IColumnTupleProjector sweepProjector)
            throws HyracksDataException {
        SweepableIndexUnit indexUnit = context.getIndexUnit();
        LSMColumnBTree lsmColumnBTree = (LSMColumnBTree) indexUnit.getIndex();
        IColumnProjectionInfo projectionInfo = captureSweepableComponents(lsmColumnBTree, sweepProjector);
        if (projectionInfo == null) {
            // no sweepable components
            return 0L;
        }

        LOGGER.info("Sweeping {}", lsmColumnBTree);
        ILSMDiskComponent latestComponent = sweepableComponents.get(0);
        ColumnBTreeReadLeafFrame leafFrame = ColumnSweeperUtil.createLeafFrame(projectionInfo, latestComponent);
        IBufferCacheReadContext bcOpCtx = SweepBufferCacheReadContext.INSTANCE;
        lockedColumns.reset(plan);
        long freedSpace = 0L;
        for (int i = 0; i < sweepableComponents.size(); i++) {
            if (context.stopSweeping()) {
                // Exit as the index is being dropped
                return 0L;
            }

            boolean failed = false;
            // Components are entered one at a time to allow components to be merged and deactivated
            ILSMDiskComponent diskComponent = enterAndGetComponent(i, lsmColumnBTree);
            // If diskComponent is null, that means it is not a viable candidate anymore
            if (diskComponent != null) {
                try {
                    freedSpace += sweepDiskComponent(leafFrame, diskComponent, plan, context, bcOpCtx);
                } catch (Throwable e) {
                    failed = true;
                    throw e;
                } finally {
                    exitComponent(diskComponent, lsmColumnBTree, failed);
                }
            }
        }

        LOGGER.info("Swept {} components and freed {} from disk", sweepableComponents.size(),
                StorageUtil.toHumanReadableSize(freedSpace));
        return freedSpace;
    }

    @CriticalPath
    private IColumnProjectionInfo captureSweepableComponents(LSMColumnBTree lsmColumnBTree,
            IColumnTupleProjector sweepProjector) throws HyracksDataException {
        ILSMOperationTracker opTracker = lsmColumnBTree.getOperationTracker();
        sweepableComponents.clear();
        synchronized (opTracker) {
            List<ILSMDiskComponent> diskComponents = lsmColumnBTree.getDiskComponents();
            for (int i = 0; i < diskComponents.size(); i++) {
                ILSMDiskComponent diskComponent = diskComponents.get(i);
                /*
                 * Get components that are only in READABLE_UNWRITABLE state. Components that are currently being
                 * merged should not be swept as they will be deleted anyway.
                 * Also, only sweep merged components as flushed components are relatively smaller and should be
                 * merged eventually. So, it is preferable to read everything locally when merging flushed components.
                 * TODO should we sweep flushed components?
                 */
                if (isMergedComponent(diskComponent.getId()) && diskComponent.getState() == READABLE_UNWRITABLE
                        && diskComponent.getComponentSize() > 0) {
                    // The component is a good candidate to be swept
                    sweepableComponents.add(diskComponent);
                }
            }

            if (sweepableComponents.isEmpty()) {
                // No sweepable components
                return null;
            }

            return ColumnSweeperUtil.createColumnProjectionInfo(sweepableComponents, sweepProjector);
        }
    }

    private ILSMDiskComponent enterAndGetComponent(int index, LSMColumnBTree lsmColumnBTree)
            throws HyracksDataException {
        ILSMDiskComponent diskComponent = sweepableComponents.get(index);
        synchronized (lsmColumnBTree.getOperationTracker()) {
            // Make sure the component is still in READABLE_UNWRITABLE state
            if (diskComponent.getState() == READABLE_UNWRITABLE
                    && diskComponent.threadEnter(LSMOperationType.DISK_COMPONENT_SCAN, false)) {
                return diskComponent;
            }
        }

        // the component is not a viable candidate anymore
        return null;
    }

    private void exitComponent(ILSMDiskComponent diskComponent, LSMColumnBTree lsmColumnBTree, boolean failed)
            throws HyracksDataException {
        synchronized (lsmColumnBTree.getOperationTracker()) {
            diskComponent.threadExit(LSMOperationType.DISK_COMPONENT_SCAN, failed, false);
        }
    }

    private long sweepDiskComponent(ColumnBTreeReadLeafFrame leafFrame, ILSMDiskComponent diskComponent, BitSet plan,
            SweepContext context, IBufferCacheReadContext bcOpCtx) throws HyracksDataException {
        long dpid = getFirstPageId(diskComponent);
        int fileId = BufferedFileHandle.getFileId(dpid);
        int nextPageId = BufferedFileHandle.getPageId(dpid);
        int freedSpace = 0;
        context.open(fileId);
        try {
            while (nextPageId >= 0) {
                if (context.stopSweeping()) {
                    // Exit as the index is being dropped
                    return 0L;
                }
                CloudCachedPage page0 = context.pin(BufferedFileHandle.getDiskPageId(fileId, nextPageId), bcOpCtx);
                boolean columnsLocked = false;
                try {
                    leafFrame.setPage(page0);
                    nextPageId = leafFrame.getNextLeaf();
                    columnsLocked = page0.trySweepLock(lockedColumns);
                    if (columnsLocked) {
                        leafFrame.setPage(page0);
                        ranges.reset(leafFrame, plan);
                        freedSpace += punchHoles(context, leafFrame);
                    }
                } finally {
                    if (columnsLocked) {
                        page0.sweepUnlock();
                    }
                    context.unpin(page0, bcOpCtx);
                }
            }
        } finally {
            context.close();
        }

        return freedSpace;
    }

    private long getFirstPageId(ILSMDiskComponent diskComponent) {
        ColumnBTree columnBTree = (ColumnBTree) diskComponent.getIndex();
        int fileId = columnBTree.getFileId();
        int firstPage = columnBTree.getBulkloadLeafStart();
        return BufferedFileHandle.getDiskPageId(fileId, firstPage);
    }

    private int punchHoles(SweepContext context, ColumnBTreeReadLeafFrame leafFrame) throws HyracksDataException {
        int freedSpace = 0;
        int numberOfPages = leafFrame.getMegaLeafNodeNumberOfPages();
        int pageZeroId = leafFrame.getPageId();

        // Start from 1 as we do not evict pageZero
        BitSet nonEvictablePages = ranges.getNonEvictablePages();
        int start = nonEvictablePages.nextClearBit(1);
        while (start < numberOfPages) {
            int end = nonEvictablePages.nextSetBit(start);
            int numberOfEvictablePages = end - start;
            freedSpace += context.punchHole(pageZeroId + start, numberOfEvictablePages);

            start = nonEvictablePages.nextClearBit(end);
        }

        return freedSpace;
    }
}
