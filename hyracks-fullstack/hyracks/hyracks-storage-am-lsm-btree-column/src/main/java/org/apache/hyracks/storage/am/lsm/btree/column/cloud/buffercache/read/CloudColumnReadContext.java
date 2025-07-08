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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.read;

import static org.apache.hyracks.cloud.buffercache.context.DefaultCloudReadContext.readAndPersistPage;
import static org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType.MERGE;
import static org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType.MODIFY;
import static org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType.QUERY;
import static org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.read.CloudMegaPageReadContext.ALL_PAGES;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.cloud.buffercache.page.CloudCachedPage;
import org.apache.hyracks.cloud.buffercache.page.ISweepLockInfo;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.ColumnRanges;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.sweep.ColumnSweepLockInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.IThreadStats;
import org.apache.hyracks.util.annotations.NotThreadSafe;

@NotThreadSafe
public final class CloudColumnReadContext implements IColumnReadContext {
    public static final Integer MAX_RANGES_COUNT = 3;

    private final ColumnProjectorType operation;
    private final IPhysicalDrive drive;
    private final BitSet plan;
    private final BitSet cloudOnlyColumns;
    private final ColumnRanges columnRanges;
    private final CloudMegaPageReadContext columnCtx;
    private final BitSet projectedColumns;
    private final AbstractPageRangesComputer mergedPageRanges;

    public CloudColumnReadContext(IColumnProjectionInfo projectionInfo, IPhysicalDrive drive, BitSet plan) {
        this.operation = projectionInfo.getProjectorType();
        this.drive = drive;
        this.plan = plan;
        columnRanges = new ColumnRanges(projectionInfo.getNumberOfPrimaryKeys());
        cloudOnlyColumns = new BitSet();
        columnCtx = new CloudMegaPageReadContext(operation, columnRanges, drive);
        projectedColumns = new BitSet();
        mergedPageRanges = AbstractPageRangesComputer.create(MAX_RANGES_COUNT);
        if (operation == QUERY || operation == MODIFY) {
            for (int i = 0; i < projectionInfo.getNumberOfProjectedColumns(); i++) {
                int columnIndex = projectionInfo.getColumnIndex(i);
                if (columnIndex >= 0) {
                    projectedColumns.set(columnIndex);
                }
            }
        }
    }

    @Override
    public void onPin(ICachedPage page) {
        CloudCachedPage cloudPage = (CloudCachedPage) page;
        ISweepLockInfo lockTest = cloudPage.beforeRead();
        if (lockTest.isLocked()) {
            ColumnSweepLockInfo lockedColumns = (ColumnSweepLockInfo) lockTest;
            lockedColumns.getLockedColumns(cloudOnlyColumns);
        }
    }

    @Override
    public void onUnpin(ICachedPage page) {
        CloudCachedPage cloudPage = (CloudCachedPage) page;
        cloudPage.afterRead();
    }

    @Override
    public boolean isNewPage() {
        return false;
    }

    @Override
    public boolean incrementStats() {
        return true;
    }

    @Override
    public ByteBuffer processHeader(IOManager ioManager, BufferedFileHandle fileHandle, BufferCacheHeaderHelper header,
            CachedPage cPage, IThreadStats threadStats) throws HyracksDataException {
        // Page zero will be persisted (always) if free space permits
        return readAndPersistPage(ioManager, fileHandle, header, cPage, threadStats, drive.hasSpace());
    }

    @Override
    public ICachedPage pinNext(ColumnBTreeReadLeafFrame leafFrame, IBufferCache bufferCache, int fileId)
            throws HyracksDataException {
        int nextLeaf = leafFrame.getNextLeaf();
        // Release the previous pages
        release(bufferCache);
        /*
         * First pin the next page0. This has to be called before the unpin below to avoid doing
         * readLock().unlock() twice. If unpin called first and pin fails, then the method onUnpin will be
         * called twice on the same pageZero, once when unpin called within this method and another one when the
         * cursor is closed due to the pin failure.
         */
        ICachedPage nextPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, nextLeaf), this);
        // Release page0
        bufferCache.unpin(leafFrame.getPage(), this);
        leafFrame.setPage(nextPage);
        return nextPage;
    }

    @Override
    public void preparePageZeroSegments(ColumnBTreeReadLeafFrame leafFrame, IBufferCache bufferCache, int fileId)
            throws HyracksDataException {
        if (leafFrame.getNumberOfPageZeroSegments() <= 1) { // don't need to include the zeroth segment
            return;
        }

        // pin the required page segments
        //        mergedPageRanges.clear();
        int pageZeroId = leafFrame.getPageId();
        // Pinning all the segments of the page zero
        // as the column eviction logic is based on the length of the columns which
        // gets evaluated from the page zero segments.
        BitSet pageZeroSegmentRanges =
                leafFrame.markRequiredPageZeroSegments(projectedColumns, pageZeroId, operation == MERGE);
        // will unpin the non-required segments after columnRanges.reset()
        // can we do lazily?
        int numberOfPageZeroSegments = leafFrame.getNumberOfPageZeroSegments();
        pinAll(fileId, pageZeroId, numberOfPageZeroSegments - 1, bufferCache);
    }

    @Override
    public void prepareColumns(ColumnBTreeReadLeafFrame leafFrame, IBufferCache bufferCache, int fileId)
            throws HyracksDataException {
        if (leafFrame.getTupleCount() == 0) {
            return;
        }

        columnRanges.reset(leafFrame, projectedColumns, plan, cloudOnlyColumns);
        int pageZeroId = leafFrame.getPageId();
        int numberOfPageZeroSegments = leafFrame.getNumberOfPageZeroSegments();

        if (operation == MERGE) {
            // will contain column pages along with page zero segments
            pinAll(fileId, pageZeroId + numberOfPageZeroSegments - 1,
                    leafFrame.getMegaLeafNodeNumberOfPages() - numberOfPageZeroSegments, bufferCache);
        } else {
            pinProjected(fileId, pageZeroId, bufferCache);
        }
    }

    private void pinAll(int fileId, int pageZeroId, int numberOfPages, IBufferCache bufferCache)
            throws HyracksDataException {
        columnCtx.pin(bufferCache, fileId, pageZeroId, 1, numberOfPages, ALL_PAGES);
    }

    private void pinProjected(int fileId, int pageZeroId, IBufferCache bufferCache) throws HyracksDataException {
        mergedPageRanges.clear();
        int[] columnsOrder = columnRanges.getColumnsOrder();
        int i = 0;
        int columnIndex = columnsOrder[i];
        while (columnIndex > -1) {
            if (columnIndex < columnRanges.getNumberOfPrimaryKeys()) {
                columnIndex = columnsOrder[++i];
                continue;
            }

            int firstPageIdx = columnRanges.getColumnStartPageIndex(columnIndex);
            // last page of the column
            int lastPageIdx = firstPageIdx + columnRanges.getColumnNumberOfPages(columnIndex) - 1;

            // Advance to the next column to check if it has contiguous pages
            columnIndex = columnsOrder[++i];
            while (columnIndex > -1) {
                int sharedPageCount = 0;
                // Get the next column's start page ID
                int nextStartPageIdx = columnRanges.getColumnStartPageIndex(columnIndex);
                if (nextStartPageIdx > lastPageIdx + 1) {
                    // The nextStartPageIdx is not contiguous, stop.
                    break;
                } else if (nextStartPageIdx == lastPageIdx) {
                    // A shared page
                    sharedPageCount = 1;
                }

                lastPageIdx += columnRanges.getColumnNumberOfPages(columnIndex) - sharedPageCount;
                // Advance to the next column
                columnIndex = columnsOrder[++i];
            }

            if (lastPageIdx >= columnRanges.getTotalNumberOfPages()) {
                throw new IndexOutOfBoundsException("lastPageIdx=" + lastPageIdx + ">=" + "megaLeafNodePages="
                        + columnRanges.getTotalNumberOfPages());
            }

            mergedPageRanges.addRange(firstPageIdx, lastPageIdx);
        }

        // pin the calculated pageRanges
        mergedPageRanges.pin(columnCtx, bufferCache, fileId, pageZeroId);
    }

    private void mergePageZeroSegmentRanges(BitSet pageZeroSegmentRanges) {
        // Since the 0th segment is already pinned, we can skip it
        pageZeroSegmentRanges.clear(0);
        if (pageZeroSegmentRanges.cardinality() == 0) {
            // No page zero segments, nothing to merge
            return;
        }

        int start = -1;
        int prev = -1;

        int current = pageZeroSegmentRanges.nextSetBit(0);
        while (current >= 0) {
            if (start == -1) {
                // Start of a new range
                start = current;
            } else if (current != prev + 1) {
                // Discontinuous: close the current range
                mergedPageRanges.addRange(start, prev);
                start = current;
            }

            prev = current;
            current = pageZeroSegmentRanges.nextSetBit(current + 1);
        }

        // Close the final range
        mergedPageRanges.addRange(start, prev);
    }

    @Override
    public void release(IBufferCache bufferCache) throws HyracksDataException {
        // Release might differ in the future if prefetching is supported
        close(bufferCache);
    }

    @Override
    public void close(IBufferCache bufferCache) throws HyracksDataException {
        columnCtx.unpinAll(bufferCache);
        columnCtx.closeStream();
    }
}
