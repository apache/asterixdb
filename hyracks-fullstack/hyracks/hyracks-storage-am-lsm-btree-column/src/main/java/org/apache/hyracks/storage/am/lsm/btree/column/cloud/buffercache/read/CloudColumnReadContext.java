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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

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
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.annotations.NotThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@NotThreadSafe
public final class CloudColumnReadContext implements IColumnReadContext {
    private static final Logger LOGGER = LogManager.getLogger();
    private final ColumnProjectorType operation;
    private final IPhysicalDrive drive;
    private final BitSet plan;
    private final BitSet cloudOnlyColumns;
    private final ColumnRanges columnRanges;
    private final CloudMegaPageReadContext columnCtx;
    private final List<ICachedPage> pinnedPages;
    private final BitSet projectedColumns;

    public CloudColumnReadContext(IColumnProjectionInfo projectionInfo, IPhysicalDrive drive, BitSet plan) {
        this.operation = projectionInfo.getProjectorType();
        this.drive = drive;
        this.plan = plan;
        columnRanges = new ColumnRanges(projectionInfo.getNumberOfPrimaryKeys());
        cloudOnlyColumns = new BitSet();
        columnCtx = new CloudMegaPageReadContext(operation, columnRanges, drive);
        pinnedPages = new ArrayList<>();
        projectedColumns = new BitSet();
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
            CachedPage cPage) throws HyracksDataException {
        // Page zero will be persisted (always) if free space permits
        return readAndPersistPage(ioManager, fileHandle, header, cPage, drive.hasSpace());
    }

    @Override
    public ICachedPage pinNext(ColumnBTreeReadLeafFrame leafFrame, IBufferCache bufferCache, int fileId)
            throws HyracksDataException {
        int nextLeaf = leafFrame.getNextLeaf();
        // Release the previous pages (including page0)
        release(bufferCache);
        bufferCache.unpin(leafFrame.getPage(), this);

        // pin the next page0
        ICachedPage nextPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, nextLeaf), this);
        leafFrame.setPage(nextPage);
        return nextPage;
    }

    @Override
    public void prepareColumns(ColumnBTreeReadLeafFrame leafFrame, IBufferCache bufferCache, int fileId)
            throws HyracksDataException {
        if (leafFrame.getTupleCount() == 0) {
            return;
        }

        columnRanges.reset(leafFrame, projectedColumns, plan, cloudOnlyColumns);
        int pageZeroId = leafFrame.getPageId();

        if (operation == MERGE) {
            pinAll(fileId, pageZeroId, leafFrame.getMegaLeafNodeNumberOfPages() - 1, bufferCache);
        } else {
            pinProjected(fileId, pageZeroId, bufferCache);
        }
    }

    private void pinAll(int fileId, int pageZeroId, int numberOfPages, IBufferCache bufferCache)
            throws HyracksDataException {
        columnCtx.prepare(numberOfPages);
        pin(bufferCache, fileId, pageZeroId, 1, numberOfPages);
    }

    private void pinProjected(int fileId, int pageZeroId, IBufferCache bufferCache) throws HyracksDataException {
        // TODO What if every other page is requested. That would do N/2 request, where N is the number of pages.
        // TODO This should be optimized in a way that minimizes the number of requests

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

            int numberOfPages = lastPageIdx - firstPageIdx + 1;
            columnCtx.prepare(numberOfPages);
            pin(bufferCache, fileId, pageZeroId, firstPageIdx, numberOfPages);
        }
    }

    private void pin(IBufferCache bufferCache, int fileId, int pageZeroId, int start, int numberOfPages)
            throws HyracksDataException {
        for (int i = start; i < start + numberOfPages; i++) {
            long dpid = BufferedFileHandle.getDiskPageId(fileId, pageZeroId + i);
            try {
                pinnedPages.add(bufferCache.pin(dpid, columnCtx));
            } catch (Throwable e) {
                LOGGER.error("Error while pinning page number {} with number of pages {}. {}\n columnRanges:\n {}", i,
                        numberOfPages, columnCtx, columnRanges);
                throw e;
            }

        }
    }

    @Override
    public void release(IBufferCache bufferCache) throws HyracksDataException {
        // Release might differ in the future if prefetching is supported
        close(bufferCache);
    }

    @Override
    public void close(IBufferCache bufferCache) throws HyracksDataException {
        release(pinnedPages, bufferCache, columnCtx);
        columnCtx.close();
    }

    private static void release(List<ICachedPage> pinnedPages, IBufferCache bufferCache, IBufferCacheReadContext ctx)
            throws HyracksDataException {
        for (int i = 0; i < pinnedPages.size(); i++) {
            bufferCache.unpin(pinnedPages.get(i), ctx);
        }
        pinnedPages.clear();
    }
}
