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

@NotThreadSafe
public final class CloudColumnReadContext implements IColumnReadContext {
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

        // TODO What if every other page is requested. That would do N/2 request, where N is the number of pages.
        // TODO This should be optimized in a way that minimizes the number of requests
        columnRanges.reset(leafFrame, projectedColumns, plan, cloudOnlyColumns);
        int pageZeroId = leafFrame.getPageId();
        int[] columnsOrders = columnRanges.getColumnsOrder();
        int i = 0;
        int columnIndex = columnsOrders[i];
        while (columnIndex > -1) {
            if (columnIndex < columnRanges.getNumberOfPrimaryKeys()) {
                columnIndex = columnsOrders[++i];
                continue;
            }

            int startPageId = columnRanges.getColumnStartPageIndex(columnIndex);
            // Will increment the number pages if the next column's pages are contiguous to this column's pages
            int numberOfPages = columnRanges.getColumnNumberOfPages(columnIndex);

            // Advance to the next column to check if it has contiguous pages
            columnIndex = columnsOrders[++i];
            while (columnIndex > -1) {
                // Get the next column's start page ID
                int nextStartPageId = columnRanges.getColumnStartPageIndex(columnIndex);
                if (nextStartPageId > startPageId + numberOfPages + 1) {
                    // The next startPageId is not contiguous, stop.
                    break;
                }

                // Last page of this column
                int nextLastPage = nextStartPageId + columnRanges.getColumnNumberOfPages(columnIndex);
                // The next column's pages are contiguous. Combine its ranges with the previous one.
                numberOfPages = nextLastPage - startPageId;
                // Advance to the next column
                columnIndex = columnsOrders[++i];
            }

            columnCtx.prepare(numberOfPages);
            pin(bufferCache, fileId, pageZeroId, startPageId, numberOfPages);
        }
    }

    private void pin(IBufferCache bufferCache, int fileId, int pageZeroId, int start, int numOfRequestedPages)
            throws HyracksDataException {
        for (int i = start; i < start + numOfRequestedPages; i++) {
            long dpid = BufferedFileHandle.getDiskPageId(fileId, pageZeroId + i);
            pinnedPages.add(bufferCache.pin(dpid, columnCtx));
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
