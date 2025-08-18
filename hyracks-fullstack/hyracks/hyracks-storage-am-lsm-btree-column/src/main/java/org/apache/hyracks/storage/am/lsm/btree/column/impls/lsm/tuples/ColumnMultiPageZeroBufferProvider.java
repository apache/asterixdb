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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.tuples;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Queue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongSet;

public class ColumnMultiPageZeroBufferProvider implements IColumnBufferProvider {
    private static final BitSet EMPTY_SEGMENTS = new BitSet();
    private final IColumnReadMultiPageOp multiPageOp;
    private final LongSet pinnedPages;
    private final List<ICachedPage> pages; // stores from segment 1 to segment n (0 is not stored here)

    private int startPage;
    private int numberOfRemainingPages;
    private BitSet pageZeroSegmentsPages;

    public ColumnMultiPageZeroBufferProvider(IColumnReadMultiPageOp multiPageOp, LongSet pinnedPages) {
        this.multiPageOp = multiPageOp;
        this.pinnedPages = pinnedPages;
        this.pages = new ArrayList<>();
    }

    @Override
    public void reset(ColumnBTreeReadLeafFrame frame) throws HyracksDataException {
        startPage = frame.getPageId() + 1;
        numberOfRemainingPages = frame.getNumberOfPageZeroSegments() - 1; // zeroth segment is not counted
        pageZeroSegmentsPages = frame.getPageZeroSegmentsPages();
        if (pageZeroSegmentsPages == null) {
            pageZeroSegmentsPages = EMPTY_SEGMENTS;
        }
    }

    @Override
    public void readAll(Queue<ByteBuffer> buffers) throws HyracksDataException {
        throw new IllegalStateException("Reading all pages is not allowed for zero buffer provider.");
    }

    public int getNumberOfRemainingPages() {
        return numberOfRemainingPages;
    }

    public void readAll(List<ByteBuffer> buffers, Int2IntMap segmentDir) throws HyracksDataException {
        if (pageZeroSegmentsPages == EMPTY_SEGMENTS) {
            // All the segments are expected to present in the flash cache, but still need pinning into buffer cache.
            // will do on request basis? or prefetch all the segments?
            return;
        }
        //Since all the pageSegments are pinned for calculating the lengths of the columns,
        //read all the segments and store them in the buffers list.
        //after ColumnRanges.reset(), unpin the segments that are not required.
        for (int segmentIndex = 0; segmentIndex < numberOfRemainingPages; segmentIndex++) {
            ByteBuffer buffer = read(segmentIndex);
            segmentDir.put(segmentIndex, buffers.size());
            buffers.add(buffer);
        }
    }

    public ByteBuffer read(int segmentIndex) throws HyracksDataException {
        if (segmentIndex < 0 || segmentIndex >= numberOfRemainingPages) {
            throw new IndexOutOfBoundsException("Segment index out of bounds: " + segmentIndex);
        }
        ICachedPage segment = readSegment(segmentIndex);
        return segment.getBuffer();
    }

    @Override
    public void releaseAll() throws HyracksDataException {
        for (ICachedPage page : pages) {
            if (page != null) {
                multiPageOp.unpin(page);
            }
        }
        pages.clear();
    }

    public void releasePages(IntList notRequiredSegmentsIndexes) throws HyracksDataException {
        //From the list of cached pages, remove those pages.
        //Pages and buffers list are in sync, so we can use the same indexes.
        Throwable th = null;
        for (int pageIndex : notRequiredSegmentsIndexes) {
            if (pageIndex < 0 || pageIndex >= pages.size()) {
                throw new IndexOutOfBoundsException("Page index out of bounds: " + pageIndex);
            }
            try {
                ICachedPage page = pages.get(pageIndex);
                if (page != null) {
                    multiPageOp.unpin(page);
                    pinnedPages.remove(((CachedPage) page).getDiskPageId());
                    pages.set(pageIndex, null); // Clear the reference
                }
            } catch (Exception e) {
                if (th == null) {
                    th = e;
                } else {
                    th.addSuppressed(e);
                }
            }
        }
        if (th != null) {
            throw HyracksDataException.create(th);
        }
    }

    @Override
    public ByteBuffer getBuffer() {
        throw new UnsupportedOperationException("getBuffer() is not supported for multi-page zero buffer provider.");
    }

    @Override
    public int getLength() {
        throw new IllegalStateException("Reading all pages is not allowed for zero buffer provider.");
    }

    private ICachedPage readSegment(int segmentIndex) throws HyracksDataException {
        // The page segments are most likely to be present in the buffer cache,
        // as the pages are pinned when a new pageZero is accessed.
        ICachedPage segmentPage = multiPageOp.pin(startPage + segmentIndex);
        pages.add(segmentPage);
        pinnedPages.add(((CachedPage) segmentPage).getDiskPageId());
        return segmentPage;
    }

    @Override
    public int getColumnIndex() {
        throw new IllegalStateException("Reading all pages is not allowed for zero buffer provider.");
    }
}
