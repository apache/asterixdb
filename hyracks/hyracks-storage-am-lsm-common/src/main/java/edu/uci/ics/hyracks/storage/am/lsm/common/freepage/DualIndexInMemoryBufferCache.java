/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.common.freepage;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.TransientFileMapManager;

/**
 * In-memory buffer cache that supports two tree indexes.
 * We assume that the tree indexes have 2 fixed pages, one at index 0 (metadata page), and one at index 1 (root page).
 */
public class DualIndexInMemoryBufferCache extends InMemoryBufferCache {

    public DualIndexInMemoryBufferCache(ICacheMemoryAllocator allocator, int pageSize, int numPages) {
        super(allocator, pageSize, numPages, new TransientFileMapManager());
    }

    @Override
    public ICachedPage pin(long dpid, boolean newPage) {
        int pageId = BufferedFileHandle.getPageId(dpid);
        int fileId = BufferedFileHandle.getFileId(dpid);
        if (pageId < pages.length) {
            // Common case: Return regular page.
            if (pageId == 0 || pageId == 1) {
                return pages[pageId + 2 * fileId];
            } else {
                return pages[pageId];
            }
        } else {
            // Rare case: Return overflow page, possibly expanding overflow
            // array.
            synchronized (overflowPages) {
                int numNewPages = pageId - pages.length - overflowPages.size() + 1;
                if (numNewPages > 0) {
                    ByteBuffer[] buffers = allocator.allocate(pageSize, numNewPages);
                    for (int i = 0; i < numNewPages; i++) {
                        CachedPage overflowPage = new CachedPage(pages.length + overflowPages.size(), buffers[i]);
                        overflowPages.add(overflowPage);
                    }
                }
                return overflowPages.get(pageId - pages.length);
            }
        }
    }

}
