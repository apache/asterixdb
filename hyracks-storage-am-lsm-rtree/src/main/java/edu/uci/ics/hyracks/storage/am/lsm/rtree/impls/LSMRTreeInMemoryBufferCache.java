package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache.CachedPage;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class LSMRTreeInMemoryBufferCache extends InMemoryBufferCache {

    public LSMRTreeInMemoryBufferCache(ICacheMemoryAllocator allocator, int pageSize, int numPages) {
        super(allocator, pageSize, numPages);
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
