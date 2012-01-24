package edu.uci.ics.hyracks.storage.am.lsmtree.rtree.impls;

import edu.uci.ics.hyracks.storage.am.lsmtree.common.impls.InMemoryBufferCache;
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

        if (pageId == 0 || pageId == 1) {
            return pages[pageId + 2 * fileId];
        } else {
            return pages[pageId];
        }
    }

}
