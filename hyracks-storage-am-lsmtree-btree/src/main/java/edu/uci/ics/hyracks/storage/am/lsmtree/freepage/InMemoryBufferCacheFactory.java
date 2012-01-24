package edu.uci.ics.hyracks.storage.am.lsmtree.freepage;

import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;

public class InMemoryBufferCacheFactory {
	
	private IBufferCache bufferCache;
    private final int pageSize;
    private final int numPages;
	
    public InMemoryBufferCacheFactory(int pageSize, int numPages) {
    	this.pageSize = pageSize;
    	this.numPages = numPages;
        bufferCache = null;
    }
    
    public synchronized IBufferCache createInMemoryBufferCache() {
        if (bufferCache == null) {
            ICacheMemoryAllocator allocator = new HeapBufferAllocator();
            bufferCache = new InMemoryBufferCache(allocator, pageSize, numPages);
        }
        return bufferCache;
    }
}
