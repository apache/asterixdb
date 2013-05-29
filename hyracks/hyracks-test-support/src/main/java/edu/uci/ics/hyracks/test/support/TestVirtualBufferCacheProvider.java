package edu.uci.ics.hyracks.test.support;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;

public class TestVirtualBufferCacheProvider implements IVirtualBufferCacheProvider {

    private static final long serialVersionUID = 1L;

    private final int pageSize;
    private final int numPages;

    public TestVirtualBufferCacheProvider(int pageSize, int numPages) {
        this.pageSize = pageSize;
        this.numPages = numPages;
    }

    @Override
    public IVirtualBufferCache getVirtualBufferCache(IHyracksTaskContext ctx) {
        return new VirtualBufferCache(new HeapBufferAllocator(), pageSize, numPages);
    }
}
