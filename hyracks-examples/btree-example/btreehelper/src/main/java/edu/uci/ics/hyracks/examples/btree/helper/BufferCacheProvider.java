package edu.uci.ics.hyracks.examples.btree.helper;

import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBufferCacheProvider;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.FileManager;

public class BufferCacheProvider implements IBufferCacheProvider {
    private static final long serialVersionUID = 1L;

    public static final BufferCacheProvider INSTANCE = new BufferCacheProvider();

    private BufferCacheProvider() {
    }

    @Override
    public IBufferCache getBufferCache() {
        return RuntimeContext.getInstance().getBufferCache();
    }

    @Override
    public FileManager getFileManager() {
        return RuntimeContext.getInstance().getFileManager();
    }
}