package edu.uci.ics.asterix.common.context;

import java.io.IOException;
import java.util.logging.Level;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexRegistry;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class AsterixAppRuntimeContext {
    private INCApplicationContext ncApplicationContext;
    private IndexRegistry<IIndex> indexRegistry;
    private IFileMapManager fileMapManager;
    private IBufferCache bufferCache;

    public AsterixAppRuntimeContext(INCApplicationContext ncAppContext) {
        this.ncApplicationContext = ncAppContext;
    }

    public void initialize() throws IOException {
        int pageSize = getBufferCachePageSize();
        int cacheSize = getBufferCacheSize();

        // Initialize file map manager
        fileMapManager = new AsterixFileMapManager();

        // Initialize the buffer cache
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
        IIOManager ioMgr = ncApplicationContext.getRootContext().getIOManager();
        bufferCache = new BufferCache(ioMgr, allocator, prs, fileMapManager, pageSize, cacheSize, Integer.MAX_VALUE);

        // Initialize the index registry
        indexRegistry = new IndexRegistry<IIndex>();
    }

    private int getBufferCachePageSize() {
        int pageSize = ncApplicationContext.getRootContext().getFrameSize();
        String pageSizeStr = System.getProperty(GlobalConfig.BUFFER_CACHE_PAGE_SIZE_PROPERTY, null);
        if (pageSizeStr != null) {
            try {
                pageSize = Integer.parseInt(pageSizeStr);
            } catch (NumberFormatException nfe) {
                if (GlobalConfig.ASTERIX_LOGGER.isLoggable(Level.WARNING)) {
                    GlobalConfig.ASTERIX_LOGGER.warning("Wrong buffer cache page size argument. "
                            + "Using default value: " + pageSize);
                }
                return pageSize;
            }
        }

        if (GlobalConfig.ASTERIX_LOGGER.isLoggable(Level.INFO)) {
            GlobalConfig.ASTERIX_LOGGER.info("Buffer cache page size: " + pageSize);
        }

        return pageSize;
    }

    private int getBufferCacheSize() {
        int cacheSize = GlobalConfig.DEFAULT_BUFFER_CACHE_SIZE;
        String cacheSizeStr = System.getProperty(GlobalConfig.BUFFER_CACHE_SIZE_PROPERTY, null);
        if (cacheSizeStr != null) {
            try {
                cacheSize = Integer.parseInt(cacheSizeStr);
            } catch (NumberFormatException nfe) {
                if (GlobalConfig.ASTERIX_LOGGER.isLoggable(Level.WARNING)) {
                    GlobalConfig.ASTERIX_LOGGER.warning("Wrong buffer cache size argument. " + "Using default value: "
                            + cacheSize);
                }
                return cacheSize;
            }
        }

        if (GlobalConfig.ASTERIX_LOGGER.isLoggable(Level.INFO)) {
            GlobalConfig.ASTERIX_LOGGER.info("Buffer cache size: " + cacheSize);
        }

        return cacheSize;
    }

    public void deinitialize() {
        bufferCache.close();
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public IFileMapProvider getFileMapManager() {
        return fileMapManager;
    }

    public IndexRegistry<IIndex> getIndexRegistry() {
        return indexRegistry;
    }

}