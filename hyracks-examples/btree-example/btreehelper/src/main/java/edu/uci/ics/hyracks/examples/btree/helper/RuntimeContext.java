package edu.uci.ics.hyracks.examples.btree.helper;

import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeRegistry;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class RuntimeContext {
    private static RuntimeContext INSTANCE;
    
    private BTreeRegistry btreeRegistry;
    private IBufferCache bufferCache;
    private IFileMapManager fileMapManager;
        
    private RuntimeContext() {
    }
    
    public static void initialize() {
        if (INSTANCE != null) {
            throw new IllegalStateException("Instance already initialized");
        }
        INSTANCE = new RuntimeContext();
        INSTANCE.start();
    }

    public static void deinitialize() {
        if (INSTANCE != null) {
            INSTANCE.stop();
            INSTANCE = null;
        }
    }

    private void stop() {
        bufferCache.close();
    }

    private void start() {
    	fileMapManager = new SimpleFileMapManager();
    	ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
        bufferCache = new BufferCache(allocator, prs, fileMapManager, 32768, 50);
        btreeRegistry = new BTreeRegistry();
    }

    public static RuntimeContext getInstance() {
        return INSTANCE;
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public IFileMapProvider getFileMapManager() {
        return fileMapManager;
    }
    
    public BTreeRegistry getBTreeRegistry() {
        return btreeRegistry;
    }
}