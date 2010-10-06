package edu.uci.ics.hyracks.examples.btree.helper;

import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeRegistry;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.FileMappingProvider;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.FileManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMappingProvider;

public class RuntimeContext {
    private static RuntimeContext INSTANCE;

    private FileManager fileManager;
    private IBufferCache bufferCache;
    private BTreeRegistry btreeRegistry;
    private IFileMappingProvider fileMappingProvider;
    
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
        fileManager.close();
    }

    private void start() {
        fileManager = new FileManager();
        bufferCache = new BufferCache(new HeapBufferAllocator(), new ClockPageReplacementStrategy(), fileManager,
                32768, 1024);
        btreeRegistry = new BTreeRegistry();
        fileMappingProvider = new FileMappingProvider();
    }

    public static RuntimeContext getInstance() {
        return INSTANCE;
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public FileManager getFileManager() {
        return fileManager;
    }

    public BTreeRegistry getBTreeRegistry() {
        return btreeRegistry;
    }
    
    public IFileMappingProvider getFileMappingProvider() {
    	return fileMappingProvider;
    }
}