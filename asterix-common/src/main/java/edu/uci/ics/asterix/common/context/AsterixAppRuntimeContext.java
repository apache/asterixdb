package edu.uci.ics.asterix.common.context;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.exceptions.AsterixRuntimeException;
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
    private static AsterixAppRuntimeContext INSTANCE;

    private IndexRegistry<IIndex> treeRegistry;
    private IBufferCache bufferCache;
    private IFileMapManager fileMapManager;
    private INCApplicationContext ncAppContext;

    private static Logger LOGGER = Logger.getLogger(AsterixAppRuntimeContext.class.getName());

    private AsterixAppRuntimeContext() {
    }

    public static void initialize(INCApplicationContext ncAppContext) throws IOException {
        if (INSTANCE != null) {
            LOGGER.info("Asterix instance already initialized");
            return;
        }

        INSTANCE = new AsterixAppRuntimeContext();
        INSTANCE.ncAppContext = ncAppContext;
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

    private void start() throws IOException {
        fileMapManager = new AsterixFileMapManager();
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
        if (ncAppContext == null) {
            throw new AsterixRuntimeException("NC Application Context has not been set.");
        }
        IIOManager ioMgr = ncAppContext.getRootContext().getIOManager();
        String pgsizeStr = System.getProperty(GlobalConfig.BUFFER_CACHE_PAGE_SIZE_PROPERTY);
        int pgSize = -1;
        if (pgsizeStr != null) {
            try {
                pgSize = Integer.parseInt(pgsizeStr);
            } catch (NumberFormatException nfe) {
                StringWriter sw = new StringWriter();
                nfe.printStackTrace(new PrintWriter(sw, true));
                sw.close();
                GlobalConfig.ASTERIX_LOGGER.warning("Wrong buffer cache page size argument. Picking frame size ("
                        + ncAppContext.getRootContext().getFrameSize() + ") instead. \n" + sw.toString() + "\n");
            }
        }
        if (pgSize < 0) {
            // by default, pick the frame size
            pgSize = ncAppContext.getRootContext().getFrameSize();
        }

        int cacheSize = GlobalConfig.DEFAULT_BUFFER_CACHE_SIZE;
        String cacheSizeStr = System.getProperty(GlobalConfig.BUFFER_CACHE_SIZE_PROPERTY);
        if (cacheSizeStr != null) {
            int cs = -1;
            try {
                cs = Integer.parseInt(cacheSizeStr);
            } catch (NumberFormatException nfe) {
                StringWriter sw = new StringWriter();
                nfe.printStackTrace(new PrintWriter(sw, true));
                sw.close();
                GlobalConfig.ASTERIX_LOGGER.warning("Wrong buffer cache size argument. Picking default value ("
                        + GlobalConfig.DEFAULT_BUFFER_CACHE_SIZE + ") instead.\n");
            }
            if (cs >= 0) {
                cacheSize = cs;
            }
        }
        System.out.println("BC :" + pgSize + " cache " + cacheSize);
        bufferCache = new BufferCache(ioMgr, allocator, prs, fileMapManager, pgSize, cacheSize, Integer.MAX_VALUE);
        treeRegistry = new IndexRegistry<IIndex>();
    }

    public static AsterixAppRuntimeContext getInstance() {
        return INSTANCE;
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public IFileMapProvider getFileMapManager() {
        return fileMapManager;
    }

    public IndexRegistry<IIndex> getTreeRegistry() {
        return treeRegistry;
    }

}