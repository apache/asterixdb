package edu.uci.ics.asterix.common.context;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import edu.uci.ics.asterix.common.api.IAsterixPropertiesProxy;
import edu.uci.ics.asterix.common.config.AsterixProperties;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;
import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushController;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ConstantMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.FlushController;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ImmediateScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ReferenceCountingOperationTracker;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepositoryFactory;
import edu.uci.ics.hyracks.storage.common.file.PersistentLocalResourceRepositoryFactory;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactoryProvider;

public class AsterixAppRuntimeContext {
    private static final int DEFAULT_BUFFER_CACHE_PAGE_SIZE = 32768;
    private static final int DEFAULT_LIFECYCLEMANAGER_MEMORY_BUDGET = 1024 * 1024 * 1024; // 1GB
    private final INCApplicationContext ncApplicationContext;

    private IIOManager ioManager;
    private IIndexLifecycleManager indexLifecycleManager;
    private IFileMapManager fileMapManager;
    private IBufferCache bufferCache;
    private TransactionProvider provider;

    private ILSMFlushController flushController;
    private ILSMMergePolicy mergePolicy;
    private ILSMOperationTracker opTracker;
    private ILSMIOOperationScheduler lsmIOScheduler;
    private ILocalResourceRepository localResourceRepository;
    private ResourceIdFactory resourceIdFactory;

    public AsterixAppRuntimeContext(INCApplicationContext ncApplicationContext) {
        this.ncApplicationContext = ncApplicationContext;
    }

    public void initialize() throws AsterixException, IOException, ACIDException {
        int pageSize = getBufferCachePageSize();
        int numPages = getBufferCacheNumPages();

        // Create a new IOManager based on the I/O device paths given in the Asterix .properties configuration file.
        IAsterixPropertiesProxy proxy = (IAsterixPropertiesProxy) ncApplicationContext.getDistributedState();
        AsterixProperties asterixProperties = proxy.getAsterixProperties();
        String[] ioDevicePaths = asterixProperties.getIODevicePaths(ncApplicationContext.getNodeId());
        if (ioDevicePaths == null) {
            throw new AsterixException("I/O devices not configured for Node Controller '" + ncApplicationContext.getNodeId() + "'."); 
        }
        ioManager = new IOManager(getDevices(ioDevicePaths), Executors.newCachedThreadPool());
        
        fileMapManager = new AsterixFileMapManager();
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
        
        bufferCache = new BufferCache(ioManager, allocator, prs, fileMapManager, pageSize, numPages, Integer.MAX_VALUE);
        indexLifecycleManager = new IndexLifecycleManager(DEFAULT_LIFECYCLEMANAGER_MEMORY_BUDGET);
        provider = new TransactionProvider(ncApplicationContext.getNodeId());

        flushController = new FlushController();
        lsmIOScheduler = ImmediateScheduler.INSTANCE;
        mergePolicy = new ConstantMergePolicy(lsmIOScheduler, 3);
        opTracker = new ReferenceCountingOperationTracker();

        ILocalResourceRepositoryFactory persistentLocalResourceRepositoryFactory = new PersistentLocalResourceRepositoryFactory(
                ioManager);
        localResourceRepository = persistentLocalResourceRepositoryFactory.createRepository();
        resourceIdFactory = (new ResourceIdFactoryProvider(localResourceRepository)).createResourceIdFactory();
    }

    private int getBufferCachePageSize() {
        int pageSize = DEFAULT_BUFFER_CACHE_PAGE_SIZE;
        String pageSizeStr = System.getProperty(GlobalConfig.BUFFER_CACHE_PAGE_SIZE_PROPERTY, null);
        if (pageSizeStr != null) {
            try {
                pageSize = Integer.parseInt(pageSizeStr);
            } catch (NumberFormatException nfe) {
                if (GlobalConfig.ASTERIX_LOGGER.isLoggable(Level.WARNING)) {
                    GlobalConfig.ASTERIX_LOGGER.warning("Wrong buffer cache page size argument. "
                            + "Using default value: " + pageSize);
                }
            }
        }

        if (GlobalConfig.ASTERIX_LOGGER.isLoggable(Level.INFO)) {
            GlobalConfig.ASTERIX_LOGGER.info("Buffer cache page size: " + pageSize);
        }

        return pageSize;
    }

    private int getBufferCacheNumPages() {
        int numPages = GlobalConfig.DEFAULT_BUFFER_CACHE_NUM_PAGES;
        String numPagesStr = System.getProperty(GlobalConfig.BUFFER_CACHE_NUM_PAGES_PROPERTY, null);
        if (numPagesStr != null) {
            try {
                numPages = Integer.parseInt(numPagesStr);
            } catch (NumberFormatException nfe) {
                if (GlobalConfig.ASTERIX_LOGGER.isLoggable(Level.WARNING)) {
                    GlobalConfig.ASTERIX_LOGGER.warning("Wrong buffer cache size argument. " + "Using default value: "
                            + numPages);
                }
                return numPages;
            }
        }

        if (GlobalConfig.ASTERIX_LOGGER.isLoggable(Level.INFO)) {
            GlobalConfig.ASTERIX_LOGGER.info("Buffer cache size (number of pages): " + numPages);
        }

        return numPages;
    }

    private List<IODeviceHandle> getDevices(String[] ioDevicePaths) {
        List<IODeviceHandle> devices = new ArrayList<IODeviceHandle>();
        for (String ioDevStr : ioDevicePaths) {
            devices.add(new IODeviceHandle(new File(ioDevStr), "."));
        }
        return devices;
    }
    
    public void deinitialize() {
        bufferCache.close();
    }

    public IIOManager getIOManager() {
        return ioManager;
    }
    
    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public IFileMapProvider getFileMapManager() {
        return fileMapManager;
    }

    public TransactionProvider getTransactionProvider() {
        return provider;
    }

    public IIndexLifecycleManager getIndexLifecycleManager() {
        return indexLifecycleManager;
    }

    public ILSMFlushController getFlushController() {
        return flushController;
    }

    public ILSMMergePolicy getLSMMergePolicy() {
        return mergePolicy;
    }

    public ILSMOperationTracker getLSMOperationTracker() {
        return opTracker;
    }

    public ILSMIOOperationScheduler getLSMIOScheduler() {
        return lsmIOScheduler;
    }

    public ILocalResourceRepository getLocalResourceRepository() {
        return localResourceRepository;
    }

    public ResourceIdFactory getResourceIdFactory() {
        return resourceIdFactory;
    }
}