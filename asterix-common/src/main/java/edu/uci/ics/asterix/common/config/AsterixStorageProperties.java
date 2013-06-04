package edu.uci.ics.asterix.common.config;

public class AsterixStorageProperties extends AbstractAsterixProperties {

    private static final String STORAGE_BUFFERCACHE_PAGESIZE_KEY = "storage.buffercache.pagesize";
    private static int STORAGE_BUFFERCACHE_PAGESIZE_DEFAULT = (32 << 10); // 32KB

    private static final String STORAGE_BUFFERCACHE_NUMPAGES_KEY = "storage.buffercache.numpages";
    private static final int STORAGE_BUFFERCACHE_NUMPAGES_DEFAULT = 1024;

    private static final String STORAGE_BUFFERCACHE_MAXOPENFILES_KEY = "storage.buffercache.maxopenfiles";
    private static int STORAGE_BUFFERCACHE_MAXOPENFILES_DEFAULT = Integer.MAX_VALUE;

    private static final String STORAGE_MEMORYCOMPONENT_PAGESIZE_KEY = "storage.memorycomponent.pagesize";
    private static final int STORAGE_MEMORYCOMPONENT_PAGESIZE_DEFAULT = (32 << 10); // 32KB

    private static final String STORAGE_MEMORYCOMPONENT_NUMPAGES_KEY = "storage.memorycomponent.numpages";
    private static final int STORAGE_MEMORYCOMPONENT_NUMPAGES_DEFAULT = 2048; // ... so 64MB components

    private static final String STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_KEY = "storage.memorycomponent.globalbudget";
    private static final long STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_DEFAULT = (1 << 30); // 1GB

    private static final String STORAGE_LSM_MERGETHRESHOLD_KEY = "storage.lsm.mergethreshold";
    private static int STORAGE_LSM_MERGETHRESHOLD_DEFAULT = 3;

    private static final String STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_KEY = "storage.lsm.bloomfilter.falsepositiverate";
    private static double STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_DEFAULT = 0.01;

    public AsterixStorageProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public int getBufferCachePageSize() {
        return accessor.getProperty(STORAGE_BUFFERCACHE_PAGESIZE_KEY, STORAGE_BUFFERCACHE_PAGESIZE_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getBufferCacheNumPages() {
        return accessor.getProperty(STORAGE_BUFFERCACHE_NUMPAGES_KEY, STORAGE_BUFFERCACHE_NUMPAGES_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getBufferCacheMaxOpenFiles() {
        return accessor.getProperty(STORAGE_BUFFERCACHE_MAXOPENFILES_KEY, STORAGE_BUFFERCACHE_MAXOPENFILES_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getMemoryComponentPageSize() {
        return accessor.getProperty(STORAGE_MEMORYCOMPONENT_PAGESIZE_KEY, STORAGE_MEMORYCOMPONENT_PAGESIZE_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getMemoryComponentNumPages() {
        return accessor.getProperty(STORAGE_MEMORYCOMPONENT_NUMPAGES_KEY, STORAGE_MEMORYCOMPONENT_NUMPAGES_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public long getMemoryComponentGlobalBudget() {
        return accessor.getProperty(STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_KEY,
                STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_DEFAULT, PropertyInterpreters.getLongPropertyInterpreter());
    }

    public int getLSMIndexMergeThreshold() {
        return accessor.getProperty(STORAGE_LSM_MERGETHRESHOLD_KEY, STORAGE_LSM_MERGETHRESHOLD_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public double getBloomFilterFalsePositiveRate() {
        return accessor.getProperty(STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_KEY,
                STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_DEFAULT, PropertyInterpreters.getDoublePropertyInterpreter());
    }
}
