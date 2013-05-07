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
    private static final int STORAGE_MEMORYCOMPONENT_NUMPAGES_DEFAULT = 4096; // ... so 128MB components

    private static final String STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_KEY = "storage.memorycomponent.globalbudget";
    private static final int STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_DEFAULT = (1 << 30); // 1GB

    private static final String STORAGE_LSM_MERGETHRESHOLD_KEY = "storage.lsm.mergethreshold";
    private static int STORAGE_LSM_MERGETHRESHOLD_DEFAULT = 3;

    public AsterixStorageProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public int getBufferCachePageSize() {
        return accessor.getInt(STORAGE_BUFFERCACHE_PAGESIZE_KEY, STORAGE_BUFFERCACHE_PAGESIZE_DEFAULT);
    }

    public int getBufferCacheNumPages() {
        return accessor.getInt(STORAGE_BUFFERCACHE_NUMPAGES_KEY, STORAGE_BUFFERCACHE_NUMPAGES_DEFAULT);
    }

    public int getBufferCacheMaxOpenFiles() {
        return accessor.getInt(STORAGE_BUFFERCACHE_MAXOPENFILES_KEY, STORAGE_BUFFERCACHE_MAXOPENFILES_DEFAULT);
    }

    public int getMemoryComponentPageSize() {
        return accessor.getInt(STORAGE_MEMORYCOMPONENT_PAGESIZE_KEY, STORAGE_MEMORYCOMPONENT_PAGESIZE_DEFAULT);
    }

    public int getMemoryComponentNumPages() {
        return accessor.getInt(STORAGE_MEMORYCOMPONENT_NUMPAGES_KEY, STORAGE_MEMORYCOMPONENT_NUMPAGES_DEFAULT);
    }

    public int getMemoryComponentGlobalBudget() {
        return accessor.getInt(STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_KEY, STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_DEFAULT);
    }

    public int getLSMIndexMergeThreshold() {
        return accessor.getInt(STORAGE_LSM_MERGETHRESHOLD_KEY, STORAGE_LSM_MERGETHRESHOLD_DEFAULT);
    }
}
