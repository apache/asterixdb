/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.config;

public class AsterixStorageProperties extends AbstractAsterixProperties {

    private static final String STORAGE_BUFFERCACHE_PAGESIZE_KEY = "storage.buffercache.pagesize";
    private static int STORAGE_BUFFERCACHE_PAGESIZE_DEFAULT = (128 << 10); // 128KB

    private static final String STORAGE_BUFFERCACHE_SIZE_KEY = "storage.buffercache.size";
    private static final long STORAGE_BUFFERCACHE_SIZE_DEFAULT = (512 << 20); // 512 MB

    private static final String STORAGE_BUFFERCACHE_MAXOPENFILES_KEY = "storage.buffercache.maxopenfiles";
    private static int STORAGE_BUFFERCACHE_MAXOPENFILES_DEFAULT = Integer.MAX_VALUE;

    private static final String STORAGE_MEMORYCOMPONENT_PAGESIZE_KEY = "storage.memorycomponent.pagesize";
    private static final int STORAGE_MEMORYCOMPONENT_PAGESIZE_DEFAULT = (128 << 10); // 128KB

    private static final String STORAGE_MEMORYCOMPONENT_NUMPAGES_KEY = "storage.memorycomponent.numpages";
    private static final int STORAGE_MEMORYCOMPONENT_NUMPAGES_DEFAULT = 256; // ... so 32MB components

    private static final String STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES_KEY = "storage.metadata.memorycomponent.numpages";
    private static final int STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES_DEFAULT = 256; // ... so 32MB components

    private static final String STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_KEY = "storage.memorycomponent.numcomponents";
    private static final int STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_DEFAULT = 2; // 2 components

    private static final String STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_KEY = "storage.memorycomponent.globalbudget";
    private static final long STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_DEFAULT = 536870912; // 512MB

    private static final String STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_KEY = "storage.lsm.bloomfilter.falsepositiverate";
    private static double STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_DEFAULT = 0.01;

    public AsterixStorageProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public int getBufferCachePageSize() {
        return accessor.getProperty(STORAGE_BUFFERCACHE_PAGESIZE_KEY, STORAGE_BUFFERCACHE_PAGESIZE_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public long getBufferCacheSize() {
        return accessor.getProperty(STORAGE_BUFFERCACHE_SIZE_KEY, STORAGE_BUFFERCACHE_SIZE_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    public int getBufferCacheNumPages() {
        return (int) (getBufferCacheSize() / getBufferCachePageSize());
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

    public int getMetadataMemoryComponentNumPages() {
        return accessor
                .getProperty(STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES_KEY,
                        STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES_DEFAULT,
                        PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getMemoryComponentsNum() {
        return accessor.getProperty(STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_KEY,
                STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_DEFAULT, PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public long getMemoryComponentGlobalBudget() {
        return accessor.getProperty(STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_KEY,
                STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_DEFAULT, PropertyInterpreters.getLongPropertyInterpreter());
    }

    public double getBloomFilterFalsePositiveRate() {
        return accessor.getProperty(STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_KEY,
                STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_DEFAULT, PropertyInterpreters.getDoublePropertyInterpreter());
    }
}