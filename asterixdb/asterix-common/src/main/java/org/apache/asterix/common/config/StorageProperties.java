/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.common.config;

import static org.apache.hyracks.util.StorageUtil.StorageUnit.KILOBYTE;

import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.StorageUtil;

public class StorageProperties extends AbstractProperties {

    private static final String STORAGE_BUFFERCACHE_PAGESIZE_KEY = "storage.buffercache.pagesize";
    private static final int STORAGE_BUFFERCACHE_PAGESIZE_DEFAULT = StorageUtil.getSizeInBytes(128, KILOBYTE);

    private static final String STORAGE_BUFFERCACHE_SIZE_KEY = "storage.buffercache.size";

    private static final String STORAGE_BUFFERCACHE_MAXOPENFILES_KEY = "storage.buffercache.maxopenfiles";
    private static final int STORAGE_BUFFERCACHE_MAXOPENFILES_DEFAULT = Integer.MAX_VALUE;

    private static final String STORAGE_MEMORYCOMPONENT_PAGESIZE_KEY = "storage.memorycomponent.pagesize";
    private static final int STORAGE_MEMORYCOMPONENT_PAGESIZE_DEFAULT = StorageUtil.getSizeInBytes(128, KILOBYTE);

    private static final String STORAGE_MEMORYCOMPONENT_NUMPAGES_KEY = "storage.memorycomponent.numpages";

    private static final String STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES_KEY =
            "storage.metadata.memorycomponent.numpages";

    private static final String STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_KEY = "storage.memorycomponent.numcomponents";
    private static final int STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_DEFAULT = 2; // 2 components

    private static final String STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_KEY = "storage.memorycomponent.globalbudget";

    private static final String STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_KEY =
            "storage.lsm.bloomfilter.falsepositiverate";
    private static final double STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_DEFAULT = 0.01;

    private final long storageBufferCacheSizeDefault;
    private final int storageMemoryComponentNumPages;
    private final int storageMetadataMemoryComponentNumPages;
    private final long storageMemorycomponentGlobalbudgetDefault;

    public StorageProperties(PropertiesAccessor accessor) {
        super(accessor);

        // Gets the -Xmx value for the JVM.
        long maxHeapSize = Runtime.getRuntime().maxMemory();
        // By default, uses 1/4 of the maximum heap size for read cache, i.e., disk buffer cache.
        storageBufferCacheSizeDefault = maxHeapSize / 4;
        // By default, uses 1/4 of the maximum heap size for the write buffer, i.e., globalbudget for memory components.
        storageMemorycomponentGlobalbudgetDefault = maxHeapSize / 4;
        // By default, uses 1/16 of the storageMemorycomponentGlobalbudgetDefault for the write buffer budget
        // for a dataset, including data and indexes.
        storageMemoryComponentNumPages = (int) (storageMemorycomponentGlobalbudgetDefault
                / (16 * getMemoryComponentPageSize()));
        // By default, uses the min of 1/64 of the storageMemorycomponentGlobalbudgetDefault and 256 pages
        // for the write buffer budget for a metadata dataset, including data and indexes.
        storageMetadataMemoryComponentNumPages = Math
                .min((int) (storageMemorycomponentGlobalbudgetDefault / (64 * getMemoryComponentPageSize())), 256);
    }

    @PropertyKey(STORAGE_BUFFERCACHE_PAGESIZE_KEY)
    public int getBufferCachePageSize() {
        return accessor.getProperty(STORAGE_BUFFERCACHE_PAGESIZE_KEY, STORAGE_BUFFERCACHE_PAGESIZE_DEFAULT,
                PropertyInterpreters.getIntegerBytePropertyInterpreter());
    }

    @PropertyKey(STORAGE_BUFFERCACHE_SIZE_KEY)
    public long getBufferCacheSize() {
        return accessor.getProperty(STORAGE_BUFFERCACHE_SIZE_KEY, storageBufferCacheSizeDefault,
                PropertyInterpreters.getLongBytePropertyInterpreter());
    }

    public int getBufferCacheNumPages() {
        return (int) (getBufferCacheSize() / (getBufferCachePageSize() + IBufferCache.RESERVED_HEADER_BYTES));
    }

    @PropertyKey(STORAGE_BUFFERCACHE_MAXOPENFILES_KEY)
    public int getBufferCacheMaxOpenFiles() {
        return accessor.getProperty(STORAGE_BUFFERCACHE_MAXOPENFILES_KEY, STORAGE_BUFFERCACHE_MAXOPENFILES_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    @PropertyKey(STORAGE_MEMORYCOMPONENT_PAGESIZE_KEY)
    public int getMemoryComponentPageSize() {
        return accessor.getProperty(STORAGE_MEMORYCOMPONENT_PAGESIZE_KEY, STORAGE_MEMORYCOMPONENT_PAGESIZE_DEFAULT,
                PropertyInterpreters.getIntegerBytePropertyInterpreter());
    }

    @PropertyKey(STORAGE_MEMORYCOMPONENT_NUMPAGES_KEY)
    public int getMemoryComponentNumPages() {
        return accessor.getProperty(STORAGE_MEMORYCOMPONENT_NUMPAGES_KEY, storageMemoryComponentNumPages,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    @PropertyKey(STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES_KEY)
    public int getMetadataMemoryComponentNumPages() {
        return accessor.getProperty(STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES_KEY,
                storageMetadataMemoryComponentNumPages, PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    @PropertyKey(STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_KEY)
    public int getMemoryComponentsNum() {
        return accessor.getProperty(STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_KEY,
                STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_DEFAULT, PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    @PropertyKey(STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_KEY)
    public long getMemoryComponentGlobalBudget() {
        return accessor.getProperty(STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_KEY, storageMemorycomponentGlobalbudgetDefault,
                PropertyInterpreters.getLongBytePropertyInterpreter());
    }

    @PropertyKey(STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_KEY)
    public double getBloomFilterFalsePositiveRate() {
        return accessor.getProperty(STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_KEY,
                STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_DEFAULT, PropertyInterpreters.getDoublePropertyInterpreter());
    }
}
