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

import static org.apache.hyracks.control.common.config.OptionTypes.DOUBLE;
import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.LONG_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING;
import static org.apache.hyracks.control.common.config.OptionTypes.UNSIGNED_INTEGER;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.KILOBYTE;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.MEGABYTE;

import java.util.function.Function;

import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.StorageUtil;

public class StorageProperties extends AbstractProperties {

    public enum Option implements IOption {
        STORAGE_BUFFERCACHE_PAGESIZE(INTEGER_BYTE_UNIT, StorageUtil.getIntSizeInBytes(128, KILOBYTE)),
        // By default, uses 1/4 of the maximum heap size for read cache, i.e., disk buffer cache.
        STORAGE_BUFFERCACHE_SIZE(LONG_BYTE_UNIT, Runtime.getRuntime().maxMemory() / 4),
        STORAGE_BUFFERCACHE_MAXOPENFILES(UNSIGNED_INTEGER, Integer.MAX_VALUE),
        STORAGE_MEMORYCOMPONENT_GLOBALBUDGET(LONG_BYTE_UNIT, Runtime.getRuntime().maxMemory() / 4),
        STORAGE_MEMORYCOMPONENT_PAGESIZE(INTEGER_BYTE_UNIT, StorageUtil.getIntSizeInBytes(128, KILOBYTE)),
        STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS(POSITIVE_INTEGER, 2),
        STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES(POSITIVE_INTEGER, 8),
        STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE(DOUBLE, 0.01d),
        STORAGE_MAX_ACTIVE_WRITABLE_DATASETS(UNSIGNED_INTEGER, 8),
        STORAGE_COMPRESSION_BLOCK(STRING, "none"),
        STORAGE_DISK_FORCE_BYTES(LONG_BYTE_UNIT, StorageUtil.getLongSizeInBytes(16, MEGABYTE)),
        STORAGE_IO_SCHEDULER(STRING, "greedy");

        private final IOptionType interpreter;
        private final Object defaultValue;

        <T> Option(IOptionType<T> interpreter, T defaultValue) {
            this.interpreter = interpreter;
            this.defaultValue = defaultValue;
        }

        @Override
        public Section section() {
            if (this == STORAGE_MAX_ACTIVE_WRITABLE_DATASETS) {
                return Section.COMMON;
            }
            return Section.NC;
        }

        @Override
        public String description() {
            switch (this) {
                case STORAGE_BUFFERCACHE_PAGESIZE:
                    return "The page size in bytes for pages in the buffer cache";
                case STORAGE_BUFFERCACHE_SIZE:
                    return "The size of memory allocated to the disk buffer cache.  The value should be a multiple"
                            + " of the buffer cache page size.";
                case STORAGE_BUFFERCACHE_MAXOPENFILES:
                    return "The maximum number of open files in the buffer cache";
                case STORAGE_MEMORYCOMPONENT_GLOBALBUDGET:
                    return "The size of memory allocated to the memory components.  The value should be a multiple "
                            + "of the memory component page size";
                case STORAGE_MEMORYCOMPONENT_PAGESIZE:
                    return "The page size in bytes for pages allocated to memory components";
                case STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS:
                    return "The number of memory components to be used per lsm index";
                case STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES:
                    return "The number of pages to allocate for a metadata memory component";
                case STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE:
                    return "The maximum acceptable false positive rate for bloom filters associated with LSM indexes";
                case STORAGE_MAX_ACTIVE_WRITABLE_DATASETS:
                    return "The maximum number of datasets that can be concurrently modified";
                case STORAGE_COMPRESSION_BLOCK:
                    return "The default compression scheme for the storage";
                case STORAGE_DISK_FORCE_BYTES:
                    return "The number of bytes before each disk force (fsync)";
                case STORAGE_IO_SCHEDULER:
                    return "The I/O scheduler for LSM flush and merge operations";
                default:
                    throw new IllegalStateException("NYI: " + this);
            }
        }

        @Override
        public IOptionType type() {
            return interpreter;
        }

        @Override
        public Object defaultValue() {
            return defaultValue;
        }

        @Override
        public String usageDefaultOverride(IApplicationConfig accessor, Function<IOption, String> optionPrinter) {
            if (this == STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES) {
                return "8 pages";
            }
            return null;
        }
    }

    private static final int SYSTEM_RESERVED_DATASETS = 0;

    public StorageProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public int getBufferCachePageSize() {
        return accessor.getInt(Option.STORAGE_BUFFERCACHE_PAGESIZE);
    }

    public long getBufferCacheSize() {
        return accessor.getLong(Option.STORAGE_BUFFERCACHE_SIZE);
    }

    public int getBufferCacheMaxOpenFiles() {
        return accessor.getInt(Option.STORAGE_BUFFERCACHE_MAXOPENFILES);
    }

    public int getMemoryComponentPageSize() {
        return accessor.getInt(Option.STORAGE_MEMORYCOMPONENT_PAGESIZE);
    }

    public int getMemoryComponentNumPages() {
        final long metadataReservedMem = getMetadataReservedMemory();
        final long globalUserDatasetMem = getMemoryComponentGlobalBudget() - metadataReservedMem;
        final long userDatasetMem =
                globalUserDatasetMem / (getMaxActiveWritableDatasets() + geSystemReservedDatasets());
        return (int) (userDatasetMem / getMemoryComponentPageSize());
    }

    public int getMetadataMemoryComponentNumPages() {
        return accessor.getInt(Option.STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES);
    }

    public int getMemoryComponentsNum() {
        return accessor.getInt(Option.STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS);
    }

    public long getMemoryComponentGlobalBudget() {
        return accessor.getLong(Option.STORAGE_MEMORYCOMPONENT_GLOBALBUDGET);
    }

    public double getBloomFilterFalsePositiveRate() {
        return accessor.getDouble(Option.STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE);
    }

    public int getBufferCacheNumPages() {
        return (int) (getBufferCacheSize() / (getBufferCachePageSize() + IBufferCache.RESERVED_HEADER_BYTES));
    }

    public long getJobExecutionMemoryBudget() {
        final long jobExecutionMemory =
                Runtime.getRuntime().maxMemory() - getBufferCacheSize() - getMemoryComponentGlobalBudget();
        if (jobExecutionMemory <= 0) {
            final String msg = String.format(
                    "Invalid node memory configuration, more memory budgeted than available in JVM. Runtime max memory:"
                            + " (%d), Buffer cache memory (%d), memory component global budget (%d)",
                    Runtime.getRuntime().maxMemory(), getBufferCacheSize(), getMemoryComponentGlobalBudget());
            throw new IllegalStateException(msg);
        }
        return jobExecutionMemory;
    }

    public int getMaxActiveWritableDatasets() {
        return accessor.getInt(Option.STORAGE_MAX_ACTIVE_WRITABLE_DATASETS);
    }

    public String getCompressionScheme() {
        return accessor.getString(Option.STORAGE_COMPRESSION_BLOCK);
    }

    public String getIoScheduler() {
        return accessor.getString(Option.STORAGE_IO_SCHEDULER);
    }

    protected int getMetadataDatasets() {
        return MetadataIndexImmutableProperties.METADATA_DATASETS_COUNT;
    }

    protected int geSystemReservedDatasets() {
        return SYSTEM_RESERVED_DATASETS;
    }

    private long getMetadataReservedMemory() {
        return (getMetadataMemoryComponentNumPages() * (long) getMemoryComponentPageSize()) * getMetadataDatasets();
    }

    public int getDiskForcePages() {
        return (int) (accessor.getLong(Option.STORAGE_DISK_FORCE_BYTES) / getBufferCachePageSize());
    }
}
