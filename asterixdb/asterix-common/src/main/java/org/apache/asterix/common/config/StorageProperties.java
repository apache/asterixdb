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
import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER;
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
        STORAGE_MEMORYCOMPONENT_FLUSH_THRESHOLD(DOUBLE, 0.9d),
        STORAGE_MEMORYCOMPONENT_MAX_CONCURRENT_FLUSHES(INTEGER, 0),
        STORAGE_FILTERED_MEMORYCOMPONENT_MAX_SIZE(LONG_BYTE_UNIT, 0L),
        STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE(DOUBLE, 0.01d),
        STORAGE_COMPRESSION_BLOCK(STRING, "snappy"),
        STORAGE_DISK_FORCE_BYTES(LONG_BYTE_UNIT, StorageUtil.getLongSizeInBytes(16, MEGABYTE)),
        STORAGE_IO_SCHEDULER(STRING, "greedy"),
        STORAGE_WRITE_RATE_LIMIT(LONG_BYTE_UNIT, 0l);

        private final IOptionType interpreter;
        private final Object defaultValue;

        <T> Option(IOptionType<T> interpreter, T defaultValue) {
            this.interpreter = interpreter;
            this.defaultValue = defaultValue;
        }

        @Override
        public Section section() {
            switch (this) {
                case STORAGE_COMPRESSION_BLOCK:
                case STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE:
                    return Section.COMMON;
                default:
                    return Section.NC;
            }
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
                case STORAGE_MEMORYCOMPONENT_MAX_CONCURRENT_FLUSHES:
                    return "The maximum number of concurrent flush operations. 0 means that the value will be "
                            + "calculated as the number of partitions";
                case STORAGE_MEMORYCOMPONENT_FLUSH_THRESHOLD:
                    return "The memory usage threshold when memory components should be flushed";
                case STORAGE_FILTERED_MEMORYCOMPONENT_MAX_SIZE:
                    return "The maximum size of a filtered memory component. 0 means that the memory component "
                            + "does not have a maximum size";
                case STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE:
                    return "The maximum acceptable false positive rate for bloom filters associated with LSM indexes";
                case STORAGE_COMPRESSION_BLOCK:
                    return "The default compression scheme for the storage";
                case STORAGE_WRITE_RATE_LIMIT:
                    return "The maximum disk write rate (bytes/s) for each storage partition (disabled if the provided value <= 0)";
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

    public double getMemoryComponentFlushThreshold() {
        return accessor.getDouble(Option.STORAGE_MEMORYCOMPONENT_FLUSH_THRESHOLD);
    }

    public int getFilteredMemoryComponentMaxNumPages() {
        return (int) (accessor.getLong(Option.STORAGE_FILTERED_MEMORYCOMPONENT_MAX_SIZE)
                / getMemoryComponentPageSize());
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

    public int getMaxConcurrentFlushes() {
        return accessor.getInt(Option.STORAGE_MEMORYCOMPONENT_MAX_CONCURRENT_FLUSHES);
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

    public long getWriteRateLimit() {
        return accessor.getLong(Option.STORAGE_WRITE_RATE_LIMIT);
    }

    public int getDiskForcePages() {
        return (int) (accessor.getLong(Option.STORAGE_DISK_FORCE_BYTES) / getBufferCachePageSize());
    }
}
