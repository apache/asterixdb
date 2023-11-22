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

import static org.apache.hyracks.control.common.config.OptionTypes.BOOLEAN;
import static org.apache.hyracks.control.common.config.OptionTypes.DOUBLE;
import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.LONG_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.NONNEGATIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.KILOBYTE;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.MEGABYTE;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.common.utils.PartitioningScheme;
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
        STORAGE_BUFFERCACHE_SIZE(LONG_BYTE_UNIT, MAX_HEAP_BYTES / 4),
        STORAGE_BUFFERCACHE_MAXOPENFILES(NONNEGATIVE_INTEGER, Integer.MAX_VALUE),
        STORAGE_MEMORYCOMPONENT_GLOBALBUDGET(LONG_BYTE_UNIT, MAX_HEAP_BYTES / 4),
        STORAGE_MEMORYCOMPONENT_PAGESIZE(INTEGER_BYTE_UNIT, StorageUtil.getIntSizeInBytes(128, KILOBYTE)),
        STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS(POSITIVE_INTEGER, 2),
        STORAGE_MEMORYCOMPONENT_FLUSH_THRESHOLD(DOUBLE, 0.9d),
        STORAGE_MEMORYCOMPONENT_MAX_SCHEDULED_FLUSHES(NONNEGATIVE_INTEGER, 0),
        STORAGE_FILTERED_MEMORYCOMPONENT_MAX_SIZE(LONG_BYTE_UNIT, 0L),
        STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE(DOUBLE, 0.01d),
        STORAGE_COMPRESSION_BLOCK(STRING, "snappy"),
        STORAGE_DISK_FORCE_BYTES(LONG_BYTE_UNIT, StorageUtil.getLongSizeInBytes(16, MEGABYTE)),
        STORAGE_IO_SCHEDULER(STRING, "greedy"),
        STORAGE_WRITE_RATE_LIMIT(LONG_BYTE_UNIT, 0L),
        STORAGE_MAX_CONCURRENT_FLUSHES_PER_PARTITION(NONNEGATIVE_INTEGER, 2),
        STORAGE_MAX_SCHEDULED_MERGES_PER_PARTITION(NONNEGATIVE_INTEGER, 8),
        STORAGE_MAX_CONCURRENT_MERGES_PER_PARTITION(NONNEGATIVE_INTEGER, 2),
        STORAGE_GLOBAL_CLEANUP(BOOLEAN, true),
        STORAGE_GLOBAL_CLEANUP_TIMEOUT(POSITIVE_INTEGER, (int) TimeUnit.MINUTES.toSeconds(10)),
        STORAGE_COLUMN_MAX_TUPLE_COUNT(NONNEGATIVE_INTEGER, 15000),
        STORAGE_COLUMN_FREE_SPACE_TOLERANCE(DOUBLE, 0.15d),
        STORAGE_COLUMN_MAX_LEAF_NODE_SIZE(INTEGER_BYTE_UNIT, StorageUtil.getIntSizeInBytes(10, MEGABYTE)),
        STORAGE_FORMAT(STRING, "row"),
        STORAGE_PARTITIONING(STRING, "dynamic"),
        STORAGE_PARTITIONS_COUNT(INTEGER, 8);

        private final IOptionType interpreter;
        private final Object defaultValue;

        <T> Option(IOptionType<T> interpreter, T defaultValue) {
            this.interpreter = interpreter;
            this.defaultValue = defaultValue;
        }

        @Override
        public Section section() {
            switch (this) {
                case STORAGE_BUFFERCACHE_PAGESIZE:
                case STORAGE_COMPRESSION_BLOCK:
                case STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE:
                case STORAGE_GLOBAL_CLEANUP:
                case STORAGE_GLOBAL_CLEANUP_TIMEOUT:
                case STORAGE_PARTITIONING:
                case STORAGE_PARTITIONS_COUNT:
                case STORAGE_FORMAT:
                case STORAGE_COLUMN_MAX_TUPLE_COUNT:
                case STORAGE_COLUMN_FREE_SPACE_TOLERANCE:
                case STORAGE_COLUMN_MAX_LEAF_NODE_SIZE:
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
                case STORAGE_MEMORYCOMPONENT_MAX_SCHEDULED_FLUSHES:
                    return "The maximum number of scheduled flush operations. 0 means that the value will be "
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
                case STORAGE_MAX_CONCURRENT_FLUSHES_PER_PARTITION:
                    return "The maximum number of concurrently executed flushes per partition (0 means unlimited)";
                case STORAGE_MAX_SCHEDULED_MERGES_PER_PARTITION:
                    return "The maximum number of scheduled merges per partition (0 means unlimited)";
                case STORAGE_MAX_CONCURRENT_MERGES_PER_PARTITION:
                    return "The maximum number of concurrently executed merges per partition (0 means unlimited)";
                case STORAGE_GLOBAL_CLEANUP:
                    return "Indicates whether or not global storage cleanup is performed";
                case STORAGE_GLOBAL_CLEANUP_TIMEOUT:
                    return "The maximum time to wait for nodes to respond to global storage cleanup requests";
                case STORAGE_COLUMN_MAX_TUPLE_COUNT:
                    return "The maximum number of tuples to be stored per a mega leaf page";
                case STORAGE_COLUMN_FREE_SPACE_TOLERANCE:
                    return "The percentage of the maximum tolerable empty space for a physical mega leaf page (e.g.,"
                            + " 0.15 means a physical page with 15% or less empty space is tolerable)";
                case STORAGE_COLUMN_MAX_LEAF_NODE_SIZE:
                    return "The maximum mega leaf node to write during flush and merge operations (default: 10MB)";
                case STORAGE_FORMAT:
                    return "The default storage format (either row or column)";
                case STORAGE_PARTITIONING:
                    return "The storage partitioning scheme (either dynamic or static). This value should not be"
                            + " changed after any dataset has been created";
                case STORAGE_PARTITIONS_COUNT:
                    return "The number of storage partitions to use for static partitioning. This value should not be"
                            + " changed after any dataset has been created";
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

        @Override
        public boolean hidden() {
            return this == STORAGE_GLOBAL_CLEANUP;
        }
    }

    public static final long MAX_HEAP_BYTES = Runtime.getRuntime().maxMemory();
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

    public int getMaxScheduledFlushes() {
        return accessor.getInt(Option.STORAGE_MEMORYCOMPONENT_MAX_SCHEDULED_FLUSHES);
    }

    public long getJobExecutionMemoryBudget() {
        final long jobExecutionMemory = MAX_HEAP_BYTES - getBufferCacheSize() - getMemoryComponentGlobalBudget();
        if (jobExecutionMemory <= 0) {
            final String msg = String.format(
                    "Invalid node memory configuration, more memory budgeted than available in JVM. Runtime max memory:"
                            + " (%d), Buffer cache memory (%d), memory component global budget (%d)",
                    MAX_HEAP_BYTES, getBufferCacheSize(), getMemoryComponentGlobalBudget());
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

    public int geMaxConcurrentFlushes(int numPartitions) {
        int value = accessor.getInt(Option.STORAGE_MAX_CONCURRENT_FLUSHES_PER_PARTITION);
        return value != 0 ? value * numPartitions : Integer.MAX_VALUE;
    }

    public int getMaxScheduledMerges(int numPartitions) {
        int value = accessor.getInt(Option.STORAGE_MAX_SCHEDULED_MERGES_PER_PARTITION);
        return value != 0 ? value * numPartitions : Integer.MAX_VALUE;
    }

    public int getMaxConcurrentMerges(int numPartitions) {
        int value = accessor.getInt(Option.STORAGE_MAX_CONCURRENT_MERGES_PER_PARTITION);
        return value != 0 ? value * numPartitions : Integer.MAX_VALUE;
    }

    public boolean isStorageGlobalCleanup() {
        return accessor.getBoolean(Option.STORAGE_GLOBAL_CLEANUP);
    }

    public int getStorageGlobalCleanupTimeout() {
        return accessor.getInt(Option.STORAGE_GLOBAL_CLEANUP_TIMEOUT);
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

    public int getColumnMaxTupleCount() {
        return accessor.getInt(Option.STORAGE_COLUMN_MAX_TUPLE_COUNT);
    }

    public double getColumnFreeSpaceTolerance() {
        return accessor.getDouble(Option.STORAGE_COLUMN_FREE_SPACE_TOLERANCE);
    }

    public int getColumnMaxLeafNodeSize() {
        return accessor.getInt(Option.STORAGE_COLUMN_MAX_LEAF_NODE_SIZE);
    }

    public String getStorageFormat() {
        return accessor.getString(Option.STORAGE_FORMAT);
    }

    public PartitioningScheme getPartitioningScheme() {
        return PartitioningScheme.fromName(accessor.getString(Option.STORAGE_PARTITIONING));
    }

    public int getStoragePartitionsCount() {
        return accessor.getInt(Option.STORAGE_PARTITIONS_COUNT);
    }
}
