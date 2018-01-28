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
import static org.apache.hyracks.util.StorageUtil.StorageUnit.KILOBYTE;

import java.util.function.Function;

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
        STORAGE_BUFFERCACHE_MAXOPENFILES(INTEGER, Integer.MAX_VALUE),
        STORAGE_MEMORYCOMPONENT_GLOBALBUDGET(LONG_BYTE_UNIT, Runtime.getRuntime().maxMemory() / 4),
        STORAGE_MEMORYCOMPONENT_PAGESIZE(INTEGER_BYTE_UNIT, StorageUtil.getIntSizeInBytes(128, KILOBYTE)),
        STORAGE_MEMORYCOMPONENT_NUMPAGES(INTEGER, (Function<IApplicationConfig, Integer>) accessor ->
        // By default, uses 1/16 of the STORAGE_MEMORYCOMPONENT_GLOBALBUDGET for the write buffer
        // budget for a dataset, including data and indexes.
        (int) (accessor.getLong(STORAGE_MEMORYCOMPONENT_GLOBALBUDGET) / (16 * accessor.getInt(STORAGE_MEMORYCOMPONENT_PAGESIZE)))),
        STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS(INTEGER, 2),
        STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES(INTEGER, (Function<IApplicationConfig, Integer>) accessor ->
        // By default, uses the min of 1/64 of the STORAGE_MEMORYCOMPONENT_GLOBALBUDGET and 256 pages
        // for the write buffer budget for a metadata dataset, including data and indexes.
        Math.min((int) (accessor.getLong(STORAGE_MEMORYCOMPONENT_GLOBALBUDGET) / (64 * accessor.getInt(STORAGE_MEMORYCOMPONENT_PAGESIZE))), 256)),
        STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE(DOUBLE, 0.01d);

        private final IOptionType interpreter;
        private final Object defaultValue;

        <T> Option(IOptionType<T> interpreter, T defaultValue) {
            this.interpreter = interpreter;
            this.defaultValue = defaultValue;
        }

        <T> Option(IOptionType<T> interpreter, Function<IApplicationConfig, T> defaultValueFunction) {
            this.interpreter = interpreter;
            this.defaultValue = defaultValueFunction;
        }

        @Override
        public Section section() {
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
                case STORAGE_MEMORYCOMPONENT_NUMPAGES:
                    return "The number of pages to allocate for a memory component.  This budget is shared by all "
                            + "the memory components of the primary index and all its secondary indexes across all I/O "
                            + "devices on a node.  Note: in-memory components usually has fill factor of 75% since "
                            + "the pages are 75% full and the remaining 25% is un-utilized";
                case STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS:
                    return "The number of memory components to be used per lsm index";
                case STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES:
                    return "The number of pages to allocate for a metadata memory component";
                case STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE:
                    return "The maximum acceptable false positive rate for bloom filters associated with LSM indexes";
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
            switch (this) {
                case STORAGE_MEMORYCOMPONENT_NUMPAGES:
                    return "1/16th of the " + optionPrinter.apply(Option.STORAGE_MEMORYCOMPONENT_GLOBALBUDGET)
                            + " value";
                case STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES:
                    return "1/64th of the " + optionPrinter.apply(Option.STORAGE_MEMORYCOMPONENT_GLOBALBUDGET)
                            + " value or 256, whichever is larger";
                default:
                    return null;
            }
        }
    }

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
        return accessor.getInt(Option.STORAGE_MEMORYCOMPONENT_NUMPAGES);
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
}
