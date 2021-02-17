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
import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.LONG_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.KILOBYTE;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.MEGABYTE;

import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.util.StorageUtil;

public class CompilerProperties extends AbstractProperties {

    public enum Option implements IOption {
        COMPILER_SORTMEMORY(
                LONG_BYTE_UNIT,
                StorageUtil.getLongSizeInBytes(32L, MEGABYTE),
                "The memory budget (in bytes) for a sort operator instance in a partition"),
        COMPILER_JOINMEMORY(
                LONG_BYTE_UNIT,
                StorageUtil.getLongSizeInBytes(32L, MEGABYTE),
                "The memory budget (in bytes) for a join operator instance in a partition"),
        COMPILER_GROUPMEMORY(
                LONG_BYTE_UNIT,
                StorageUtil.getLongSizeInBytes(32L, MEGABYTE),
                "The memory budget (in bytes) for a group by operator instance in a partition"),
        COMPILER_WINDOWMEMORY(
                LONG_BYTE_UNIT,
                StorageUtil.getLongSizeInBytes(32L, MEGABYTE),
                "The memory budget (in bytes) for a window operator instance in a partition"),
        COMPILER_TEXTSEARCHMEMORY(
                LONG_BYTE_UNIT,
                StorageUtil.getLongSizeInBytes(32L, MEGABYTE),
                "The memory budget (in bytes) for an inverted-index-search operator instance in a partition"),
        COMPILER_FRAMESIZE(
                INTEGER_BYTE_UNIT,
                StorageUtil.getIntSizeInBytes(32, KILOBYTE),
                "The page size (in bytes) for computation"),
        COMPILER_PARALLELISM(
                INTEGER,
                COMPILER_PARALLELISM_AS_STORAGE,
                "The degree of parallelism for query "
                        + "execution. Zero means to use the storage parallelism as the query execution parallelism, while "
                        + "other integer values dictate the number of query execution parallel partitions. The system will "
                        + "fall back to use the number of all available CPU cores in the cluster as the degree of parallelism "
                        + "if the number set by a user is too large or too small"),
        COMPILER_SORT_PARALLEL(
                BOOLEAN,
                AlgebricksConfig.SORT_PARALLEL_DEFAULT,
                "Enabling/Disabling full parallel sort"),
        COMPILER_SORT_SAMPLES(
                POSITIVE_INTEGER,
                AlgebricksConfig.SORT_SAMPLES_DEFAULT,
                "The number of samples which parallel sorting should take from each partition"),
        COMPILER_INDEXONLY(BOOLEAN, AlgebricksConfig.INDEX_ONLY_DEFAULT, "Enabling/disabling index-only plans"),
        COMPILER_INTERNAL_SANITYCHECK(
                BOOLEAN,
                AlgebricksConfig.SANITYCHECK_DEFAULT,
                "Enable/disable compiler sanity check"),
        COMPILER_EXTERNAL_FIELD_PUSHDOWN(
                BOOLEAN,
                AlgebricksConfig.EXTERNAL_FIELD_PUSHDOWN_DEFAULT,
                "Enable pushdown of field accesses to the external dataset data-scan operator"),
        COMPILER_SUBPLAN_MERGE(
                BOOLEAN,
                AlgebricksConfig.SUBPLAN_MERGE_DEFAULT,
                "Enable merging subplans with other subplans"),
        COMPILER_SUBPLAN_NESTEDPUSHDOWN(
                BOOLEAN,
                AlgebricksConfig.SUBPLAN_NESTEDPUSHDOWN_DEFAULT,
                "When merging subplans into groupby/suplan allow nesting of subplans"),
        COMPILER_MIN_MEMORY_ALLOCATION(
                BOOLEAN,
                AlgebricksConfig.MIN_MEMORY_ALLOCATION_DEFAULT,
                "Enable/disable allocating minimum budget for certain queries");

        private final IOptionType type;
        private final Object defaultValue;
        private final String description;

        Option(IOptionType type, Object defaultValue, String description) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        @Override
        public Section section() {
            return Section.COMMON;
        }

        @Override
        public String description() {
            return description;
        }

        @Override
        public IOptionType type() {
            return type;
        }

        @Override
        public Object defaultValue() {
            return defaultValue;
        }
    }

    public static final String COMPILER_SORTMEMORY_KEY = Option.COMPILER_SORTMEMORY.ini();

    public static final String COMPILER_GROUPMEMORY_KEY = Option.COMPILER_GROUPMEMORY.ini();

    public static final String COMPILER_JOINMEMORY_KEY = Option.COMPILER_JOINMEMORY.ini();

    public static final String COMPILER_WINDOWMEMORY_KEY = Option.COMPILER_WINDOWMEMORY.ini();

    public static final String COMPILER_TEXTSEARCHMEMORY_KEY = Option.COMPILER_TEXTSEARCHMEMORY.ini();

    public static final String COMPILER_PARALLELISM_KEY = Option.COMPILER_PARALLELISM.ini();

    public static final String COMPILER_SORT_PARALLEL_KEY = Option.COMPILER_SORT_PARALLEL.ini();

    public static final String COMPILER_SORT_SAMPLES_KEY = Option.COMPILER_SORT_SAMPLES.ini();

    public static final String COMPILER_INDEXONLY_KEY = Option.COMPILER_INDEXONLY.ini();

    public static final String COMPILER_INTERNAL_SANITYCHECK_KEY = Option.COMPILER_INTERNAL_SANITYCHECK.ini();

    public static final String COMPILER_EXTERNAL_FIELD_PUSHDOWN_KEY = Option.COMPILER_EXTERNAL_FIELD_PUSHDOWN.ini();

    public static final String COMPILER_SUBPLAN_MERGE_KEY = Option.COMPILER_SUBPLAN_MERGE.ini();

    public static final String COMPILER_SUBPLAN_NESTEDPUSHDOWN_KEY = Option.COMPILER_SUBPLAN_NESTEDPUSHDOWN.ini();

    public static final String COMPILER_MIN_MEMORY_ALLOCATION_KEY = Option.COMPILER_MIN_MEMORY_ALLOCATION.ini();

    public static final int COMPILER_PARALLELISM_AS_STORAGE = 0;

    public CompilerProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public long getSortMemorySize() {
        return accessor.getLong(Option.COMPILER_SORTMEMORY);
    }

    public long getJoinMemorySize() {
        return accessor.getLong(Option.COMPILER_JOINMEMORY);
    }

    public long getGroupMemorySize() {
        return accessor.getLong(Option.COMPILER_GROUPMEMORY);
    }

    public long getWindowMemorySize() {
        return accessor.getLong(Option.COMPILER_WINDOWMEMORY);
    }

    public long getTextSearchMemorySize() {
        return accessor.getLong(Option.COMPILER_TEXTSEARCHMEMORY);
    }

    public int getFrameSize() {
        return accessor.getInt(Option.COMPILER_FRAMESIZE);
    }

    public int getParallelism() {
        return accessor.getInt(Option.COMPILER_PARALLELISM);
    }

    public boolean getSortParallel() {
        return accessor.getBoolean(Option.COMPILER_SORT_PARALLEL);
    }

    public int getSortSamples() {
        return accessor.getInt(Option.COMPILER_SORT_SAMPLES);
    }

    public boolean isIndexOnly() {
        return accessor.getBoolean(Option.COMPILER_INDEXONLY);
    }

    public boolean isSanityCheck() {
        return accessor.getBoolean(Option.COMPILER_INTERNAL_SANITYCHECK);
    }

    public boolean isFieldAccessPushdown() {
        return accessor.getBoolean(Option.COMPILER_EXTERNAL_FIELD_PUSHDOWN);
    }

    public boolean getSubplanMerge() {
        return accessor.getBoolean(Option.COMPILER_SUBPLAN_MERGE);
    }

    public boolean getSubplanNestedPushdown() {
        return accessor.getBoolean(Option.COMPILER_SUBPLAN_NESTEDPUSHDOWN);
    }

    public boolean getMinMemoryAllocation() {
        return accessor.getBoolean(Option.COMPILER_MIN_MEMORY_ALLOCATION);
    }
}
