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
import static org.apache.hyracks.control.common.config.OptionTypes.LONG_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.NONNEGATIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.GIGABYTE;

import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.cloud.CloudCachePolicy;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.util.StorageUtil;

public class CloudProperties extends AbstractProperties {

    public CloudProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public enum Option implements IOption {
        CLOUD_STORAGE_SCHEME(STRING, ""),
        CLOUD_STORAGE_BUCKET(STRING, ""),
        CLOUD_STORAGE_PREFIX(STRING, ""),
        CLOUD_STORAGE_REGION(STRING, ""),
        CLOUD_STORAGE_ENDPOINT(STRING, ""),
        CLOUD_STORAGE_ANONYMOUS_AUTH(BOOLEAN, false),
        CLOUD_STORAGE_CACHE_POLICY(STRING, "selective"),
        // 80% of the total disk space
        CLOUD_STORAGE_ALLOCATION_PERCENTAGE(DOUBLE, 0.8d),
        // 90% of the allocated space for storage (i.e., 90% of the 80% of the total disk space)
        CLOUD_STORAGE_SWEEP_THRESHOLD_PERCENTAGE(DOUBLE, 0.9d),
        CLOUD_STORAGE_DISK_MONITOR_INTERVAL(POSITIVE_INTEGER, 120),
        CLOUD_STORAGE_INDEX_INACTIVE_DURATION_THRESHOLD(POSITIVE_INTEGER, 360),
        CLOUD_STORAGE_DEBUG_MODE_ENABLED(BOOLEAN, false),
        CLOUD_STORAGE_DEBUG_SWEEP_THRESHOLD_SIZE(LONG_BYTE_UNIT, StorageUtil.getLongSizeInBytes(1, GIGABYTE)),
        CLOUD_PROFILER_LOG_INTERVAL(NONNEGATIVE_INTEGER, 5),
        CLOUD_WRITE_BUFFER_SIZE(POSITIVE_INTEGER, StorageUtil.getIntSizeInBytes(8, StorageUtil.StorageUnit.MEGABYTE));

        private final IOptionType interpreter;
        private final Object defaultValue;

        <T> Option(IOptionType<T> interpreter, T defaultValue) {
            this.interpreter = interpreter;
            this.defaultValue = defaultValue;
        }

        @Override
        public Section section() {
            switch (this) {
                case CLOUD_STORAGE_SCHEME:
                case CLOUD_STORAGE_BUCKET:
                case CLOUD_STORAGE_PREFIX:
                case CLOUD_STORAGE_REGION:
                case CLOUD_STORAGE_ENDPOINT:
                case CLOUD_STORAGE_ANONYMOUS_AUTH:
                case CLOUD_STORAGE_CACHE_POLICY:
                case CLOUD_STORAGE_ALLOCATION_PERCENTAGE:
                case CLOUD_STORAGE_SWEEP_THRESHOLD_PERCENTAGE:
                case CLOUD_STORAGE_DISK_MONITOR_INTERVAL:
                case CLOUD_STORAGE_INDEX_INACTIVE_DURATION_THRESHOLD:
                case CLOUD_STORAGE_DEBUG_SWEEP_THRESHOLD_SIZE:
                case CLOUD_STORAGE_DEBUG_MODE_ENABLED:
                case CLOUD_PROFILER_LOG_INTERVAL:
                case CLOUD_WRITE_BUFFER_SIZE:
                    return Section.COMMON;
                default:
                    return Section.NC;
            }
        }

        @Override
        public String description() {
            switch (this) {
                case CLOUD_STORAGE_SCHEME:
                    return "The cloud storage scheme e.g. (s3)";
                case CLOUD_STORAGE_BUCKET:
                    return "The cloud storage bucket name";
                case CLOUD_STORAGE_PREFIX:
                    return "The cloud storage path prefix";
                case CLOUD_STORAGE_REGION:
                    return "The cloud storage region";
                case CLOUD_STORAGE_ENDPOINT:
                    return "The cloud storage endpoint";
                case CLOUD_STORAGE_ANONYMOUS_AUTH:
                    return "Indicates whether or not anonymous auth should be used for the cloud storage";
                case CLOUD_STORAGE_CACHE_POLICY:
                    return "The caching policy (either eager, lazy or selective). 'eager' caching will download"
                            + "all partitions upon booting, whereas 'lazy' caching will download a file upon"
                            + " request to open it. 'selective' caching will act as the 'lazy' policy; however, "
                            + " it allows to use the local disk(s) as a cache, where pages and indexes can be "
                            + " cached or evicted according to the pressure imposed on the local disks."
                            + " (default: 'lazy')";
                case CLOUD_STORAGE_ALLOCATION_PERCENTAGE:
                    return "The percentage of the total disk space that should be allocated for data storage when the"
                            + " 'selective' caching policy is used. The remaining will act as a buffer for "
                            + " query workspace (i.e., for query operations that require spilling to disk)."
                            + " (default: 80% of the total disk space)";
                case CLOUD_STORAGE_SWEEP_THRESHOLD_PERCENTAGE:
                    return "The percentage of the used storage space at which the disk sweeper starts freeing space by"
                            + " punching holes in stored indexes or by evicting them entirely, "
                            + " when the 'selective' caching policy is used."
                            + " (default: 90% of the allocated space for storage)";
                case CLOUD_STORAGE_DISK_MONITOR_INTERVAL:
                    return "The disk monitoring interval time (in seconds): determines how often the system"
                            + " checks for pressure on disk space when using the 'selective' caching policy."
                            + " (default : 120 seconds)";
                case CLOUD_STORAGE_INDEX_INACTIVE_DURATION_THRESHOLD:
                    return "The duration in minutes to consider an index is inactive. (default: 360 or 6 hours)";
                case CLOUD_STORAGE_DEBUG_MODE_ENABLED:
                    return "Whether or not the debug mode is enabled when using the 'selective' caching policy."
                            + "(default: false)";
                case CLOUD_STORAGE_DEBUG_SWEEP_THRESHOLD_SIZE:
                    return "For debugging only. Pressure size will be the current used space + the additional bytes"
                            + " provided by this configuration option instead of using "
                            + " CLOUD_STORAGE_SWEEP_THRESHOLD_PERCENTAGE."
                            + " (default: 0. I.e., CLOUD_STORAGE_SWEEP_THRESHOLD_PERCENTAGE will be used by default)";
                case CLOUD_PROFILER_LOG_INTERVAL:
                    return "The waiting time (in minutes) to log cloud request statistics (default: 0, which means"
                            + " the profiler is disabled by default). The minimum is 1 minute."
                            + " NOTE: Enabling the profiler could perturb the performance of cloud requests";
                case CLOUD_WRITE_BUFFER_SIZE:
                    return "The write buffer size in bytes. (default: 8MB)";
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

    }

    public String getStorageScheme() {
        return accessor.getString(Option.CLOUD_STORAGE_SCHEME);
    }

    public String getStorageBucket() {
        return accessor.getString(Option.CLOUD_STORAGE_BUCKET);
    }

    public String getStoragePrefix() {
        return accessor.getString(Option.CLOUD_STORAGE_PREFIX);
    }

    public String getStorageEndpoint() {
        return accessor.getString(Option.CLOUD_STORAGE_ENDPOINT);
    }

    public String getStorageRegion() {
        return accessor.getString(Option.CLOUD_STORAGE_REGION);
    }

    public boolean isStorageAnonymousAuth() {
        return accessor.getBoolean(Option.CLOUD_STORAGE_ANONYMOUS_AUTH);
    }

    public CloudCachePolicy getCloudCachePolicy() {
        return CloudCachePolicy.fromName(accessor.getString(Option.CLOUD_STORAGE_CACHE_POLICY));
    }

    public double getStorageAllocationPercentage() {
        return accessor.getDouble(Option.CLOUD_STORAGE_ALLOCATION_PERCENTAGE);
    }

    public double getStorageSweepThresholdPercentage() {
        return accessor.getDouble(Option.CLOUD_STORAGE_SWEEP_THRESHOLD_PERCENTAGE);
    }

    public int getStorageDiskMonitorInterval() {
        return accessor.getInt(Option.CLOUD_STORAGE_DISK_MONITOR_INTERVAL);
    }

    public long getStorageIndexInactiveDurationThreshold() {
        int minutes = accessor.getInt(Option.CLOUD_STORAGE_INDEX_INACTIVE_DURATION_THRESHOLD);
        return TimeUnit.MINUTES.toNanos(minutes);
    }

    public boolean isStorageDebugModeEnabled() {
        return accessor.getBoolean(Option.CLOUD_STORAGE_DEBUG_MODE_ENABLED);
    }

    public long getStorageDebugSweepThresholdSize() {
        return isStorageDebugModeEnabled() ? accessor.getLong(Option.CLOUD_STORAGE_DEBUG_SWEEP_THRESHOLD_SIZE) : 0L;
    }

    public long getProfilerLogInterval() {
        long interval = TimeUnit.MINUTES.toNanos(accessor.getInt(Option.CLOUD_PROFILER_LOG_INTERVAL));
        return interval == 0 ? 0 : Math.max(interval, TimeUnit.MINUTES.toNanos(1));
    }

    public int getWriteBufferSize() {
        return accessor.getInt(Option.CLOUD_WRITE_BUFFER_SIZE);
    }
}
