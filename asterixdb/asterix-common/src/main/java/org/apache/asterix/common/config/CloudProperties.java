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
import static org.apache.hyracks.control.common.config.OptionTypes.getRangedIntegerType;
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
        CLOUD_ACQUIRE_TOKEN_TIMEOUT(POSITIVE_INTEGER, 100),
        CLOUD_MAX_WRITE_REQUESTS_PER_SECOND(NONNEGATIVE_INTEGER, 250),
        CLOUD_MAX_READ_REQUESTS_PER_SECOND(NONNEGATIVE_INTEGER, 1500),
        CLOUD_WRITE_BUFFER_SIZE(
                getRangedIntegerType(5, Integer.MAX_VALUE),
                StorageUtil.getIntSizeInBytes(8, StorageUtil.StorageUnit.MEGABYTE)),
        CLOUD_EVICTION_PLAN_REEVALUATE_THRESHOLD(POSITIVE_INTEGER, 50),
        CLOUD_REQUESTS_MAX_HTTP_CONNECTIONS(POSITIVE_INTEGER, 1000),
        CLOUD_REQUESTS_MAX_PENDING_HTTP_CONNECTIONS(POSITIVE_INTEGER, 10000),
        CLOUD_REQUESTS_HTTP_CONNECTION_ACQUIRE_TIMEOUT(POSITIVE_INTEGER, 120),
        CLOUD_STORAGE_FORCE_PATH_STYLE(BOOLEAN, false),
        CLOUD_STORAGE_DISABLE_SSL_VERIFY(BOOLEAN, false),
        CLOUD_STORAGE_LIST_EVENTUALLY_CONSISTENT(BOOLEAN, false);

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
                case CLOUD_ACQUIRE_TOKEN_TIMEOUT:
                case CLOUD_MAX_WRITE_REQUESTS_PER_SECOND:
                case CLOUD_MAX_READ_REQUESTS_PER_SECOND:
                case CLOUD_WRITE_BUFFER_SIZE:
                case CLOUD_EVICTION_PLAN_REEVALUATE_THRESHOLD:
                case CLOUD_REQUESTS_MAX_HTTP_CONNECTIONS:
                case CLOUD_REQUESTS_MAX_PENDING_HTTP_CONNECTIONS:
                case CLOUD_REQUESTS_HTTP_CONNECTION_ACQUIRE_TIMEOUT:
                case CLOUD_STORAGE_FORCE_PATH_STYLE:
                case CLOUD_STORAGE_DISABLE_SSL_VERIFY:
                case CLOUD_STORAGE_LIST_EVENTUALLY_CONSISTENT:
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
                            + " (default: 'selective')";
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
                    return "The waiting time (in minutes) to log cloud request statistics. The minimum is 1 minute."
                            + " Note: by default, the logging is disabled. Enabling it could perturb the performance of cloud requests";
                case CLOUD_ACQUIRE_TOKEN_TIMEOUT:
                    return "The waiting time (in milliseconds) if a requesting thread failed to acquire a token if the"
                            + " rate limit of cloud requests exceeded (default: 100, min: 1, and max: 5000)";
                case CLOUD_MAX_WRITE_REQUESTS_PER_SECOND:
                    return "The maximum number of write requests per second (default: 2500, 0 means unlimited)";
                case CLOUD_MAX_READ_REQUESTS_PER_SECOND:
                    return "The maximum number of read requests per second (default: 4000, 0 means unlimited)";
                case CLOUD_WRITE_BUFFER_SIZE:
                    return "The write buffer size in bytes. (default: 8MB, min: 5MB)";
                case CLOUD_EVICTION_PLAN_REEVALUATE_THRESHOLD:
                    return "The number of cloud reads for re-evaluating an eviction plan. (default: 50)";
                case CLOUD_REQUESTS_MAX_HTTP_CONNECTIONS:
                    return "The maximum number of HTTP connections to use concurrently for cloud requests per node. (default: 1000)";
                case CLOUD_REQUESTS_MAX_PENDING_HTTP_CONNECTIONS:
                    return "The maximum number of HTTP connections allowed to wait for a connection per node. (default: 10000)";
                case CLOUD_REQUESTS_HTTP_CONNECTION_ACQUIRE_TIMEOUT:
                    return "The waiting time (in seconds) to acquire an HTTP connection before failing the request."
                            + " (default: 120 seconds)";
                case CLOUD_STORAGE_FORCE_PATH_STYLE:
                    return "Indicates whether or not to force path style when accessing the cloud storage. (default:"
                            + " false)";
                case CLOUD_STORAGE_DISABLE_SSL_VERIFY:
                    return "Indicates whether or not to disable SSL certificate verification on the cloud storage. "
                            + "(default: false)";
                case CLOUD_STORAGE_LIST_EVENTUALLY_CONSISTENT:
                    return "Indicates whether or not deleted objects may be contained in list operations for some time"
                            + "after they are deleted. (default: false)";
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

    public long getTokenAcquireTimeout() {
        int time = accessor.getInt(Option.CLOUD_ACQUIRE_TOKEN_TIMEOUT);
        return Math.min(time, 1000);
    }

    public int getWriteMaxRequestsPerSecond() {
        return accessor.getInt(Option.CLOUD_MAX_WRITE_REQUESTS_PER_SECOND);
    }

    public int getReadMaxRequestsPerSecond() {
        return accessor.getInt(Option.CLOUD_MAX_READ_REQUESTS_PER_SECOND);
    }

    public int getWriteBufferSize() {
        return accessor.getInt(Option.CLOUD_WRITE_BUFFER_SIZE);
    }

    public int getEvictionPlanReevaluationThreshold() {
        return accessor.getInt(Option.CLOUD_EVICTION_PLAN_REEVALUATE_THRESHOLD);
    }

    public int getRequestsMaxHttpConnections() {
        return accessor.getInt(Option.CLOUD_REQUESTS_MAX_HTTP_CONNECTIONS);
    }

    public int getRequestsMaxPendingHttpConnections() {
        return accessor.getInt(Option.CLOUD_REQUESTS_MAX_PENDING_HTTP_CONNECTIONS);
    }

    public int getRequestsHttpConnectionAcquireTimeout() {
        return accessor.getInt(Option.CLOUD_REQUESTS_HTTP_CONNECTION_ACQUIRE_TIMEOUT);
    }

    public boolean isStorageForcePathStyle() {
        return accessor.getBoolean(Option.CLOUD_STORAGE_FORCE_PATH_STYLE);
    }

    public boolean isStorageDisableSSLVerify() {
        return accessor.getBoolean(Option.CLOUD_STORAGE_DISABLE_SSL_VERIFY);
    }

    public boolean isStorageListEventuallyConsistent() {
        return accessor.getBoolean(Option.CLOUD_STORAGE_LIST_EVENTUALLY_CONSISTENT);
    }
}
