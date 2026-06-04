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
import static org.apache.hyracks.control.common.config.OptionTypes.LONG_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.NONNEGATIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING_ARRAY;
import static org.apache.hyracks.control.common.config.OptionTypes.getRangedIntegerType;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.GIGABYTE;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.asterix.common.cloud.CloudCachePolicy;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.cloud.io.ICloudProperties;
import org.apache.hyracks.cloud.io.S3ChecksumBehavior;
import org.apache.hyracks.util.StorageUtil;

public class CloudProperties extends AbstractProperties implements ICloudProperties {

    // TODO(mblow): these should not be being used for external datasets- extract separate properties
    //              for these and make these defaults private to this class
    public static final int MAX_HTTP_CONNECTIONS_DEFAULT = 1000;
    public static final int MAX_PENDING_HTTP_CONNECTIONS_DEFAULT = 10000;
    public static final int HTTP_CONNECTION_ACQUIRE_TIMEOUT_DEFAULT = 120;
    public static final int HTTP_CONNECTION_MAX_IDLE_SECONDS_DEFAULT = 120;
    // TODO(mblow): this seems way too conservative, we should consider reducing this in a future release
    public static final int HTTP_CONNECTION_MAX_LIFETIME_SECONDS_DEFAULT = (int) TimeUnit.HOURS.toSeconds(1);

    public CloudProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public enum Option implements IOption {
        CLOUD_STORAGE_SCHEME(STRING, ""),
        CLOUD_STORAGE_BUCKET(STRING, ""),
        CLOUD_STORAGE_PREFIX(STRING, ""),
        CLOUD_STORAGE_REGION(STRING, ""),
        CLOUD_STORAGE_ENDPOINT(STRING, ""),
        CLOUD_STORAGE_CERTIFICATES(STRING_ARRAY, new String[0]),
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
        CLOUD_REQUESTS_MAX_HTTP_CONNECTIONS(POSITIVE_INTEGER, MAX_HTTP_CONNECTIONS_DEFAULT),
        CLOUD_REQUESTS_MAX_PENDING_HTTP_CONNECTIONS(POSITIVE_INTEGER, MAX_PENDING_HTTP_CONNECTIONS_DEFAULT),
        CLOUD_REQUESTS_HTTP_CONNECTION_ACQUIRE_TIMEOUT(POSITIVE_INTEGER, HTTP_CONNECTION_ACQUIRE_TIMEOUT_DEFAULT),
        CLOUD_REQUESTS_HTTP_CONNECTION_MAX_IDLE_SECONDS(NONNEGATIVE_INTEGER, HTTP_CONNECTION_MAX_IDLE_SECONDS_DEFAULT),
        CLOUD_REQUESTS_HTTP_CONNECTION_MAX_LIFETIME_SECONDS(
                NONNEGATIVE_INTEGER,
                HTTP_CONNECTION_MAX_LIFETIME_SECONDS_DEFAULT),
        CLOUD_STORAGE_FORCE_PATH_STYLE(BOOLEAN, false),
        CLOUD_STORAGE_DISABLE_SSL_VERIFY(BOOLEAN, false),
        CLOUD_STORAGE_S3_CLIENT_READ_TIMEOUT(INTEGER, -1),
        CLOUD_STORAGE_S3_PARALLEL_DOWNLOADER_CLIENT_TYPE(STRING, (Function<IApplicationConfig, String>) app -> {
            String endpoint = app.getString(CLOUD_STORAGE_ENDPOINT);
            return (endpoint == null || endpoint.isEmpty()) ? "crt" : "async";
        }),
        CLOUD_STORAGE_S3_USE_ROUND_ROBIN_DNS_RESOLVER(BOOLEAN, false),
        CLOUD_STORAGE_S3_ACCESS_KEY_ID(STRING, (String) null),
        CLOUD_STORAGE_S3_SECRET_ACCESS_KEY(STRING, (String) null),
        CLOUD_STORAGE_AZURE_CLIENT_ID(STRING, (String) null),
        CLOUD_STORAGE_S3_CHECKSUM_BEHAVIOR(STRING, S3ChecksumBehavior.SDK_DEFAULT.stringValue());

        private final IOptionType interpreter;
        private final Object defaultValue;

        <T> Option(IOptionType<T> interpreter, T defaultValue) {
            this.interpreter = interpreter;
            this.defaultValue = defaultValue;
        }

        <T> Option(IOptionType<T> interpreter, Function<IApplicationConfig, T> defaultOption) {
            this.interpreter = interpreter;
            this.defaultValue = defaultOption;
        }

        @Override
        public Section section() {
            switch (this) {
                case CLOUD_STORAGE_SCHEME:
                case CLOUD_STORAGE_BUCKET:
                case CLOUD_STORAGE_PREFIX:
                case CLOUD_STORAGE_REGION:
                case CLOUD_STORAGE_ENDPOINT:
                case CLOUD_STORAGE_CERTIFICATES:
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
                case CLOUD_REQUESTS_HTTP_CONNECTION_MAX_IDLE_SECONDS:
                case CLOUD_REQUESTS_HTTP_CONNECTION_MAX_LIFETIME_SECONDS:
                case CLOUD_STORAGE_FORCE_PATH_STYLE:
                case CLOUD_STORAGE_DISABLE_SSL_VERIFY:
                case CLOUD_STORAGE_S3_CLIENT_READ_TIMEOUT:
                case CLOUD_STORAGE_S3_PARALLEL_DOWNLOADER_CLIENT_TYPE:
                case CLOUD_STORAGE_S3_USE_ROUND_ROBIN_DNS_RESOLVER:
                case CLOUD_STORAGE_S3_ACCESS_KEY_ID:
                case CLOUD_STORAGE_S3_SECRET_ACCESS_KEY:
                case CLOUD_STORAGE_AZURE_CLIENT_ID:
                case CLOUD_STORAGE_S3_CHECKSUM_BEHAVIOR:
                    return Section.COMMON;
                default:
                    throw new IllegalStateException("NYI: " + this);
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
                case CLOUD_STORAGE_CERTIFICATES:
                    return "The certificates to use to validate the cloud storage server";
                case CLOUD_STORAGE_CACHE_POLICY:
                    return "The caching policy (either eager, lazy or selective). 'eager' caching will download"
                            + "all partitions upon booting, whereas 'lazy' caching will download a file upon"
                            + " request to open it. 'selective' caching will act as the 'lazy' policy; however, "
                            + " it allows to use the local disk(s) as a cache, where pages and indexes can be "
                            + " cached or evicted according to the pressure imposed on the local disks";
                case CLOUD_STORAGE_ALLOCATION_PERCENTAGE:
                    return "The percentage of the total disk space that should be allocated for data storage when the"
                            + " 'selective' caching policy is used. The remaining will act as a buffer for "
                            + " query workspace (i.e., for query operations that require spilling to disk)";
                case CLOUD_STORAGE_SWEEP_THRESHOLD_PERCENTAGE:
                    return "The percentage of the used storage space at which the disk sweeper starts freeing space by"
                            + " punching holes in stored indexes or by evicting them entirely, "
                            + " when the 'selective' caching policy is used";
                case CLOUD_STORAGE_DISK_MONITOR_INTERVAL:
                    return "The disk monitoring interval time (in seconds): determines how often the system"
                            + " checks for pressure on disk space when using the 'selective' caching policy";
                case CLOUD_STORAGE_INDEX_INACTIVE_DURATION_THRESHOLD:
                    return "The duration in minutes to consider an index is inactive";
                case CLOUD_STORAGE_DEBUG_MODE_ENABLED:
                    return "Whether or not the debug mode is enabled when using the 'selective' caching policy";
                case CLOUD_STORAGE_DEBUG_SWEEP_THRESHOLD_SIZE:
                    return "For debugging only. Pressure size will be the current used space + the additional bytes"
                            + " provided by this configuration option instead of using "
                            + " CLOUD_STORAGE_SWEEP_THRESHOLD_PERCENTAGE";
                case CLOUD_PROFILER_LOG_INTERVAL:
                    return "The waiting time (in minutes) to log cloud request statistics. The minimum is 1 minute"
                            + " Note: enabling this logging may perturb the performance of cloud requests";
                case CLOUD_ACQUIRE_TOKEN_TIMEOUT:
                    return "The waiting time (in milliseconds) if a requesting thread failed to acquire a token if the"
                            + " rate limit of cloud requests exceeded (min: 1, max: 5000)";
                case CLOUD_MAX_WRITE_REQUESTS_PER_SECOND:
                    return "The maximum number of write requests per second (0 means unlimited)";
                case CLOUD_MAX_READ_REQUESTS_PER_SECOND:
                    return "The maximum number of read requests per second (0 means unlimited)";
                case CLOUD_WRITE_BUFFER_SIZE:
                    return "The write buffer size in bytes. (min: 5MiB)";
                case CLOUD_EVICTION_PLAN_REEVALUATE_THRESHOLD:
                    return "The number of cloud reads for re-evaluating an eviction plan";
                case CLOUD_REQUESTS_MAX_HTTP_CONNECTIONS:
                    return "The maximum number of HTTP connections to use concurrently for cloud requests per node";
                case CLOUD_REQUESTS_MAX_PENDING_HTTP_CONNECTIONS:
                    return "The maximum number of HTTP connections allowed to wait for a connection per node";
                case CLOUD_REQUESTS_HTTP_CONNECTION_ACQUIRE_TIMEOUT:
                    return "The waiting time (in seconds) to acquire an HTTP connection before failing the request";
                case CLOUD_REQUESTS_HTTP_CONNECTION_MAX_IDLE_SECONDS:
                    return "The time (in seconds) after which an idle cloud connection will be closed. (0 == unlimited idle)";
                case CLOUD_REQUESTS_HTTP_CONNECTION_MAX_LIFETIME_SECONDS:
                    return "The time (in seconds) after which an cloud connection will no longer be reused. (0 == unlimited lifetime)";
                case CLOUD_STORAGE_FORCE_PATH_STYLE:
                    return "Indicates whether or not to force path style when accessing the cloud storage";
                case CLOUD_STORAGE_DISABLE_SSL_VERIFY:
                    return "Indicates whether or not to disable SSL certificate verification on the cloud storage";
                case CLOUD_STORAGE_S3_CLIENT_READ_TIMEOUT:
                    return "The read timeout (in seconds) for S3 sync client (-1 means SDK default)";
                case CLOUD_STORAGE_S3_PARALLEL_DOWNLOADER_CLIENT_TYPE:
                    return "The S3 client to use for parallel downloads (crt, async or sync)";
                case CLOUD_STORAGE_S3_USE_ROUND_ROBIN_DNS_RESOLVER:
                    return "Whether or not to use a round-robin DNS resolver for S3 client connections. Currently"
                            + " only applicable when using the async S3 client for parallel downloads.";
                case CLOUD_STORAGE_S3_ACCESS_KEY_ID:
                    return "The S3 access key ID for static credential authentication (defaults to null, which indicates to use default credential chain)";
                case CLOUD_STORAGE_S3_SECRET_ACCESS_KEY:
                    return "The S3 secret access key for static credential authentication (defaults to null, which indicates to use default credential chain)";
                case CLOUD_STORAGE_AZURE_CLIENT_ID:
                    return "The Azure user managed identity client ID (defaults to null, which takes the system managed identity client ID)";
                case CLOUD_STORAGE_S3_CHECKSUM_BEHAVIOR:
                    return "The checksum behavior for S3 requests and responses. Accepted values: "
                            + "'when_required' (only checksums mandated by the operation), "
                            + "'when_supported' (checksums on all eligible operations, SDK >= 2.30 default), "
                            + "'sdk_default' (defer to SDK default. " + "Defaults to 'sdk_default'";
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
            if (this == CLOUD_STORAGE_S3_PARALLEL_DOWNLOADER_CLIENT_TYPE) {
                return "crt if no custom endpoint is set; async otherwise";
            }
            return IOption.super.usageDefaultOverride(accessor, optionPrinter);
        }

        @Override
        public boolean sensitive() {
            return this == CLOUD_STORAGE_S3_SECRET_ACCESS_KEY;
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

    public Collection<String> getStorageCertificates() {
        String[] certificates = accessor.getStringArray(Option.CLOUD_STORAGE_CERTIFICATES);
        return certificates == null || certificates.length == 0 ? Collections.emptyList() : List.of(certificates);
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

    @Override
    public int getRequestsHttpConnectionMaxIdleSeconds() {
        return accessor.getInt(Option.CLOUD_REQUESTS_HTTP_CONNECTION_MAX_IDLE_SECONDS);
    }

    @Override
    public int getRequestsHttpConnectionMaxLifetimeSeconds() {
        return accessor.getInt(Option.CLOUD_REQUESTS_HTTP_CONNECTION_MAX_LIFETIME_SECONDS);
    }

    public boolean isStorageForcePathStyle() {
        return accessor.getBoolean(Option.CLOUD_STORAGE_FORCE_PATH_STYLE);
    }

    public boolean isStorageDisableSSLVerify() {
        return accessor.getBoolean(Option.CLOUD_STORAGE_DISABLE_SSL_VERIFY);
    }

    public String getS3ParallelDownloaderClientType() {
        return accessor.getString(Option.CLOUD_STORAGE_S3_PARALLEL_DOWNLOADER_CLIENT_TYPE).toUpperCase();
    }

    public int getS3ReadTimeoutInSeconds() {
        return accessor.getInt(Option.CLOUD_STORAGE_S3_CLIENT_READ_TIMEOUT);
    }

    public boolean useRoundRobinDnsResolver() {
        return accessor.getBoolean(Option.CLOUD_STORAGE_S3_USE_ROUND_ROBIN_DNS_RESOLVER);
    }

    public String getS3AccessKeyId() {
        return accessor.getString(Option.CLOUD_STORAGE_S3_ACCESS_KEY_ID);
    }

    public String getS3SecretAccessKey() {
        return accessor.getString(Option.CLOUD_STORAGE_S3_SECRET_ACCESS_KEY);
    }

    public String getAzureClientId() {
        return accessor.getString(Option.CLOUD_STORAGE_AZURE_CLIENT_ID);
    }

    // Parses the stored string value to the S3ChecksumBehavior enum
    public S3ChecksumBehavior getS3ChecksumBehavior() {
        return S3ChecksumBehavior.fromString(accessor.getString(Option.CLOUD_STORAGE_S3_CHECKSUM_BEHAVIOR));
    }
}
