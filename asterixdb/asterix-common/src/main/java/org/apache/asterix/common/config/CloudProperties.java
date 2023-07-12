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
import static org.apache.hyracks.control.common.config.OptionTypes.NONNEGATIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING;

import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.cloud.CloudCachePolicy;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;

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
        CLOUD_STORAGE_CACHE_POLICY(STRING, "lazy"),
        CLOUD_PROFILER_LOG_INTERVAL(NONNEGATIVE_INTEGER, 0);

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
                case CLOUD_PROFILER_LOG_INTERVAL:
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
                    return "The caching policy (either eager or lazy). 'Eager' caching will download all partitions"
                            + " upon booting, whereas lazy caching will download a file upon request to open it."
                            + " (default: 'lazy')";
                case CLOUD_PROFILER_LOG_INTERVAL:
                    return "The waiting time (in minutes) to log cloud request statistics (default: 0, which means"
                            + " the profiler is disabled by default). The minimum is 1 minute."
                            + " NOTE: Enabling the profiler could perturb the performance of cloud requests";
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

    public long getProfilerLogInterval() {
        long interval = TimeUnit.MINUTES.toNanos(accessor.getInt(Option.CLOUD_PROFILER_LOG_INTERVAL));
        return interval == 0 ? 0 : Math.max(interval, TimeUnit.MINUTES.toNanos(1));
    }
}
