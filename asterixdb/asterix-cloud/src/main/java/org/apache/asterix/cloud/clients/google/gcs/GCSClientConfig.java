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
package org.apache.asterix.cloud.clients.google.gcs;

import static org.apache.asterix.external.util.google.GCSConstants.ENDPOINT_FIELD_NAME;
import static org.apache.asterix.external.util.google.GCSConstants.STORAGE_PREFIX;

import java.io.IOException;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.cloud.io.ICloudProperties;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.NoCredentials;

public class GCSClientConfig {

    // The maximum number of files that can be deleted (GCS restriction): https://cloud.google.com/storage/quotas#json-requests
    static final int DELETE_BATCH_SIZE = 100;
    private final String region;
    private final String endpoint;
    private final boolean anonymousAuth;
    private final long profilerLogInterval;
    private final long tokenAcquireTimeout;
    private final int readMaxRequestsPerSeconds;
    private final int writeMaxRequestsPerSeconds;
    private final int writeBufferSize;
    private final int maxIdleSeconds;
    private final int maxLifetimeSeconds;
    private final String prefix;

    private GCSClientConfig(String region, String endpoint, boolean anonymousAuth, long profilerLogInterval,
            long tokenAcquireTimeout, int writeMaxRequestsPerSeconds, int readMaxRequestsPerSeconds,
            int writeBufferSize, int maxIdleSeconds, int maxLifetimeSeconds, String prefix) {
        this.region = region;
        this.endpoint = endpoint;
        this.anonymousAuth = anonymousAuth;
        this.profilerLogInterval = profilerLogInterval;
        this.tokenAcquireTimeout = tokenAcquireTimeout;
        this.writeMaxRequestsPerSeconds = writeMaxRequestsPerSeconds;
        this.readMaxRequestsPerSeconds = readMaxRequestsPerSeconds;
        this.writeBufferSize = writeBufferSize;
        this.maxIdleSeconds = maxIdleSeconds;
        this.maxLifetimeSeconds = maxLifetimeSeconds;
        this.prefix = prefix;
    }

    public GCSClientConfig(String region, String endpoint, boolean anonymousAuth, long profilerLogInterval,
            int writeBufferSize, String prefix) {
        this(region, endpoint, anonymousAuth, profilerLogInterval, 1, 0, 0, writeBufferSize, 0, 0, prefix);
    }

    public static GCSClientConfig of(ICloudProperties cloudProperties) {
        return new GCSClientConfig(cloudProperties.getStorageRegion(), cloudProperties.getStorageEndpoint(),
                cloudProperties.isStorageAnonymousAuth(), cloudProperties.getProfilerLogInterval(),
                cloudProperties.getTokenAcquireTimeout(), cloudProperties.getWriteMaxRequestsPerSecond(),
                cloudProperties.getReadMaxRequestsPerSecond(), cloudProperties.getWriteBufferSize(),
                cloudProperties.getRequestsHttpConnectionMaxIdleSeconds(),
                cloudProperties.getRequestsHttpConnectionMaxLifetimeSeconds(), cloudProperties.getStoragePrefix());
    }

    public static GCSClientConfig of(Map<String, String> configuration, int writeBufferSize) {
        String endPoint = configuration.getOrDefault(ENDPOINT_FIELD_NAME, "");
        long profilerLogInterval = 0;

        String region = "";
        String prefix = configuration.getOrDefault(STORAGE_PREFIX, "");
        boolean anonymousAuth = false;

        return new GCSClientConfig(region, endPoint, anonymousAuth, profilerLogInterval, writeBufferSize, prefix);
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public long getProfilerLogInterval() {
        return profilerLogInterval;
    }

    public boolean isAnonymousAuth() {
        return anonymousAuth;
    }

    public OAuth2Credentials createCredentialsProvider() throws HyracksDataException {
        try {
            return anonymousAuth ? NoCredentials.getInstance() : GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public long getTokenAcquireTimeout() {
        return tokenAcquireTimeout;
    }

    public int getWriteMaxRequestsPerSeconds() {
        return writeMaxRequestsPerSeconds;
    }

    public int getReadMaxRequestsPerSeconds() {
        return readMaxRequestsPerSeconds;
    }

    public int getWriteBufferSize() {
        return writeBufferSize;
    }

    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    public int getMaxLifetimeSeconds() {
        return maxLifetimeSeconds;
    }

    public String getPrefix() {
        return prefix;
    }
}
