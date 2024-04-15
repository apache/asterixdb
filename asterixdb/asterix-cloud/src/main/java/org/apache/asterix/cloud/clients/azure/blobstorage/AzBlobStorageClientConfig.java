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
package org.apache.asterix.cloud.clients.azure.blobstorage;

import java.util.Objects;

import org.apache.asterix.common.config.CloudProperties;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;

public class AzBlobStorageClientConfig {
    private final int writeBufferSize;
    // Ref: https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch?tabs=microsoft-entra-id
    static final int DELETE_BATCH_SIZE = 256;
    private final String region;
    private final String endpoint;
    private final String prefix;

    private final boolean anonymousAuth;
    private final long profilerLogInterval;
    private final String bucket;
    private final long tokenAcquireTimeout;
    private final int writeMaxRequestsPerSeconds;
    private final int readMaxRequestsPerSeconds;

    public AzBlobStorageClientConfig(String region, String endpoint, String prefix, boolean anonymousAuth,
            long profilerLogInterval, String bucket, long tokenAcquireTimeout, int writeMaxRequestsPerSeconds,
            int readMaxRequestsPerSeconds, int writeBufferSize) {
        this.region = Objects.requireNonNull(region, "region");
        this.endpoint = endpoint;
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.anonymousAuth = anonymousAuth;
        this.profilerLogInterval = profilerLogInterval;
        this.bucket = bucket;
        this.tokenAcquireTimeout = tokenAcquireTimeout;
        this.writeMaxRequestsPerSeconds = writeMaxRequestsPerSeconds;
        this.readMaxRequestsPerSeconds = readMaxRequestsPerSeconds;
        this.writeBufferSize = writeBufferSize;
    }

    public static AzBlobStorageClientConfig of(CloudProperties cloudProperties) {
        return new AzBlobStorageClientConfig(cloudProperties.getStorageRegion(), cloudProperties.getStorageEndpoint(),
                cloudProperties.getStoragePrefix(), cloudProperties.isStorageAnonymousAuth(),
                cloudProperties.getProfilerLogInterval(), cloudProperties.getStorageBucket(),
                cloudProperties.getTokenAcquireTimeout(), cloudProperties.getWriteMaxRequestsPerSecond(),
                cloudProperties.getReadMaxRequestsPerSecond(), cloudProperties.getWriteBufferSize());
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getBucket() {
        return bucket;
    }

    public long getProfilerLogInterval() {
        return profilerLogInterval;
    }

    public boolean isAnonymousAuth() {
        return anonymousAuth;
    }

    public DefaultAzureCredential createCredentialsProvider() {
        return new DefaultAzureCredentialBuilder().build();
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
}
