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

import java.util.Map;
import java.util.Objects;

import org.apache.asterix.common.config.CloudProperties;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.azure.blob_storage.AzureConstants;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;

public class AzBlobStorageClientConfig {
    // Ref: https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch?tabs=microsoft-entra-id
    static final int DELETE_BATCH_SIZE = 256;

    private final int writeBufferSize;
    private final String region;
    private final String endpoint;
    private final String prefix;

    private final boolean anonymousAuth;
    private final long profilerLogInterval;
    private final String bucket;
    private final long tokenAcquireTimeout;
    private final int writeMaxRequestsPerSeconds;
    private final int readMaxRequestsPerSeconds;
    private final boolean storageDisableSSLVerify;

    public AzBlobStorageClientConfig(String region, String endpoint, String prefix, boolean anonymousAuth,
            long profilerLogInterval, String bucket, int writeBufferSize) {
        this(region, endpoint, prefix, anonymousAuth, profilerLogInterval, bucket, 1, 0, 0, writeBufferSize, false);
    }

    public AzBlobStorageClientConfig(String region, String endpoint, String prefix, boolean anonymousAuth,
            long profilerLogInterval, String bucket, long tokenAcquireTimeout, int writeMaxRequestsPerSeconds,
            int readMaxRequestsPerSeconds, int writeBufferSize, boolean storageDisableSSLVerify) {
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
        this.storageDisableSSLVerify = storageDisableSSLVerify;
    }

    public static AzBlobStorageClientConfig of(CloudProperties cloudProperties) {
        return new AzBlobStorageClientConfig(cloudProperties.getStorageRegion(), cloudProperties.getStorageEndpoint(),
                cloudProperties.getStoragePrefix(), cloudProperties.isStorageAnonymousAuth(),
                cloudProperties.getProfilerLogInterval(), cloudProperties.getStorageBucket(),
                cloudProperties.getTokenAcquireTimeout(), cloudProperties.getWriteMaxRequestsPerSecond(),
                cloudProperties.getReadMaxRequestsPerSecond(), cloudProperties.getWriteBufferSize(),
                cloudProperties.isStorageDisableSSLVerify());
    }

    public static AzBlobStorageClientConfig of(Map<String, String> configuration, int writeBufferSize) {
        // Used to determine local vs. actual azure
        String endPoint = configuration.getOrDefault(AzureConstants.ENDPOINT_FIELD_NAME, "");
        String bucket = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        // Disabled
        long profilerLogInterval = 0;

        // Dummy values;
        String region = "";
        String prefix = "";
        boolean anonymousAuth = false;

        return new AzBlobStorageClientConfig(region, endPoint, prefix, anonymousAuth, profilerLogInterval, bucket,
                writeBufferSize);
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

    public boolean isStorageDisableSSLVerify() {
        return storageDisableSSLVerify;
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
