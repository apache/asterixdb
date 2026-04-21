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

import static org.apache.hyracks.util.StringUtil.quoteNullableString;

import java.util.Map;
import java.util.Objects;

import org.apache.asterix.common.config.CloudProperties;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.azure.AzureConstants;
import org.apache.hyracks.cloud.io.ICloudProperties;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.models.AccessTier;

public class AzBlobStorageClientConfig {
    private static final AccessTier INTERNAL_STORAGE_ACCESS_TIER = AccessTier.HOT;
    private final String endpoint;
    private final String container;
    private final String prefix;
    private final AccessTier accessTier;
    private final int writeBufferSize;
    private final long tokenAcquireTimeout;
    private final int writeMaxRequestsPerSeconds;
    private final int readMaxRequestsPerSeconds;
    private final long profilerLogInterval;
    private final boolean storageDisableSSLVerify;
    private final int requestsMaxHttpConnections;
    private final int requestsMaxPendingHttpConnections;
    private final int requestsHttpConnectionAcquireTimeout;
    private final int maxIdleSeconds;
    private final int maxLifetimeSeconds;
    private final String clientId;

    public AzBlobStorageClientConfig(String endpoint, String prefix, long profilerLogInterval, String container,
            int writeBufferSize) {
        // TODO(mblow): using the same values as our defaults for blob storage seems sus, refactor to configurable and
        //              and use reasonable defaults
        this(endpoint, container, prefix, profilerLogInterval, 1, 0, 0, writeBufferSize, false, null,
                CloudProperties.MAX_HTTP_CONNECTIONS_DEFAULT, CloudProperties.MAX_PENDING_HTTP_CONNECTIONS_DEFAULT,
                CloudProperties.HTTP_CONNECTION_ACQUIRE_TIMEOUT_DEFAULT,
                CloudProperties.HTTP_CONNECTION_MAX_IDLE_SECONDS_DEFAULT,
                CloudProperties.HTTP_CONNECTION_MAX_LIFETIME_SECONDS_DEFAULT, null);
    }

    public AzBlobStorageClientConfig(String endpoint, String container, String prefix, long profilerLogInterval,
            long tokenAcquireTimeout, int writeMaxRequestsPerSeconds, int readMaxRequestsPerSeconds,
            int writeBufferSize, boolean storageDisableSSLVerify, AccessTier accessTier, int requestsMaxHttpConnections,
            int requestsMaxPendingHttpConnections, int requestsHttpConnectionAcquireTimeout, int maxIdleSeconds,
            int maxLifetimeSeconds, String clientId) {
        this.endpoint = endpoint;
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.profilerLogInterval = profilerLogInterval;
        this.container = container;
        this.tokenAcquireTimeout = tokenAcquireTimeout;
        this.writeMaxRequestsPerSeconds = writeMaxRequestsPerSeconds;
        this.readMaxRequestsPerSeconds = readMaxRequestsPerSeconds;
        this.writeBufferSize = writeBufferSize;
        this.storageDisableSSLVerify = storageDisableSSLVerify;
        this.requestsMaxHttpConnections = requestsMaxHttpConnections;
        this.requestsMaxPendingHttpConnections =
                getRequestsMaxPendingHttpConnections(requestsMaxPendingHttpConnections);
        this.requestsHttpConnectionAcquireTimeout = requestsHttpConnectionAcquireTimeout;
        this.accessTier = accessTier;
        this.maxIdleSeconds = maxIdleSeconds;
        this.maxLifetimeSeconds = maxLifetimeSeconds;
        this.clientId = clientId;
    }

    public static AzBlobStorageClientConfig of(ICloudProperties cloudProperties) {
        return new AzBlobStorageClientConfig(cloudProperties.getStorageEndpoint(), cloudProperties.getStorageBucket(),
                cloudProperties.getStoragePrefix(), cloudProperties.getProfilerLogInterval(),
                cloudProperties.getTokenAcquireTimeout(), cloudProperties.getWriteMaxRequestsPerSecond(),
                cloudProperties.getReadMaxRequestsPerSecond(), cloudProperties.getWriteBufferSize(),
                cloudProperties.isStorageDisableSSLVerify(), INTERNAL_STORAGE_ACCESS_TIER,
                cloudProperties.getRequestsMaxHttpConnections(), cloudProperties.getRequestsMaxPendingHttpConnections(),
                cloudProperties.getRequestsHttpConnectionAcquireTimeout(),
                cloudProperties.getRequestsHttpConnectionMaxIdleSeconds(),
                cloudProperties.getRequestsHttpConnectionMaxLifetimeSeconds(), cloudProperties.getAzureClientId());
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

        return new AzBlobStorageClientConfig(endPoint, prefix, profilerLogInterval, bucket, writeBufferSize);
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getContainer() {
        return container;
    }

    public long getProfilerLogInterval() {
        return profilerLogInterval;
    }

    public boolean isStorageDisableSSLVerify() {
        return storageDisableSSLVerify;
    }

    public DefaultAzureCredential createCredentialsProvider() {
        return new DefaultAzureCredentialBuilder().managedIdentityClientId(clientId).build();
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

    public AccessTier getAccessTier() {
        return accessTier;
    }

    public int getRequestsMaxHttpConnections() {
        return requestsMaxHttpConnections;
    }

    public int getRequestsMaxPendingHttpConnections() {
        return requestsMaxPendingHttpConnections;
    }

    public int getRequestsHttpConnectionAcquireTimeout() {
        return requestsHttpConnectionAcquireTimeout;
    }

    private static int getRequestsMaxPendingHttpConnections(int requestsMaxPendingHttpConnections) {
        if (requestsMaxPendingHttpConnections <= 0) {
            throw new IllegalArgumentException("requestsMaxPendingHttpConnections must be greater than 0");
        }
        return requestsMaxPendingHttpConnections;
    }

    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    public int getMaxLifetimeSeconds() {
        return maxLifetimeSeconds;
    }

    public String getClientId() {
        return clientId;
    }

    @Override
    public String toString() {
        return "AzBlobStorageClientConfig{" + "endpoint=" + quoteNullableString(endpoint) + ", " + "container="
                + quoteNullableString(container) + ", " + "prefix=" + quoteNullableString(prefix) + ", " + "accessTier="
                + accessTier + ", " + "writeBufferSize=" + writeBufferSize + ", " + "tokenAcquireTimeout="
                + tokenAcquireTimeout + ", " + "writeMaxRequestsPerSeconds=" + writeMaxRequestsPerSeconds + ", "
                + "readMaxRequestsPerSeconds=" + readMaxRequestsPerSeconds + ", " + "profilerLogInterval="
                + profilerLogInterval + ", " + "storageDisableSSLVerify=" + storageDisableSSLVerify + ", "
                + "requestsMaxHttpConnections=" + requestsMaxHttpConnections + ", "
                + "requestsMaxPendingHttpConnections=" + requestsMaxPendingHttpConnections + ", "
                + "requestsHttpConnectionAcquireTimeout=" + requestsHttpConnectionAcquireTimeout + ", "
                + "maxIdleSeconds=" + maxIdleSeconds + ", " + "maxLifetimeSeconds=" + maxLifetimeSeconds + ", "
                + "clientId=" + quoteNullableString(clientId) + '}';
    }
}
