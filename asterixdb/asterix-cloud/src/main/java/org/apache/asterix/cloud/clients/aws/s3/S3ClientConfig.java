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
package org.apache.asterix.cloud.clients.aws.s3;

import java.util.Map;
import java.util.Objects;

import org.apache.asterix.common.config.CloudProperties;
import org.apache.asterix.external.util.aws.s3.S3Constants;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

public final class S3ClientConfig {

    // The maximum number of file that can be deleted (AWS restriction)
    static final int DELETE_BATCH_SIZE = 1000;
    private final String region;
    private final String endpoint;
    private final String prefix;
    private final boolean anonymousAuth;
    private final long profilerLogInterval;
    private final int writeBufferSize;
    private final long tokenAcquireTimeout;
    private final int readMaxRequestsPerSeconds;
    private final int writeMaxRequestsPerSeconds;
    private final int requestsMaxHttpConnections;
    private final int requestsMaxPendingHttpConnections;
    private final int requestsHttpConnectionAcquireTimeout;
    private final boolean forcePathStyle;
    private final boolean disableSslVerify;
    private final boolean storageListEventuallyConsistent;

    public S3ClientConfig(String region, String endpoint, String prefix, boolean anonymousAuth,
            long profilerLogInterval, int writeBufferSize) {
        this(region, endpoint, prefix, anonymousAuth, profilerLogInterval, writeBufferSize, 1, 0, 0, 0, false, false,
                false, 0, 0);
    }

    private S3ClientConfig(String region, String endpoint, String prefix, boolean anonymousAuth,
            long profilerLogInterval, int writeBufferSize, long tokenAcquireTimeout, int writeMaxRequestsPerSeconds,
            int readMaxRequestsPerSeconds, int requestsMaxHttpConnections, boolean forcePathStyle,
            boolean disableSslVerify, boolean storageListEventuallyConsistent, int requestsMaxPendingHttpConnections,
            int requestsHttpConnectionAcquireTimeout) {
        this.region = Objects.requireNonNull(region, "region");
        this.endpoint = endpoint;
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.anonymousAuth = anonymousAuth;
        this.profilerLogInterval = profilerLogInterval;
        this.writeBufferSize = writeBufferSize;
        this.tokenAcquireTimeout = tokenAcquireTimeout;
        this.writeMaxRequestsPerSeconds = writeMaxRequestsPerSeconds;
        this.readMaxRequestsPerSeconds = readMaxRequestsPerSeconds;
        this.requestsMaxHttpConnections = requestsMaxHttpConnections;
        this.requestsMaxPendingHttpConnections = requestsMaxPendingHttpConnections;
        this.requestsHttpConnectionAcquireTimeout = requestsHttpConnectionAcquireTimeout;
        this.forcePathStyle = forcePathStyle;
        this.disableSslVerify = disableSslVerify;
        this.storageListEventuallyConsistent = storageListEventuallyConsistent;
    }

    public static S3ClientConfig of(CloudProperties cloudProperties) {
        return new S3ClientConfig(cloudProperties.getStorageRegion(), cloudProperties.getStorageEndpoint(),
                cloudProperties.getStoragePrefix(), cloudProperties.isStorageAnonymousAuth(),
                cloudProperties.getProfilerLogInterval(), cloudProperties.getWriteBufferSize(),
                cloudProperties.getTokenAcquireTimeout(), cloudProperties.getWriteMaxRequestsPerSecond(),
                cloudProperties.getReadMaxRequestsPerSecond(), cloudProperties.getRequestsMaxHttpConnections(),
                cloudProperties.isStorageForcePathStyle(), cloudProperties.isStorageDisableSSLVerify(),
                cloudProperties.isStorageListEventuallyConsistent(),
                cloudProperties.getRequestsMaxPendingHttpConnections(),
                cloudProperties.getRequestsHttpConnectionAcquireTimeout());
    }

    public static S3ClientConfig of(Map<String, String> configuration, int writeBufferSize) {
        // Used to determine local vs. actual S3
        String endPoint = configuration.getOrDefault(S3Constants.SERVICE_END_POINT_FIELD_NAME, "");
        // Disabled
        long profilerLogInterval = 0;

        // Dummy values;
        String region = "";
        String prefix = "";
        boolean anonymousAuth = false;

        return new S3ClientConfig(region, endPoint, prefix, anonymousAuth, profilerLogInterval, writeBufferSize);
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

    public boolean isLocalS3Provider() {
        // to workaround https://github.com/findify/s3mock/issues/187 in our S3Mock, we encode/decode keys
        return isS3Mock();
    }

    public AwsCredentialsProvider createCredentialsProvider() {
        return anonymousAuth ? AnonymousCredentialsProvider.create() : DefaultCredentialsProvider.builder().build();
    }

    public long getProfilerLogInterval() {
        return profilerLogInterval;
    }

    public int getWriteBufferSize() {
        return writeBufferSize;
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

    public int getRequestsMaxHttpConnections() {
        return requestsMaxHttpConnections;
    }

    public int getRequestsMaxPendingHttpConnections() {
        return requestsMaxPendingHttpConnections;
    }

    public int getRequestsHttpConnectionAcquireTimeout() {
        return requestsHttpConnectionAcquireTimeout;
    }

    public boolean isDisableSslVerify() {
        return disableSslVerify;
    }

    public boolean isForcePathStyle() {
        return forcePathStyle;
    }

    public boolean isStorageListEventuallyConsistent() {
        return storageListEventuallyConsistent;
    }

    private boolean isS3Mock() {
        return endpoint != null && !endpoint.isEmpty();
    }

}
