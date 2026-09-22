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
package org.apache.hyracks.cloud.io;

import java.util.Collection;

/**
 * Interface extracted from CloudProperties to allow consumers to depend on an abstraction.
 */
public interface ICloudProperties {

    String getStorageScheme();

    String getStorageBucket();

    String getStoragePrefix();

    String getStorageEndpoint();

    String getStorageRegion();

    boolean isStorageAnonymousAuth();

    Collection<String> getStorageCertificates();

    double getStorageAllocationPercentage();

    double getStorageSweepThresholdPercentage();

    int getStorageDiskMonitorInterval();

    long getStorageIndexInactiveDurationThreshold();

    boolean isStorageDebugModeEnabled();

    long getStorageDebugSweepThresholdSize();

    long getProfilerLogInterval();

    long getTokenAcquireTimeout();

    int getWriteMaxRequestsPerSecond();

    int getReadMaxRequestsPerSecond();

    int getWriteBufferSize();

    int getEvictionPlanReevaluationThreshold();

    int getRequestsMaxHttpConnections();

    int getRequestsMaxPendingHttpConnections();

    int getRequestsHttpConnectionAcquireTimeout();

    int getRequestsHttpConnectionMaxIdleSeconds();

    int getRequestsHttpConnectionMaxLifetimeSeconds();

    boolean isStorageForcePathStyle();

    boolean isStorageDisableSSLVerify();

    String getS3ParallelDownloaderClientType();

    int getS3ReadTimeoutInSeconds();

    boolean useRoundRobinDnsResolver();

    String getS3AccessKeyId();

    String getS3SecretAccessKey();

    /**
     * Valid values for {@link #getS3ChecksumBehavior()}.
     */
    S3ChecksumBehavior getS3ChecksumBehavior();

    String getAzureClientId();
}
