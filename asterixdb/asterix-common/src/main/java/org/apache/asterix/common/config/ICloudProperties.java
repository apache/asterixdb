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

import static org.apache.hyracks.util.annotations.AiProvenance.Agent.GPT_5_MINI;
import static org.apache.hyracks.util.annotations.AiProvenance.ContributionKind.GENERATED;
import static org.apache.hyracks.util.annotations.AiProvenance.Tool.GITHUB_COPILOT;

import java.util.Collection;

import org.apache.asterix.common.cloud.CloudCachePolicy;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Interface extracted from CloudProperties to allow consumers to depend on an abstraction.
 */
@AiProvenance(agent = GPT_5_MINI, tool = GITHUB_COPILOT, contributionKind = GENERATED)
public interface ICloudProperties {

    String getStorageScheme();

    String getStorageBucket();

    String getStoragePrefix();

    String getStorageEndpoint();

    String getStorageRegion();

    boolean isStorageAnonymousAuth();

    Collection<String> getStorageCertificates();

    CloudCachePolicy getCloudCachePolicy();

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

    boolean isStorageForcePathStyle();

    boolean isStorageDisableSSLVerify();

    boolean isStorageListEventuallyConsistent();

    String getS3ParallelDownloaderClientType();

    int getS3ReadTimeoutInSeconds();

    boolean useRoundRobinDnsResolver();
}
