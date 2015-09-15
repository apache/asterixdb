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
package org.apache.asterix.common.feeds;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.feeds.api.IFeedJoint;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class FeedConnectJobInfo extends FeedJobInfo {

    private final FeedConnectionId connectionId;
    private final Map<String, String> feedPolicy;
    private final IFeedJoint sourceFeedJoint;
    private IFeedJoint computeFeedJoint;

    private List<String> collectLocations;
    private List<String> computeLocations;
    private List<String> storageLocations;

    public FeedConnectJobInfo(JobId jobId, FeedJobState state, FeedConnectionId connectionId,
            IFeedJoint sourceFeedJoint, IFeedJoint computeFeedJoint, JobSpecification spec,
            Map<String, String> feedPolicy) {
        super(jobId, state, FeedJobInfo.JobType.FEED_CONNECT, spec);
        this.connectionId = connectionId;
        this.sourceFeedJoint = sourceFeedJoint;
        this.computeFeedJoint = computeFeedJoint;
        this.feedPolicy = feedPolicy;
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

    public List<String> getCollectLocations() {
        return collectLocations;
    }

    public List<String> getComputeLocations() {
        return computeLocations;
    }

    public List<String> getStorageLocations() {
        return storageLocations;
    }

    public void setCollectLocations(List<String> collectLocations) {
        this.collectLocations = collectLocations;
    }

    public void setComputeLocations(List<String> computeLocations) {
        this.computeLocations = computeLocations;
    }

    public void setStorageLocations(List<String> storageLocations) {
        this.storageLocations = storageLocations;
    }

    public IFeedJoint getSourceFeedJoint() {
        return sourceFeedJoint;
    }

    public IFeedJoint getComputeFeedJoint() {
        return computeFeedJoint;
    }

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }

    public void setComputeFeedJoint(IFeedJoint computeFeedJoint) {
        this.computeFeedJoint = computeFeedJoint;
    }

}
