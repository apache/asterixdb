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
package org.apache.asterix.feeds;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class FeedCollectInfo extends FeedInfo {
    public FeedId sourceFeedId;
    public FeedConnectionId feedConnectionId;
    public List<String> collectLocations = new ArrayList<String>();
    public List<String> computeLocations = new ArrayList<String>();
    public List<String> storageLocations = new ArrayList<String>();
    public Map<String, String> feedPolicy;
    public String superFeedManagerHost;
    public int superFeedManagerPort;
    public boolean fullyConnected;

    public FeedCollectInfo(FeedId sourceFeedId, FeedConnectionId feedConnectionId, JobSpecification jobSpec,
            JobId jobId, Map<String, String> feedPolicy) {
        super(jobSpec, jobId, FeedInfoType.COLLECT);
        this.sourceFeedId = sourceFeedId;
        this.feedConnectionId = feedConnectionId;
        this.feedPolicy = feedPolicy;
        this.fullyConnected = true;
    }

    @Override
    public String toString() {
        return FeedInfoType.COLLECT + "[" + feedConnectionId + "]";
    }
}
