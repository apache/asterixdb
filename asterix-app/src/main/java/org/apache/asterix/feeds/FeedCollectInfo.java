/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.feeds;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

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
