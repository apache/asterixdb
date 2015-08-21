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
package edu.uci.ics.asterix.common.feeds.api;

import java.util.List;

import edu.uci.ics.asterix.common.api.IClusterEventsSubscriber;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedJointKey;
import edu.uci.ics.hyracks.api.job.IJobLifecycleListener;

public interface IFeedLifecycleListener extends IJobLifecycleListener, IClusterEventsSubscriber {

    public enum ConnectionLocation {
        SOURCE_FEED_INTAKE_STAGE,
        SOURCE_FEED_COMPUTE_STAGE
    }

    public IFeedJoint getAvailableFeedJoint(FeedJointKey feedJoinKey);

    public boolean isFeedJointAvailable(FeedJointKey feedJoinKey);

    public List<FeedConnectionId> getActiveFeedConnections(FeedId feedId);

    public List<String> getComputeLocations(FeedId feedId);

    public List<String> getIntakeLocations(FeedId feedId);

    public List<String> getStoreLocations(FeedConnectionId feedId);

    public void registerFeedEventSubscriber(FeedConnectionId connectionId, IFeedLifecycleEventSubscriber subscriber);

    public void deregisterFeedEventSubscriber(FeedConnectionId connectionId, IFeedLifecycleEventSubscriber subscriber);

    public List<String> getCollectLocations(FeedConnectionId feedConnectionId);

    boolean isFeedConnectionActive(FeedConnectionId connectionId);

}
