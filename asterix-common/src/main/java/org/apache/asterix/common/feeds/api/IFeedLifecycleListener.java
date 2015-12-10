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
package org.apache.asterix.common.feeds.api;

import java.util.List;

import org.apache.asterix.common.active.ActiveObjectId;
import org.apache.asterix.common.active.api.IActiveLifecycleEventSubscriber;
import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.api.IClusterEventsSubscriber;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedJointKey;
import org.apache.hyracks.api.job.IJobLifecycleListener;

public interface IFeedLifecycleListener extends IJobLifecycleListener, IClusterEventsSubscriber {

    public enum ConnectionLocation {
        SOURCE_FEED_INTAKE_STAGE,
        SOURCE_FEED_COMPUTE_STAGE
    }

    public IFeedJoint getAvailableFeedJoint(FeedJointKey feedJoinKey);

    public boolean isFeedJointAvailable(FeedJointKey feedJoinKey);

    public List<FeedConnectionId> getActiveFeedConnections(ActiveObjectId feedId);

    public List<String> getComputeLocations(ActiveObjectId feedId);

    public List<String> getIntakeLocations(ActiveObjectId feedId);

    public List<String> getStoreLocations(ActiveJobId feedId);

    public void registerFeedEventSubscriber(FeedConnectionId connectionId, IActiveLifecycleEventSubscriber subscriber);

    public void deregisterFeedEventSubscriber(ActiveJobId connectionId, IActiveLifecycleEventSubscriber subscriber);

    public List<String> getCollectLocations(ActiveJobId feedConnectionId);

    boolean isFeedConnectionActive(ActiveJobId connectionId);

}
