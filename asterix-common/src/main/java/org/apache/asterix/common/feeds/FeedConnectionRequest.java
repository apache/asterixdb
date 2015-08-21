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
package edu.uci.ics.asterix.common.feeds;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import edu.uci.ics.asterix.common.feeds.api.IFeedLifecycleListener.ConnectionLocation;

/**
 * A request for connecting a feed to a dataset.
 */
public class FeedConnectionRequest {

    public enum ConnectionStatus {
        /** initial state upon creating a connection request **/
        INITIALIZED,

        /** connection establish; feed is receiving data **/
        ACTIVE,

        /** connection removed; feed is not receiving data **/
        INACTIVE,

        /** connection request failed **/
        FAILED
    }

    /** Feed joint on the feed pipeline that serves as the source for this subscription **/
    private final FeedJointKey feedJointKey;

    /** Location in the source feed pipeline from where feed tuples are received. **/
    private final ConnectionLocation connectionLocation;

    /** List of functions that need to be applied in sequence after the data hand-off at the source feedPointKey. **/
    private final List<String> functionsToApply;

    /** Status associated with the subscription. */
    private ConnectionStatus connectionStatus;

    /** Name of the policy that governs feed ingestion **/
    private final String policy;

    /** Policy associated with a feed connection **/
    private final Map<String, String> policyParameters;

    /** Target dataset associated with the connection request **/
    private final String targetDataset;

    private final FeedId receivingFeedId;

    
    public FeedConnectionRequest(FeedJointKey feedPointKey, ConnectionLocation connectionLocation,
            List<String> functionsToApply, String targetDataset, String policy, Map<String, String> policyParameters,
            FeedId receivingFeedId) {
        this.feedJointKey = feedPointKey;
        this.connectionLocation = connectionLocation;
        this.functionsToApply = functionsToApply;
        this.targetDataset = targetDataset;
        this.policy = policy;
        this.policyParameters = policyParameters;
        this.receivingFeedId = receivingFeedId;
        this.connectionStatus = ConnectionStatus.INITIALIZED;
    }

    public FeedJointKey getFeedJointKey() {
        return feedJointKey;
    }

    public ConnectionStatus getConnectionStatus() {
        return connectionStatus;
    }

    public void setSubscriptionStatus(ConnectionStatus connectionStatus) {
        this.connectionStatus = connectionStatus;
    }

    public String getPolicy() {
        return policy;
    }

    public String getTargetDataset() {
        return targetDataset;
    }

    public ConnectionLocation getSubscriptionLocation() {
        return connectionLocation;
    }

    public FeedId getReceivingFeedId() {
        return receivingFeedId;
    }

    public Map<String, String> getPolicyParameters() {
        return policyParameters;
    }

    public List<String> getFunctionsToApply() {
        return functionsToApply;
    }

    @Override
    public String toString() {
        return "Feed Connection Request " + feedJointKey + " [" + connectionLocation + "]" + " Apply ("
                + StringUtils.join(functionsToApply, ",") + ")";
    }

}
