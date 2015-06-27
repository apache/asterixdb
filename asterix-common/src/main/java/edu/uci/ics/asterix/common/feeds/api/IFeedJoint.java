/*
 * Copyright 2009-2014 by The Regents of the University of California
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

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedJointKey;
import edu.uci.ics.asterix.common.feeds.FeedConnectionRequest;
import edu.uci.ics.asterix.common.feeds.api.IFeedLifecycleListener.ConnectionLocation;

public interface IFeedJoint {

    public enum FeedJointType {
        /** Feed Joint is located at the intake stage of a primary feed **/
        INTAKE,

        /** Feed Joint is located at the compute stage of a primary/secondary feed **/
        COMPUTE
    }

    public enum State {
        /** Initial state of a feed joint post creation but prior to scheduling of corresponding Hyracks job. **/
        CREATED,

        /** State acquired post creation of Hyracks job and known physical locations of the joint **/
        INITIALIZED,

        /** State acquired post starting of Hyracks job at which point, data begins to flow through the joint **/
        ACTIVE
    }

    /**
     * @return the {@link State} associated with the FeedJoint
     */
    public State getState();

    /**
     * @return the {@link FeedJointType} associated with the FeedJoint
     */
    public FeedJointType getType();

    /**
     * @return the list of data receivers that are
     *         receiving the data flowing through this FeedJoint
     */
    public List<FeedConnectionId> getReceivers();

    /**
     * @return the list of pending subscription request {@link FeedConnectionRequest} submitted for data flowing through the FeedJoint
     */
    public List<FeedConnectionRequest> getConnectionRequests();

    /**
     * @return the subscription location {@link ConnectionLocation} associated with the FeedJoint
     */
    public ConnectionLocation getConnectionLocation();

    /**
     * @return the unique {@link FeedJointKey} associated with the FeedJoint
     */
    public FeedJointKey getFeedJointKey();

    /**
     * Returns the feed subscriber {@link FeedSubscriber} corresponding to a given feed connection id.
     * 
     * @param feedConnectionId
     *            the unique id of a feed connection
     * @return an instance of feedConnectionId {@link FeedConnectionId}
     */
    public FeedConnectionId getReceiver(FeedConnectionId feedConnectionId);

    /**
     * @param active
     */
    public void setState(State active);

    /**
     * Remove the subscriber from the set of registered subscribers to the FeedJoint
     * 
     * @param connectionId
     *            the connectionId that needs to be removed
     */
    public void removeReceiver(FeedConnectionId connectionId);

    public FeedId getOwnerFeedId();

    /**
     * Add a feed connectionId to the set of registered subscribers
     * 
     * @param connectionId
     */
    public void addReceiver(FeedConnectionId connectionId);

    /**
     * Add a feed subscription request {@link FeedConnectionRequest} for the FeedJoint
     * 
     * @param request
     */
    public void addConnectionRequest(FeedConnectionRequest request);

    public FeedConnectionId getProvider();

}
