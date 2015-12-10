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
package org.apache.asterix.active;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.active.ActiveObjectId;
import org.apache.asterix.common.active.api.IActiveJobLifeCycleListener.ConnectionLocation;
import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedConnectionRequest;
import org.apache.asterix.common.feeds.FeedJointKey;
import org.apache.asterix.common.feeds.api.IFeedJoint;

public class FeedJoint implements IFeedJoint {

    private static final Logger LOGGER = Logger.getLogger(FeedJoint.class.getName());

    /** A unique key associated with the feed point **/
    private final FeedJointKey key;

    /** The state associated with the FeedJoint **/
    private State state;

    /** A list of subscribers that receive data from this FeedJoint **/
    private final List<FeedConnectionId> receivers;

    /** The feedId on which the feedPoint resides **/
    private final ActiveObjectId ownerFeedId;

    /** A list of feed subscription requests submitted for subscribing to the FeedPoint's data **/
    private final List<FeedConnectionRequest> connectionRequests;

    private final ConnectionLocation connectionLocation;

    private final FeedJointType type;

    private ActiveJobId provider;

    public FeedJoint(FeedJointKey key, ActiveObjectId ownerFeedId, ConnectionLocation subscriptionLocation, FeedJointType type,
            ActiveJobId provider) {
        this.key = key;
        this.ownerFeedId = ownerFeedId;
        this.type = type;
        this.receivers = new ArrayList<FeedConnectionId>();
        this.state = State.CREATED;
        this.connectionLocation = subscriptionLocation;
        this.connectionRequests = new ArrayList<FeedConnectionRequest>();
        this.provider = provider;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    public void addReceiver(FeedConnectionId connectionId) {
        receivers.add(connectionId);
    }

    public void removeReceiver(ActiveJobId connectionId) {
        receivers.remove(connectionId);
    }

    public synchronized void addConnectionRequest(FeedConnectionRequest request) {
        connectionRequests.add(request);
        if (state.equals(State.ACTIVE)) {
            handlePendingConnectionRequest();
        }
    }

    public synchronized void setState(State state) {
        if (this.state.equals(state)) {
            return;
        }
        this.state = state;
        if (this.state.equals(State.ACTIVE)) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Feed joint " + this + " is now " + State.ACTIVE);
            }
            handlePendingConnectionRequest();
        }
    }

    private void handlePendingConnectionRequest() {
        for (FeedConnectionRequest connectionRequest : connectionRequests) {
            FeedConnectionId connectionId = new FeedConnectionId(connectionRequest.getReceivingFeedId(),
                    connectionRequest.getTargetDataset());
            try {
                ActiveJobLifecycleListener.INSTANCE.submitFeedConnectionRequest(this, connectionRequest);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Submitted feed connection request " + connectionRequest + " at feed joint " + this);
                }
                addReceiver(connectionId);
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Unsuccessful attempt at submitting connection request " + connectionRequest
                            + " at feed joint " + this + ". Message " + e.getMessage());
                }
                e.printStackTrace();
            }
        }
        connectionRequests.clear();
    }

    public ActiveJobId getReceiver(ActiveJobId connectionId) {
        for (ActiveJobId cid : receivers) {
            if (cid.equals(connectionId)) {
                return cid;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return key.toString() + " [" + connectionLocation + "]" + "[" + state + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof FeedJoint)) {
            return false;
        }
        return ((FeedJoint) o).getFeedJointKey().equals(this.key);
    }

    public ActiveObjectId getOwnerFeedId() {
        return ownerFeedId;
    }

    public List<FeedConnectionRequest> getConnectionRequests() {
        return connectionRequests;
    }

    public ConnectionLocation getConnectionLocation() {
        return connectionLocation;
    }

    public FeedJointType getType() {
        return type;
    }

    @Override
    public ActiveJobId getProvider() {
        return provider;
    }

    public List<FeedConnectionId> getReceivers() {
        return receivers;
    }

    public FeedJointKey getKey() {
        return key;
    }

    public synchronized State getState() {
        return state;
    }

    @Override
    public FeedJointKey getFeedJointKey() {
        return key;
    }

}
