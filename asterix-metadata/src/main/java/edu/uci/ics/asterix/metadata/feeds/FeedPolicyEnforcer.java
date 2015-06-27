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
package edu.uci.ics.asterix.metadata.feeds;

import java.rmi.RemoteException;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;

public class FeedPolicyEnforcer {

    private final FeedConnectionId connectionId;
    private final FeedPolicyAccessor policyAccessor;

    public FeedPolicyEnforcer(FeedConnectionId feedConnectionId, Map<String, String> feedPolicy) {
        this.connectionId = feedConnectionId;
        this.policyAccessor = new FeedPolicyAccessor(feedPolicy);
    }

    public boolean continueIngestionPostSoftwareFailure(Exception e) throws RemoteException, ACIDException {
        return policyAccessor.continueOnSoftFailure();
    }

    public FeedPolicyAccessor getFeedPolicyAccessor() {
        return policyAccessor;
    }

    public FeedConnectionId getFeedId() {
        return connectionId;
    }

}
