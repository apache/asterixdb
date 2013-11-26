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
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityType;

public class FeedPolicyEnforcer {

    private final FeedConnectionId feedId;
    private final FeedPolicyAccessor feedPolicyAccessor;
    private final FeedActivity feedActivity;

    public FeedPolicyEnforcer(FeedConnectionId feedId, Map<String, String> feedPolicy) {
        this.feedId = feedId;
        this.feedPolicyAccessor = new FeedPolicyAccessor(feedPolicy);
        this.feedActivity = new FeedActivity(feedId.getDataverse(), feedId.getFeedName(), feedId.getDatasetName(),
                null, new HashMap<String, String>());
    }

    public boolean continueIngestionPostSoftwareFailure(Exception e) throws RemoteException, ACIDException {
        boolean continueIngestion = feedPolicyAccessor.continueOnApplicationFailure();
        if (feedPolicyAccessor.logErrorOnFailure()) {
            persistExceptionDetails(e);
        }
        return continueIngestion;
    }

    private synchronized void persistExceptionDetails(Exception e) throws RemoteException, ACIDException {
        MetadataManager.INSTANCE.acquireWriteLatch();
        MetadataTransactionContext ctx = null;
        try {
            ctx = MetadataManager.INSTANCE.beginTransaction();
            feedActivity.setActivityType(FeedActivityType.FEED_FAILURE);
            feedActivity.getFeedActivityDetails().put(FeedActivity.FeedActivityDetails.EXCEPTION_MESSAGE,
                    e.getMessage());
            MetadataManager.INSTANCE.registerFeedActivity(ctx, feedId, feedActivity);
            MetadataManager.INSTANCE.commitTransaction(ctx);
        } catch (Exception e2) {
            MetadataManager.INSTANCE.abortTransaction(ctx);
        } finally {
            MetadataManager.INSTANCE.releaseWriteLatch();
        }
    }

    public FeedPolicyAccessor getFeedPolicyAccessor() {
        return feedPolicyAccessor;
    }

    public FeedConnectionId getFeedId() {
        return feedId;
    }

}
