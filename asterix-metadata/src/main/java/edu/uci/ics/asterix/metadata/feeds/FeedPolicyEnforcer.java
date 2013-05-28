package edu.uci.ics.asterix.metadata.feeds;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityType;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;

public class FeedPolicyEnforcer {

    private final FeedId feedId;
    private final FeedPolicyAccessor feedPolicyAccessor;
    private final FeedActivity feedActivity;

    public FeedPolicyEnforcer(FeedId feedId, Map<String, String> feedPolicy) {
        this.feedId = feedId;
        this.feedPolicyAccessor = new FeedPolicyAccessor(feedPolicy);
        this.feedActivity = new FeedActivity(feedId.getDataverse(), feedId.getDataset(), null,
                new HashMap<String, String>());
    }

    public boolean handleSoftwareFailure(Exception e) throws RemoteException, ACIDException {
        boolean continueIngestion = feedPolicyAccessor.continueOnApplicationFailure();
        if (feedPolicyAccessor.persistExceptionDetailsOnApplicationFailure()) {
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

    public void handleHardwareFailure(List<String> nodeId) {

    }

    public FeedPolicyAccessor getFeedPolicyAccessor() {
        return feedPolicyAccessor;
    }

    public FeedId getFeedId() {
        return feedId;
    }

}
