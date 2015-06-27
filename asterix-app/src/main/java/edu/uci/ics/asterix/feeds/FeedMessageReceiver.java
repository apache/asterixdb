package edu.uci.ics.asterix.feeds;

import java.util.logging.Level;

import org.json.JSONObject;

import edu.uci.ics.asterix.common.feeds.FeedConstants;
import edu.uci.ics.asterix.common.feeds.FeedTupleCommitAckMessage;
import edu.uci.ics.asterix.common.feeds.MessageReceiver;
import edu.uci.ics.asterix.common.feeds.NodeLoadReport;
import edu.uci.ics.asterix.common.feeds.api.IFeedLoadManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedMessage.MessageType;
import edu.uci.ics.asterix.common.feeds.api.IFeedTrackingManager;
import edu.uci.ics.asterix.common.feeds.message.FeedCongestionMessage;
import edu.uci.ics.asterix.common.feeds.message.FeedReportMessage;
import edu.uci.ics.asterix.common.feeds.message.ScaleInReportMessage;
import edu.uci.ics.asterix.common.feeds.message.StorageReportFeedMessage;
import edu.uci.ics.asterix.common.feeds.message.ThrottlingEnabledFeedMessage;
import edu.uci.ics.asterix.feeds.CentralFeedManager.AQLExecutor;
import edu.uci.ics.asterix.hyracks.bootstrap.FeedBootstrap;

public class FeedMessageReceiver extends MessageReceiver<String> {

    private static boolean initialized;

    private final IFeedLoadManager feedLoadManager;
    private final IFeedTrackingManager feedTrackingManager;

    public FeedMessageReceiver(CentralFeedManager centralFeedManager) {
        this.feedLoadManager = centralFeedManager.getFeedLoadManager();
        this.feedTrackingManager = centralFeedManager.getFeedTrackingManager();
    }

    @Override
    public void processMessage(String message) throws Exception {
        JSONObject obj = new JSONObject(message);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Received message " + obj);
        }
        MessageType messageType = MessageType.valueOf(obj.getString(FeedConstants.MessageConstants.MESSAGE_TYPE));
        switch (messageType) {
            case XAQL:
                if (!initialized) {
                    FeedBootstrap.setUpInitialArtifacts();
                    initialized = true;
                }
                AQLExecutor.executeAQL(obj.getString(FeedConstants.MessageConstants.AQL));
                break;
            case CONGESTION:
                feedLoadManager.reportCongestion(FeedCongestionMessage.read(obj));
                break;
            case FEED_REPORT:
                feedLoadManager.submitFeedRuntimeReport(FeedReportMessage.read(obj));
                break;
            case NODE_REPORT:
                feedLoadManager.submitNodeLoadReport(NodeLoadReport.read(obj));
                break;
            case SCALE_IN_REQUEST:
                feedLoadManager.submitScaleInPossibleReport(ScaleInReportMessage.read(obj));
                break;
            case STORAGE_REPORT:
                FeedLifecycleListener.INSTANCE.updateTrackingInformation(StorageReportFeedMessage.read(obj));
                break;
            case COMMIT_ACK:
                feedTrackingManager.submitAckReport(FeedTupleCommitAckMessage.read(obj));
                break;
            case THROTTLING_ENABLED:
                feedLoadManager.reportThrottlingEnabled(ThrottlingEnabledFeedMessage.read(obj));
            default:
                break;
        }

    }
}
