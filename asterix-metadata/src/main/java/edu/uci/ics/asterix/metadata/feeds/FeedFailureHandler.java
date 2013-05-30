package edu.uci.ics.asterix.metadata.feeds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.asterix.common.config.AsterixClusterProperties;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.metadata.feeds.FeedLifecycleListener.FeedFailure;
import edu.uci.ics.asterix.metadata.feeds.FeedLifecycleListener.FeedFailure.FailureType;
import edu.uci.ics.asterix.metadata.feeds.FeedLifecycleListener.FeedFailureReport;
import edu.uci.ics.asterix.metadata.feeds.FeedLifecycleListener.FeedInfo;

public class FeedFailureHandler implements Runnable {

    private LinkedBlockingQueue<FeedFailureReport> inbox = null;

    public FeedFailureHandler(LinkedBlockingQueue<FeedFailureReport> inbox) {
        this.inbox = inbox;
    }

    @Override
    public void run() {
        while (true) {
            try {

                FeedFailureReport failureReport = inbox.take();
                Map<String, Map<FeedInfo, List<FailureType>>> failureMap = new HashMap<String, Map<FeedInfo, List<FailureType>>>();
                for (Map.Entry<FeedInfo, List<FeedFailure>> entry : failureReport.failures.entrySet()) {
                    FeedInfo feedInfo = entry.getKey();
                    List<FeedFailure> feedFailures = entry.getValue();
                    for (FeedFailure feedFailure : feedFailures) {
                        switch (feedFailure.failureType) {
                            case COMPUTE_NODE:
                            case INGESTION_NODE:
                                Map<FeedInfo, List<FailureType>> failuresBecauseOfThisNode = failureMap
                                        .get(feedFailure.nodeId);
                                if (failuresBecauseOfThisNode == null) {
                                    failuresBecauseOfThisNode = new HashMap<FeedInfo, List<FailureType>>();
                                    failuresBecauseOfThisNode.put(feedInfo, new ArrayList<FailureType>());
                                    failureMap.put(feedFailure.nodeId, failuresBecauseOfThisNode);
                                }
                                List<FailureType> feedF = failuresBecauseOfThisNode.get(feedInfo);
                                if (feedF == null) {
                                    feedF = new ArrayList<FailureType>();
                                    failuresBecauseOfThisNode.put(feedInfo, feedF);
                                }
                                feedF.add(feedFailure.failureType);
                                break;
                            case STORAGE_NODE:
                        }
                    }
                }

                correctFailure(failureMap);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private void correctFailure(Map<String, Map<FeedInfo, List<FailureType>>> failureMap) {
        for (String nodeId : failureMap.keySet()) {
            Node node = AsterixClusterProperties.INSTANCE.getAvailableSubstitutionNode();

        }

    }

}
