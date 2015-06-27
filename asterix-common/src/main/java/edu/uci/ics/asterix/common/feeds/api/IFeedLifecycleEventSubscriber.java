package edu.uci.ics.asterix.common.feeds.api;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConnectJobInfo;

public interface IFeedLifecycleEventSubscriber {

    public enum FeedLifecycleEvent {
        FEED_INTAKE_STARTED,
        FEED_COLLECT_STARTED,
        FEED_INTAKE_FAILURE,
        FEED_COLLECT_FAILURE,
        FEED_ENDED
    }

    public void assertEvent(FeedLifecycleEvent event) throws AsterixException, InterruptedException;

    public void handleFeedEvent(FeedLifecycleEvent event);
}
