package edu.uci.ics.asterix.common.feeds.api;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedIntakeInfo;

public interface IFeedLifecycleIntakeEventSubscriber extends IFeedLifecycleEventSubscriber {

    public void handleFeedEvent(FeedIntakeInfo iInfo, FeedLifecycleEvent event) throws AsterixException;

}
