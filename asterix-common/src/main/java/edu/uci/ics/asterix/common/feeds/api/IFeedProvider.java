package edu.uci.ics.asterix.common.feeds.api;

import edu.uci.ics.asterix.common.feeds.FeedId;

public interface IFeedProvider {

    public void subscribeFeed(FeedId sourceDeedId);
}
