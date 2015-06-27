package edu.uci.ics.asterix.common.feeds.api;

public interface ITupleTrackingFeedAdapter extends IFeedAdapter {

    public void tuplePersistedTimeCallback(long timestamp);
}
