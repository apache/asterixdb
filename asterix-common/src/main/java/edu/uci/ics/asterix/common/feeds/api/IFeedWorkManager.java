package edu.uci.ics.asterix.common.feeds.api;

public interface IFeedWorkManager {

    public void submitWork(IFeedWork work, IFeedWorkEventListener listener);

}
