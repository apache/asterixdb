package edu.uci.ics.asterix.metadata.feeds;

public interface IAdapterExecutor {

    public void start() throws Exception;

    public void stop() throws Exception;

    public FeedId getFeedId();

}
