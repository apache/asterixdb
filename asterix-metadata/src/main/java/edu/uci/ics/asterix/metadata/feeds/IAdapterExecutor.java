package edu.uci.ics.asterix.metadata.feeds;

public interface IAdapterExecutor {

    /**
     * @throws Exception
     */
    public void start() throws Exception;

    /**
     * @throws Exception
     */
    public void stop() throws Exception;

    /**
     * @return
     */
    public FeedConnectionId getFeedId();

}
