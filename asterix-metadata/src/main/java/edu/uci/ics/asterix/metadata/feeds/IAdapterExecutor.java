package edu.uci.ics.asterix.metadata.feeds;

import java.util.concurrent.ExecutorService;

public interface IAdapterExecutor {

    public void start() throws Exception;

    public void stop() throws Exception;

    public FeedConnectionId getFeedId();

    public ExecutorService getFeedExecutorService();

}
