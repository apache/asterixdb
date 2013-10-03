/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.feeds;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeId;

/**
 * Handle (de)registration of feeds for delivery of control messages.
 */
public interface IFeedManager {

    public static final long SOCKET_CONNECT_TIMEOUT = 5000;

    /**
     * Returns the executor service associated with the feed.
     * 
     * @param feedId
     * @return
     */
    public ExecutorService getFeedExecutorService(FeedConnectionId feedId);

    /**
     * Allows registration of a feedRuntime.
     * 
     * @param feedRuntime
     * @throws Exception
     */
    public void registerFeedRuntime(FeedRuntime feedRuntime) throws Exception;

    /**
     * Allows de-registration of a feed runtime.
     * 
     * @param feedRuntimeId
     */
    public void deRegisterFeedRuntime(FeedRuntimeId feedRuntimeId);

    /**
     * Obtain feed runtime corresponding to a feedRuntimeId
     * 
     * @param feedRuntimeId
     * @return
     */
    public FeedRuntime getFeedRuntime(FeedRuntimeId feedRuntimeId);

    /**
     * Register the Super Feed Manager associated witht a feed.
     * 
     * @param feedId
     * @param sfm
     * @throws Exception
     */
    public void registerSuperFeedManager(FeedConnectionId feedId, SuperFeedManager sfm) throws Exception;

    /**
     * Obtain a handle to the Super Feed Manager associated with the feed.
     * 
     * @param feedId
     * @return
     */
    public SuperFeedManager getSuperFeedManager(FeedConnectionId feedId);

    /**
     * De-register a feed
     * 
     * @param feedId
     * @throws IOException
     */
    void deregisterFeed(FeedConnectionId feedId);

    /**
     * Obtain the feed runtime manager associated with a feed.
     * 
     * @param feedId
     * @return
     */
    public FeedRuntimeManager getFeedRuntimeManager(FeedConnectionId feedId);

    /**
     * Obtain a handle to the feed Message service associated with a feed.
     * 
     * @param feedId
     * @return
     */
    public FeedMessageService getFeedMessageService(FeedConnectionId feedId);

}
