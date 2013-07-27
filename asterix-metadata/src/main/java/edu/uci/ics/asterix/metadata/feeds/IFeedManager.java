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
package edu.uci.ics.asterix.metadata.feeds;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import edu.uci.ics.asterix.metadata.feeds.FeedRuntime.FeedRuntimeId;

/**
 * Handle (de)registration of feeds for delivery of control messages.
 */
public interface IFeedManager {

    /**
     * @param feedId
     * @return
     */
    public ExecutorService getFeedExecutorService(FeedConnectionId feedId);

    /**
     * @param feedRuntime
     */
    public ExecutorService registerFeedRuntime(FeedRuntime feedRuntime);

    /**
     * @param feedRuntimeId
     */
    public void deRegisterFeedRuntime(FeedRuntimeId feedRuntimeId);

    /**
     * @param feedRuntimeId
     * @return
     */
    public FeedRuntime getFeedRuntime(FeedRuntimeId feedRuntimeId);

    /**
     * @param feedId
     * @param sfm
     */
    public void registerSuperFeedManager(FeedConnectionId feedId, SuperFeedManager sfm);

    /**
     * @param feedId
     */
    public void deregisterSuperFeedManager(FeedConnectionId feedId);

    /**
     * @param feedId
     * @return
     */
    public SuperFeedManager getSuperFeedManager(FeedConnectionId feedId);

    /**
     * @param feedId
     * @throws IOException 
     */
    void deregisterFeed(FeedConnectionId feedId) throws IOException;

}
