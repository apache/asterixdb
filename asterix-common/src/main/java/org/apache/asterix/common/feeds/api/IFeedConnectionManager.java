/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.common.feeds.api;

import java.io.IOException;
import java.util.List;

import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedRuntime;
import org.apache.asterix.common.feeds.FeedRuntimeId;
import org.apache.asterix.common.feeds.FeedRuntimeManager;

/**
 * Handle (de)registration of feeds for delivery of control messages.
 */
public interface IFeedConnectionManager {

    /**
     * Allows registration of a feedRuntime.
     * 
     * @param feedRuntime
     * @throws Exception
     */
    public void registerFeedRuntime(FeedConnectionId connectionId, FeedRuntime feedRuntime) throws Exception;

    /**
     * Obtain feed runtime corresponding to a feedRuntimeId
     * 
     * @param feedRuntimeId
     * @return
     */
    public FeedRuntime getFeedRuntime(FeedConnectionId connectionId, FeedRuntimeId feedRuntimeId);

    /**
     * De-register a feed
     * 
     * @param feedConnection
     * @throws IOException
     */
    void deregisterFeed(FeedConnectionId feedConnection);

    /**
     * Obtain the feed runtime manager associated with a feed.
     * 
     * @param feedConnection
     * @return
     */
    public FeedRuntimeManager getFeedRuntimeManager(FeedConnectionId feedConnection);

    /**
     * Allows de-registration of a feed runtime.
     * 
     * @param feedRuntimeId
     */
    void deRegisterFeedRuntime(FeedConnectionId connectionId, FeedRuntimeId feedRuntimeId);

    public List<FeedRuntimeId> getRegisteredRuntimes();

}
