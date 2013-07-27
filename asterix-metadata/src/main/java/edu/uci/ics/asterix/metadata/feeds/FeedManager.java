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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.metadata.feeds.FeedRuntime.FeedRuntimeId;

/**
 * Handle (de)registration of feeds for delivery of control messages.
 */
public class FeedManager implements IFeedManager {

    private static final Logger LOGGER = Logger.getLogger(FeedManager.class.getName());

    public static FeedManager INSTANCE = new FeedManager();

    private FeedManager() {

    }

    private Map<FeedConnectionId, SuperFeedManager> superFeedManagers = new HashMap<FeedConnectionId, SuperFeedManager>();
    private Map<FeedConnectionId, Map<FeedRuntimeId, FeedRuntime>> feedRuntimes = new HashMap<FeedConnectionId, Map<FeedRuntimeId, FeedRuntime>>();
    private Map<FeedConnectionId, ExecutorService> feedExecutorService = new HashMap<FeedConnectionId, ExecutorService>();

    public ExecutorService getFeedExecutorService(FeedConnectionId feedId) {
        return feedExecutorService.get(feedId);
    }

    @Override
    public void deregisterFeed(FeedConnectionId feedId) {
        try {
            Map<FeedRuntimeId, FeedRuntime> feedRuntimesForFeed = feedRuntimes.get(feedId);
            if (feedRuntimesForFeed != null) {
                feedRuntimesForFeed.clear();
            }

            feedRuntimes.remove(feedId);

            SuperFeedManager sfm = superFeedManagers.get(feedId);
            if (sfm != null && sfm.isLocal()) {
                sfm.stop();
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Shutdown super feed manager " + sfm);
                }
            }

            ExecutorService executorService = feedExecutorService.remove(feedId);
            if (executorService != null && !executorService.isShutdown()) {
                executorService.shutdownNow();
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("unable to shutdown feed services for" + feedId);
            }
        }
    }

    @Override
    public ExecutorService registerFeedRuntime(FeedRuntime feedRuntime) {
        ExecutorService execService = feedExecutorService.get(feedRuntime.getFeedRuntimeId().getFeedId());
        if (execService == null) {
            execService = Executors.newCachedThreadPool();
            feedExecutorService.put(feedRuntime.getFeedRuntimeId().getFeedId(), execService);
        }

        Map<FeedRuntimeId, FeedRuntime> feedRuntimesForFeed = feedRuntimes.get(feedRuntime.getFeedRuntimeId()
                .getFeedId());
        if (feedRuntimesForFeed == null) {
            feedRuntimesForFeed = new HashMap<FeedRuntimeId, FeedRuntime>();
            feedRuntimes.put(feedRuntime.getFeedRuntimeId().getFeedId(), feedRuntimesForFeed);
        }
        feedRuntimesForFeed.put(feedRuntime.getFeedRuntimeId(), feedRuntime);
        System.out.println("REGISTERED feed runtime " + feedRuntime);
        return execService;
    }

    @Override
    public void deRegisterFeedRuntime(FeedRuntimeId feedRuntimeId) {
        Map<FeedRuntimeId, FeedRuntime> feedRuntimesForFeed = feedRuntimes.get(feedRuntimeId.getFeedId());
        if (feedRuntimesForFeed != null) {
            FeedRuntime feedRuntime = feedRuntimesForFeed.get(feedRuntimeId);
            if (feedRuntime != null) {
                feedRuntimesForFeed.remove(feedRuntimeId);
                if (feedRuntimesForFeed.isEmpty()) {
                    System.out.println("CLEARING OUT FEED RUNTIME INFO" + feedRuntimeId.getFeedId());
                    feedRuntimes.remove(feedRuntimeId.getFeedId());
                }
            }
        }

    }

    @Override
    public FeedRuntime getFeedRuntime(FeedRuntimeId feedRuntimeId) {
        Map<FeedRuntimeId, FeedRuntime> feedRuntimesForFeed = feedRuntimes.get(feedRuntimeId.getFeedId());
        if (feedRuntimesForFeed != null) {
            return feedRuntimesForFeed.get(feedRuntimeId);
        }
        return null;
    }

    @Override
    public void registerSuperFeedManager(FeedConnectionId feedId, SuperFeedManager sfm) {
        superFeedManagers.put(feedId, sfm);
    }

    @Override
    public void deregisterSuperFeedManager(FeedConnectionId feedId) {
        SuperFeedManager sfm = superFeedManagers.remove(feedId);
        try {
            sfm.stop();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public SuperFeedManager getSuperFeedManager(FeedConnectionId feedId) {
        return superFeedManagers.get(feedId);
    }

}
