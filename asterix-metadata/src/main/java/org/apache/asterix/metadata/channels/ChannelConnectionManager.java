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
package org.apache.asterix.metadata.channels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.channels.ChannelId;
import org.apache.asterix.common.channels.api.IChannelConnectionManager;
import org.apache.asterix.common.channels.api.IChannelRuntime;
import org.apache.asterix.metadata.feeds.FeedConnectionManager;

/**
 * An implementation of the IChannelManager interface.
 * Provider necessary central repository for registering/retrieving
 * artifacts/services associated with a channel.
 */
public class ChannelConnectionManager implements IChannelConnectionManager {

    private static final Logger LOGGER = Logger.getLogger(FeedConnectionManager.class.getName());

    private final Map<ChannelId, IChannelRuntime> channelRuntimes;
    private final String nodeId;
    private final Map<ChannelId, ExecutorService> executorServices;

    public ChannelConnectionManager(String nodeId) {
        this.nodeId = nodeId;
        this.channelRuntimes = new ConcurrentHashMap<ChannelId, IChannelRuntime>();
        this.executorServices = new ConcurrentHashMap<ChannelId, ExecutorService>();
    }

    public void close(ChannelId channelId) throws IOException {
        if (executorServices.get(channelId) != null) {
            executorServices.get(channelId).shutdownNow();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Shut down executor service for :" + channelId);
            }
        }
    }

    @Override
    public IChannelRuntime getChannelRuntime(ChannelId channelId) {
        return channelRuntimes.get(channelId);
    }

    @Override
    public synchronized void registerChannelRuntime(ChannelId channelId, IChannelRuntime channelRuntime)
            throws Exception {
        IChannelRuntime existing = channelRuntimes.get(channelId);
        if (existing == null) {
            channelRuntimes.put(channelId, channelRuntime);
            executorServices.put(channelId, Executors.newCachedThreadPool());
        }
    }

    @Override
    public void deregisterChannelRuntime(ChannelId channelId) throws IOException {
        getChannelRuntime(channelId).drop();
        close(channelId);
        channelRuntimes.remove(channelId);
        executorServices.remove(channelId);
    }

    @Override
    public String toString() {
        return "ChannelConnectionManager " + "[" + nodeId + "]";
    }

    @Override
    public List<ChannelId> getRegisteredRuntimes() {
        List<ChannelId> results = new ArrayList<ChannelId>();
        results.addAll(channelRuntimes.keySet());
        return results;
    }
}
