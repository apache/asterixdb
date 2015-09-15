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
package org.apache.asterix.common.feeds.message;

import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.asterix.common.config.AsterixFeedProperties;
import org.apache.asterix.common.feeds.FeedConstants;
import org.apache.asterix.common.feeds.api.IFeedMessage;
import org.apache.asterix.common.feeds.api.IFeedMessageService;

/**
 * Sends feed report messages on behalf of an operator instance
 * to the SuperFeedManager associated with the feed.
 */
public class FeedMessageService implements IFeedMessageService {

    private static final Logger LOGGER = Logger.getLogger(FeedMessageService.class.getName());

    private final LinkedBlockingQueue<String> inbox;
    private final FeedMessageHandler mesgHandler;
    private final String nodeId;
    private ExecutorService executor;

    public FeedMessageService(AsterixFeedProperties feedProperties, String nodeId, String ccClusterIp) {
        this.inbox = new LinkedBlockingQueue<String>();
        this.mesgHandler = new FeedMessageHandler(inbox, ccClusterIp, feedProperties.getFeedCentralManagerPort());
        this.nodeId = nodeId;
        this.executor = Executors.newSingleThreadExecutor();
    }

    public void start() throws Exception {
        executor.execute(mesgHandler);
    }

    public void stop() {
        synchronized (mesgHandler.getLock()) {
            executor.shutdownNow();
        }
        mesgHandler.stop();
    }

    @Override
    public void sendMessage(IFeedMessage message) {
        try {
            JSONObject obj = message.toJSON();
            obj.put(FeedConstants.MessageConstants.NODE_ID, nodeId);
            obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, message.getMessageType().name());
            inbox.add(obj.toString());
        } catch (JSONException jse) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("JSON Exception in parsing message " + message);
            }
        }
    }

    private static class FeedMessageHandler implements Runnable {

        private final LinkedBlockingQueue<String> inbox;
        private final String host;
        private final int port;

        private Socket cfmSocket;
        private Object lock;

        private static final byte[] EOL = "\n".getBytes();

        public FeedMessageHandler(LinkedBlockingQueue<String> inbox, String host, int port) {
            this.inbox = inbox;
            this.host = host;
            this.port = port;
            this.lock = new Object();
        }

        public void run() {
            try {
                cfmSocket = new Socket(host, port);
                if (cfmSocket != null) {
                    while (true) {
                        String message = inbox.take();
                        synchronized (lock) {
                            cfmSocket.getOutputStream().write(message.getBytes());
                            cfmSocket.getOutputStream().write(EOL);
                        }
                    }
                } else {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Unable to start feed message service");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Exception in handling incoming feed messages" + e.getMessage());
                }
            } finally {
                stop();
            }

        }

        public void stop() {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Stopping feed message handler");
            }
            if (cfmSocket != null) {
                try {
                    cfmSocket.close();
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Exception in closing socket " + e.getMessage());
                    }
                }
            }
        }

        public Object getLock() {
            return lock;
        }

    }

}
