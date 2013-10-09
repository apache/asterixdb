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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeId;

public class FeedRuntimeManager {

    private static Logger LOGGER = Logger.getLogger(FeedRuntimeManager.class.getName());

    private final FeedConnectionId feedId;
    private final IFeedManager feedManager;
    private SuperFeedManager superFeedManager;
    private final Map<FeedRuntimeId, FeedRuntime> feedRuntimes;
    private final ExecutorService executorService;
    private FeedMessageService messageService;
    private SocketFactory socketFactory = new SocketFactory();
    private final LinkedBlockingQueue<String> feedReportQueue;

    public FeedRuntimeManager(FeedConnectionId feedId, IFeedManager feedManager) {
        this.feedId = feedId;
        feedRuntimes = new ConcurrentHashMap<FeedRuntimeId, FeedRuntime>();
        executorService = Executors.newCachedThreadPool();
        feedReportQueue = new LinkedBlockingQueue<String>();
        this.feedManager = feedManager;
    }

    public void close(boolean closeAll) throws IOException {
        socketFactory.close();

        if (messageService != null) {
            messageService.stop();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Shut down message services for :" + feedId);
            }
            messageService = null;
        }
        if (superFeedManager != null && superFeedManager.isLocal()) {
            superFeedManager.stop();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Shut down super feed manager for :" + feedId);
            }
        }

        if (closeAll) {
            if (executorService != null) {
                executorService.shutdownNow();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Shut down executor service for :" + feedId);
                }
            }
        }
    }

    public void setSuperFeedManager(SuperFeedManager sfm) throws UnknownHostException, IOException, Exception {
        this.superFeedManager = sfm;
        if (sfm.isLocal()) {
            sfm.start();
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Started Super Feed Manager for feed :" + feedId);
        }
        this.messageService = new FeedMessageService(feedId, feedManager);
        messageService.start();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Started Feed Message Service for feed :" + feedId);
        }
    }

    public SuperFeedManager getSuperFeedManager() {
        return superFeedManager;
    }

    public FeedRuntime getFeedRuntime(FeedRuntimeId runtimeId) {
        return feedRuntimes.get(runtimeId);
    }

    public void registerFeedRuntime(FeedRuntimeId runtimeId, FeedRuntime feedRuntime) {
        feedRuntimes.put(runtimeId, feedRuntime);
    }

    public void deregisterFeedRuntime(FeedRuntimeId runtimeId) {
        feedRuntimes.remove(runtimeId);
        if (feedRuntimes.isEmpty()) {
            synchronized (this) {
                if (feedRuntimes.isEmpty()) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("De-registering feed");
                    }
                    feedManager.deregisterFeed(runtimeId.getFeedId());
                }
            }
        }
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public FeedMessageService getMessageService() {
        return messageService;
    }

    public Socket createClientSocket(String host, int port) throws UnknownHostException, IOException {
        return socketFactory.createClientSocket(host, port);
    }

    public Socket createClientSocket(String host, int port, long timeout) throws UnknownHostException, IOException {
        Socket client = null;
        boolean continueAttempt = true;
        long startAttempt = System.currentTimeMillis();
        long endAttempt = System.currentTimeMillis();
        while (client == null && continueAttempt) {
            try {
                client = socketFactory.createClientSocket(host, port);
            } catch (Exception e) {
                endAttempt = System.currentTimeMillis();
                if (endAttempt - startAttempt > timeout) {
                    continueAttempt = false;
                }
            }
        }
        return client;
    }

    public ServerSocket createServerSocket(int port) throws IOException {
        return socketFactory.createServerSocket(port);
    }

    private static class SocketFactory {

        private final Map<SocketId, Socket> sockets = new HashMap<SocketId, Socket>();
        private final List<ServerSocket> serverSockets = new ArrayList<ServerSocket>();

        public Socket createClientSocket(String host, int port) throws UnknownHostException, IOException {
            Socket socket = new Socket(host, port);
            sockets.put(new SocketId(host, port), socket);
            return socket;
        }

        public void close() throws IOException {
            for (ServerSocket socket : serverSockets) {
                socket.close();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Closed server socket :" + socket);
                }
            }

            for (Entry<SocketId, Socket> entry : sockets.entrySet()) {
                entry.getValue().close();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Closed client socket :" + entry.getKey());
                }
            }
        }

        public ServerSocket createServerSocket(int port) throws IOException {
            ServerSocket socket = new ServerSocket(port);
            serverSockets.add(socket);
            return socket;
        }

        private static class SocketId {
            private final String host;
            private final int port;

            public SocketId(String host, int port) {
                this.host = host;
                this.port = port;
            }

            public String getHost() {
                return host;
            }

            public int getPort() {
                return port;
            }

            @Override
            public String toString() {
                return host + "[" + port + "]";
            }

            @Override
            public int hashCode() {
                return toString().hashCode();
            }

            @Override
            public boolean equals(Object o) {
                if (!(o instanceof SocketId)) {
                    return false;
                }

                return ((SocketId) o).getHost().equals(host) && ((SocketId) o).getPort() == port;
            }

        }
    }

    public FeedConnectionId getFeedId() {
        return feedId;
    }

    public LinkedBlockingQueue<String> getFeedReportQueue() {
        return feedReportQueue;
    }

}
