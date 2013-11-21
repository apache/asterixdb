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
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The feed operators running in an NC report their health (statistics) to the local Feed Manager.
 * A feed thus has a Feed Manager per NC. From amongst the Feed Maanger, a SuperFeedManager is chosen (randomly)
 * The SuperFeedManager collects reports from the FeedMaangers and has the global cluster view in terms of
 * how different feed operators running in a distributed fashion are performing.
 */
public class SuperFeedManager {

    private static final Logger LOGGER = Logger.getLogger(SuperFeedManager.class.getName());

    /**
     * IP Address or DNS name of the host where Super Feed Manager is running.
     */
    private String host;

    private AtomicInteger availablePort; // starting value is fixed

    /**
     * The port at which the SuperFeedManager listens for connections by other Feed Managers.
     */
    private final int feedReportPort; // fixed

    /**
     * The port at which the SuperFeedManager listens for connections by clients that wish
     * to subscribe to the feed health reports.E.g. feed management console.
     */
    private final int feedReportSubscribePort; // fixed

    /**
     * The Id of Node Controller
     */
    private final String nodeId;

    /**
     * A unique identifier for the feed instance. A feed instance represents the flow of data
     * from a feed to a dataset.
     **/
    private final FeedConnectionId feedConnectionId;

    /**
     * Set to true of the Super Feed Manager is local to the NC.
     **/
    private boolean isLocal = false;

    private FeedReportDestinationSocketProvider sfmService;

    private SuperFeedReportSubscriptionService subscriptionService;

    private LinkedBlockingQueue<String> feedReportInbox; ///

    private boolean started = false;

    private final IFeedManager feedManager;

    public static final int PORT_RANGE_ASSIGNED = 10;

    public enum FeedReportMessageType {
        CONGESTION,
        THROUGHPUT
    }

    public SuperFeedManager(FeedConnectionId feedId, String host, String nodeId, int port, IFeedManager feedManager)
            throws Exception {
        this.feedConnectionId = feedId;
        this.feedManager = feedManager;
        this.nodeId = nodeId;
        this.feedReportPort = port;
        this.feedReportSubscribePort = port + 1;
        this.availablePort = new AtomicInteger(feedReportSubscribePort + 1);
        this.host = host;
        this.feedReportInbox = new LinkedBlockingQueue<String>();
    }

    public int getPort() {
        return feedReportPort;
    }

    public String getHost() throws Exception {
        return host;
    }

    public String getNodeId() {
        return nodeId;
    }

    public FeedConnectionId getFeedConnectionId() {
        return feedConnectionId;
    }

    public boolean isLocal() {
        return isLocal;
    }

    public void setLocal(boolean isLocal) {
        this.isLocal = isLocal;
    }

    public void start() throws IOException {
        if (sfmService == null) {
            ExecutorService executorService = feedManager.getFeedExecutorService(feedConnectionId);
            sfmService = new FeedReportDestinationSocketProvider(feedReportPort, feedReportInbox, feedConnectionId,
                    availablePort, feedManager);
            executorService.execute(sfmService);
            subscriptionService = new SuperFeedReportSubscriptionService(feedConnectionId, feedReportSubscribePort,
                    sfmService.getMesgAnalyzer(), availablePort, feedManager);
            executorService.execute(subscriptionService);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Started super feed manager! " + this);
        }
        started = true;
    }

    public void stop() throws IOException {
        sfmService.stop();
        subscriptionService.stop();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Stopped super feed manager! " + this);
        }
        started = false;
    }

    public boolean isStarted() {
        return started;
    }

    @Override
    public String toString() {
        return feedConnectionId + "[" + nodeId + "(" + host + ")" + ":" + feedReportPort + "]"
                + (isLocal ? started ? "Started " : "Not Started" : " Remote ");
    }

    public AtomicInteger getAvailablePort() {
        return availablePort;
    }

    private static class SuperFeedReportSubscriptionService implements Runnable {

        private final FeedConnectionId feedId;
        private ServerSocket serverFeedSubscribe;
        private AtomicInteger subscriptionPort;
        private boolean active = true;
        private String EOM = "\n";
        private final FeedReportProvider reportProvider;
        private final List<FeedDataProviderService> dataProviders = new ArrayList<FeedDataProviderService>();
        private final IFeedManager feedManager;

        public SuperFeedReportSubscriptionService(FeedConnectionId feedId, int port, FeedReportProvider reportProvider,
                AtomicInteger nextPort, IFeedManager feedManager) throws IOException {
            this.feedId = feedId;
            serverFeedSubscribe = feedManager.getFeedRuntimeManager(feedId).createServerSocket(port);
            this.subscriptionPort = nextPort;
            this.reportProvider = reportProvider;
            this.feedManager = feedManager;
        }

        public void stop() {
            active = false;
            for (FeedDataProviderService dataProviderService : dataProviders) {
                dataProviderService.stop();
            }
        }

        @Override
        public void run() {
            while (active) {
                try {
                    Socket client = serverFeedSubscribe.accept();
                    OutputStream os = client.getOutputStream();
                    int port = subscriptionPort.incrementAndGet();
                    LinkedBlockingQueue<String> reportInbox = new LinkedBlockingQueue<String>();
                    reportProvider.registerSubsription(reportInbox);
                    FeedDataProviderService dataProviderService = new FeedDataProviderService(feedId, port,
                            reportInbox, feedManager);
                    dataProviders.add(dataProviderService);
                    feedManager.getFeedRuntimeManager(feedId).getExecutorService().execute(dataProviderService);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Recevied subscription request for feed :" + feedId
                                + " Subscripton available at port " + subscriptionPort);
                    }
                    os.write((port + EOM).getBytes());
                    os.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class FeedDataProviderService implements Runnable {

        private final FeedConnectionId feedId;
        private final IFeedManager feedManager;
        private int subscriptionPort;
        private ServerSocket dataProviderSocket;
        private LinkedBlockingQueue<String> inbox;
        private boolean active = true;
        private String EOM = "\n";

        public FeedDataProviderService(FeedConnectionId feedId, int port, LinkedBlockingQueue<String> inbox,
                IFeedManager feedManager) throws IOException {
            this.feedId = feedId;
            this.subscriptionPort = port;
            this.inbox = inbox;
            dataProviderSocket = feedManager.getFeedRuntimeManager(feedId).createServerSocket(port);
            this.feedManager = feedManager;
        }

        @Override
        public void run() {
            try {
                Socket client = dataProviderSocket.accept();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Subscriber to " + feedId + " data connected");
                }
                OutputStream os = client.getOutputStream();
                while (active) {
                    String message = inbox.take();
                    os.write((message + EOM).getBytes());
                }
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Unsubscribed from " + feedId + " disconnected");
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void stop() {
            active = false;
        }

        @Override
        public String toString() {
            return "DATA_PROVIDER_" + feedId + "[" + subscriptionPort + "]";
        }

    }

    private static class FeedReportDestinationSocketProvider implements Runnable {

        private static final String EOM = "\n";

        private AtomicInteger nextPort;
        private final ServerSocket feedReportSocket;
        private final LinkedBlockingQueue<String> inbox;
        private final List<MessageListener> messageListeners;
        private final FeedReportProvider mesgAnalyzer;
        private final FeedConnectionId feedId;
        private boolean process = true;

        public FeedReportDestinationSocketProvider(int port, LinkedBlockingQueue<String> inbox,
                FeedConnectionId feedId, AtomicInteger availablePort, IFeedManager feedManager) throws IOException {
            FeedRuntimeManager runtimeManager = feedManager.getFeedRuntimeManager(feedId);
            this.feedReportSocket = runtimeManager.createServerSocket(port);
            this.nextPort = availablePort;
            this.inbox = inbox;
            this.feedId = feedId;
            this.messageListeners = new ArrayList<MessageListener>();
            this.mesgAnalyzer = new FeedReportProvider(inbox, feedId);
            feedManager.getFeedExecutorService(feedId).execute(mesgAnalyzer);
        }

        public void stop() {
            process = false;
            if (feedReportSocket != null) {
                try {
                    feedReportSocket.close();
                    process = false;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            for (MessageListener listener : messageListeners) {
                listener.stop();
            }
            mesgAnalyzer.stop();
        }

        @Override
        public void run() {
            Socket client = null;
            while (process) {
                try {
                    client = feedReportSocket.accept();
                    int port = nextPort.incrementAndGet();
                    /**
                     * MessageListener provides the functionality of listening at a port for messages
                     * and delivering each received message to an input queue (inbox).
                     */
                    MessageListener listener = new MessageListener(port, inbox);
                    listener.start();
                    synchronized (messageListeners) {
                        messageListeners.add(listener);
                    }
                    OutputStream os = client.getOutputStream();
                    os.write((port + EOM).getBytes());
                    os.flush();
                } catch (IOException e) {
                    if (process == false) {
                        break;
                    }
                } finally {
                    if (client != null) {
                        try {
                            client.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        public FeedReportProvider getMesgAnalyzer() {
            return mesgAnalyzer;
        }

    }

    /**
     * The report messages sent by the feed operators are sent to the FeedReportProvider.
     * FeedReportMessageAnalyzer is responsible for distributing the messages to the subscribers.
     * The Feed Management Console is an example of a subscriber.
     */
    private static class FeedReportProvider implements Runnable {

        private final LinkedBlockingQueue<String> inbox;
        private final FeedConnectionId feedId;
        private boolean process = true;
        private final List<LinkedBlockingQueue<String>> subscriptionQueues;
        private final Map<String, String> ingestionThroughputs;

        public FeedReportProvider(LinkedBlockingQueue<String> inbox, FeedConnectionId feedId)
                throws UnknownHostException, IOException {
            this.inbox = inbox;
            this.feedId = feedId;
            this.subscriptionQueues = new ArrayList<LinkedBlockingQueue<String>>();
            this.ingestionThroughputs = new HashMap<String, String>();
        }

        public void stop() {
            process = false;
        }

        public void registerSubsription(LinkedBlockingQueue<String> subscriptionQueue) {
            subscriptionQueues.add(subscriptionQueue);
        }

        public void deregisterSubsription(LinkedBlockingQueue<String> subscriptionQueue) {
            subscriptionQueues.remove(subscriptionQueue);
        }

        public void run() {
            StringBuilder finalMessage = new StringBuilder();
            FeedReport report = new FeedReport();
            while (process) {
                try {
                    String message = inbox.take();
                    report.reset(message);
                    FeedReportMessageType mesgType = report.getReportType();
                    switch (mesgType) {
                        case THROUGHPUT:
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.warning("Feed Health Report " + message);
                            }
                            String[] msgComponents = message.split("\\|");
                            String partition = msgComponents[3];
                            String tput = msgComponents[4];
                            String timestamp = msgComponents[6];

                            boolean dispatchReport = true;
                            if (ingestionThroughputs.get(partition) == null) {
                                ingestionThroughputs.put(partition, tput);
                                dispatchReport = false;
                            } else {
                                for (int i = 0; i < ingestionThroughputs.size(); i++) {
                                    String tp = ingestionThroughputs.get(i + "");
                                    if (tp != null) {
                                        ingestionThroughputs.put(i + "", null);
                                        finalMessage.append(tp + FeedMessageService.MessageSeparator);
                                    } else {
                                        dispatchReport = false;
                                        break;
                                    }
                                }
                                ingestionThroughputs.put(partition, tput);
                            }

                            if (dispatchReport) {
                                String dispatchedReport = finalMessage.toString();
                                if (LOGGER.isLoggable(Level.INFO)) {
                                    LOGGER.info("Dispatched report " + dispatchedReport);
                                }
                                for (LinkedBlockingQueue<String> q : subscriptionQueues) {
                                    q.add(dispatchedReport);
                                }
                            }
                            finalMessage.delete(0, finalMessage.length());
                            break;
                        case CONGESTION:
                            // congestionInbox.add(report);
                            break;
                    }
                } catch (InterruptedException e) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Unable to process messages " + e.getMessage() + " for feed " + feedId);
                    }
                }
            }
        }

    }
}
