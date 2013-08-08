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

import edu.uci.ics.asterix.metadata.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.metadata.feeds.MessageListener.IMessageAnalyzer;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;

public class SuperFeedManager {

    private static final Logger LOGGER = Logger.getLogger(SuperFeedManager.class.getName());
    private String host;

    private AtomicInteger availablePort; // starting value is fixed

    private final int feedReportPort; // fixed

    private final int feedReportSubscribePort; // fixed

    private final String nodeId;

    private final FeedConnectionId feedConnectionId;

    private boolean isLocal = false;

    private SuperFeedReportService sfmService;

    private SuperFeedReportSubscriptionService subscriptionService;

    private LinkedBlockingQueue<String> feedReportInbox; ///

    private boolean started = false;

    public static final int PORT_RANGE_ASSIGNED = 10;

    public enum FeedReportMessageType {
        CONGESTION,
        THROUGHPUT
    }

    public SuperFeedManager(FeedConnectionId feedId, String host, String nodeId, int port) throws Exception {
        this.feedConnectionId = feedId;
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
            ExecutorService executorService = FeedManager.INSTANCE.getFeedExecutorService(feedConnectionId);
            sfmService = new SuperFeedReportService(feedReportPort, feedReportInbox, feedConnectionId, availablePort);
            executorService.execute(sfmService);
            subscriptionService = new SuperFeedReportSubscriptionService(feedConnectionId, feedReportSubscribePort,
                    sfmService.getMesgAnalyzer(), availablePort);
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
        private final SFMessageAnalyzer reportProvider;
        private final List<FeedDataProviderService> dataProviders = new ArrayList<FeedDataProviderService>();

        public SuperFeedReportSubscriptionService(FeedConnectionId feedId, int port, SFMessageAnalyzer reportProvider,
                AtomicInteger nextPort) throws IOException {
            this.feedId = feedId;
            serverFeedSubscribe = FeedManager.INSTANCE.getFeedRuntimeManager(feedId).createServerSocket(port);
            this.subscriptionPort = nextPort;
            this.reportProvider = reportProvider;
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
                    FeedDataProviderService dataProviderService = new FeedDataProviderService(feedId, port, reportInbox);
                    dataProviders.add(dataProviderService);
                    FeedManager.INSTANCE.getFeedRuntimeManager(feedId).getExecutorService()
                            .execute(dataProviderService);
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
        private int subscriptionPort;
        private ServerSocket dataProviderSocket;
        private LinkedBlockingQueue<String> inbox;
        private boolean active = true;
        private String EOM = "\n";

        public FeedDataProviderService(FeedConnectionId feedId, int port, LinkedBlockingQueue<String> inbox)
                throws IOException {
            this.feedId = feedId;
            this.subscriptionPort = port;
            this.inbox = inbox;
            dataProviderSocket = FeedManager.INSTANCE.getFeedRuntimeManager(feedId).createServerSocket(port);
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

    private static class SuperFeedReportService implements Runnable {

        private AtomicInteger nextPort;
        private final ServerSocket serverFeedReport;

        private String EOM = "\n";

        private final LinkedBlockingQueue<String> inbox;
        private final List<MessageListener> messageListeners;
        private final SFMessageAnalyzer mesgAnalyzer;
        private final FeedConnectionId feedId;
        private boolean process = true;

        public SuperFeedReportService(int port, LinkedBlockingQueue<String> inbox, FeedConnectionId feedId,
                AtomicInteger availablePort) throws IOException {
            FeedRuntimeManager runtimeManager = FeedManager.INSTANCE.getFeedRuntimeManager(feedId);
            serverFeedReport = runtimeManager.createServerSocket(port);
            nextPort = availablePort;
            this.inbox = inbox;
            this.feedId = feedId;
            this.messageListeners = new ArrayList<MessageListener>();
            mesgAnalyzer = new SFMessageAnalyzer(inbox, feedId);
            FeedManager.INSTANCE.getFeedExecutorService(feedId).execute(mesgAnalyzer);
        }

        public void stop() {
            process = false;
            if (serverFeedReport != null) {
                try {
                    serverFeedReport.close();
                    process = false;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            mesgAnalyzer.stop();
        }

        @Override
        public void run() {
            Socket client = null;
            while (process) {
                try {
                    client = serverFeedReport.accept();
                    OutputStream os = client.getOutputStream();
                    int port = nextPort.incrementAndGet();
                    MessageListener listener = new MessageListener(port, inbox);
                    listener.start();
                    messageListeners.add(listener);
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

        public SFMessageAnalyzer getMesgAnalyzer() {
            return mesgAnalyzer;
        }

    }

    private static class SFMessageAnalyzer implements Runnable {

        private final LinkedBlockingQueue<String> inbox;
        private final FeedConnectionId feedId;
        private boolean process = true;
        private final List<LinkedBlockingQueue<String>> subscriptionQueues;
        private final Map<String, String> ingestionThroughputs;

        public SFMessageAnalyzer(LinkedBlockingQueue<String> inbox, FeedConnectionId feedId)
                throws UnknownHostException, IOException {
            this.inbox = inbox;
            this.feedId = feedId;
            this.subscriptionQueues = new ArrayList<LinkedBlockingQueue<String>>();
            this.ingestionThroughputs = new HashMap<String, String>();
            String ccHost = AsterixClusterProperties.INSTANCE.getCluster().getMasterNode().getClusterIp();
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
            while (process) {
                try {
                    String message = inbox.take();
                    FeedReport report = new FeedReport(message);
                    FeedReportMessageType mesgType = report.getReportType();
                    switch (mesgType) {
                        case THROUGHPUT:
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.warning("SuperFeedManager received message " + message);
                            }
                            for (LinkedBlockingQueue<String> q : subscriptionQueues) {
                                String[] comp = message.split("\\|");
                                String parition = comp[3];
                                String tput = comp[4];
                                ingestionThroughputs.put(parition, tput);
                                for (String tp : ingestionThroughputs.values()) {
                                    finalMessage.append(tp + "|");
                                }
                                q.add(finalMessage.toString());
                                finalMessage.delete(0, finalMessage.length());
                            }
                            break;
                        case CONGESTION:
                            // congestionInbox.add(report);
                            break;
                    }
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning(message);
                    }
                } catch (InterruptedException e) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Unable to process messages " + e.getMessage());
                    }
                }
            }
        }

        private static class SuperFeedManagerMessageAnalzer implements IMessageAnalyzer {

            private String ccHost;
            private CongestionAnalyzer congestionAnalyzer;
            private LinkedBlockingQueue<FeedReport> congestionInbox = new LinkedBlockingQueue<FeedReport>();
            private ExecutorService executorService;
            private LinkedBlockingQueue<String> mesgInbox = new LinkedBlockingQueue<String>();

            public SuperFeedManagerMessageAnalzer(FeedConnectionId feedId) {
                ccHost = AsterixClusterProperties.INSTANCE.getCluster().getMasterNode().getClusterIp();
                congestionAnalyzer = new CongestionAnalyzer(congestionInbox);
                executorService = FeedManager.INSTANCE.getFeedExecutorService(feedId);
                executorService.execute(congestionAnalyzer);
            }

            public void receiveMessage(String message) {
                Socket socket = null;
                OutputStream os = null;
                try {
                    FeedReport report = new FeedReport(message);
                    FeedReportMessageType mesgType = report.getReportType();
                    switch (mesgType) {
                        case THROUGHPUT:
                            //send message to FeedHealthDataReceiver at CC (2999)
                            socket = new Socket(ccHost, 2999);
                            os = socket.getOutputStream();
                            os.write(message.getBytes());
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.warning("SuperFeedManager received message " + message);
                            }
                            break;
                        case CONGESTION:
                            congestionInbox.add(report);
                            break;
                    }
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning(message);
                    }
                    if (os != null) {
                        os.close();
                    }
                    if (socket != null) {
                        socket.close();
                    }
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("unable to send message to FeedHealthDataReceiver");
                    }
                }
            }

            @Override
            public LinkedBlockingQueue<String> getMessageQueue() {
                return mesgInbox;
            }

        }

        private static class CongestionAnalyzer implements Runnable {

            private final LinkedBlockingQueue<FeedReport> inbox;
            private FeedReport lastMaxReport;
            private int congestionCount;
            private long lastReportedCongestion = 0;
            private long closeEnoughTimeBound = FeedFrameWriter.FLUSH_THRESHOLD_TIME * 2;

            public CongestionAnalyzer(LinkedBlockingQueue<FeedReport> inbox) {
                this.inbox = inbox;
            }

            @Override
            public void run() {
                FeedReport report;
                while (true) {
                    try {
                        report = inbox.take();
                        long currentReportedCongestionTime = System.currentTimeMillis();
                        boolean closeEnough = lastReportedCongestion == 0
                                || currentReportedCongestionTime - lastReportedCongestion < closeEnoughTimeBound;
                        if (lastMaxReport == null) {
                            lastMaxReport = report;
                            if (closeEnough) {
                                congestionCount++;
                            }
                        } else {
                            if (report.compareTo(lastMaxReport) > 0) {
                                lastMaxReport = report;
                                congestionCount = 1;
                            } else if (report.compareTo(lastMaxReport) == 0) {
                                lastMaxReport = report;
                                if (closeEnough) {
                                    congestionCount++;
                                    if (congestionCount > 5) {
                                        FeedRuntimeType sourceOfCongestion = null;
                                        switch (lastMaxReport.getRuntimeType()) {
                                            case INGESTION:
                                                sourceOfCongestion = FeedRuntimeType.COMPUTE;
                                                break;
                                            case COMPUTE:
                                                sourceOfCongestion = FeedRuntimeType.STORAGE;
                                                break;
                                            case STORAGE:
                                            case COMMIT:
                                                sourceOfCongestion = FeedRuntimeType.COMMIT;
                                                break;
                                        }
                                        if (LOGGER.isLoggable(Level.WARNING)) {
                                            LOGGER.warning(" Need elasticity at " + sourceOfCongestion
                                                    + " as per report " + lastMaxReport);
                                        }
                                    }
                                }
                            }
                        }
                        lastReportedCongestion = System.currentTimeMillis();

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }

        }
    }
}