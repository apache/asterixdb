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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.metadata.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.metadata.feeds.MessageListener.IMessageAnalyzer;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;

public class SuperFeedManager {

    private static final Logger LOGGER = Logger.getLogger(SuperFeedManager.class.getName());
    private String host;

    private final int port;

    private final String nodeId;

    private final FeedConnectionId feedConnectionId;

    private boolean isLocal = false;

    private SuperFeedManagerService sfmService;

    private LinkedBlockingQueue<String> inbox;

    private boolean started = false;

    public enum FeedReportMessageType {
        CONGESTION,
        THROUGHPUT
    }

    public SuperFeedManager(FeedConnectionId feedId, String host, String nodeId, int port) throws Exception {
        this.feedConnectionId = feedId;
        this.nodeId = nodeId;
        this.port = port;
        this.host = host;
        this.inbox = new LinkedBlockingQueue<String>();
    }

    public int getPort() {
        return port;
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
            sfmService = new SuperFeedManagerService(port, inbox, feedConnectionId);
            executorService.execute(sfmService);
        }
        System.out.println("STARTED SUPER FEED MANAGER!");
        started = true;
    }

    public void stop() throws IOException {
        sfmService.stop();
        System.out.println("STOPPED SUPER FEED MANAGER!");
    }

    @Override
    public String toString() {
        return feedConnectionId + "[" + nodeId + "(" + host + ")" + ":" + port + "]"
                + (isLocal ? started ? "Started " : "Not Started" : " Remote ");
    }

    public static class SuperFeedManagerMessages {

        private static final String EOL = "\n";

        public enum MessageType {
            FEED_PORT_REQUEST,
            FEED_PORT_RESPONSE
        }

        public static final byte[] SEND_PORT_REQUEST = (MessageType.FEED_PORT_REQUEST.name() + EOL).getBytes();
    }

    private static class SuperFeedManagerService implements Runnable {

        private int nextPort;
        private ServerSocket server;
        private String EOM = "\n";

        private final LinkedBlockingQueue<String> inbox;
        private List<MessageListener> messageListeners;
        private SFMessageAnalyzer mesgAnalyzer;
        private final FeedConnectionId feedId;
        private boolean process = true;

        public SuperFeedManagerService(int port, LinkedBlockingQueue<String> inbox, FeedConnectionId feedId)
                throws IOException {
            FeedRuntimeManager runtimeManager = FeedManager.INSTANCE.getFeedRuntimeManager(feedId);
            server = runtimeManager.createServerSocket(port);
            nextPort = port;
            this.inbox = inbox;
            this.feedId = feedId;
            this.messageListeners = new ArrayList<MessageListener>();
            mesgAnalyzer = new SFMessageAnalyzer(inbox);
            FeedManager.INSTANCE.getFeedExecutorService(feedId).execute(mesgAnalyzer);
        }

        public void stop() {
            process = false;
            if (server != null) {
                try {
                    server.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            mesgAnalyzer.stop();
        }

        @Override
        public void run() {
            Socket client = null;
            while (true) {
                try {
                    client = server.accept();
                    OutputStream os = client.getOutputStream();
                    nextPort++;
                    MessageListener listener = new MessageListener(nextPort, inbox);
                    listener.start();
                    messageListeners.add(listener);
                    os.write((nextPort + EOM).getBytes());
                    os.flush();
                } catch (IOException e) {
                    if (process == false) {
                        break;
                    }
                    e.printStackTrace();
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

    }

    private static class SFMessageAnalyzer implements Runnable {

        private final LinkedBlockingQueue<String> inbox;
        private final Socket socket;
        private final OutputStream os;
        private boolean process = true;

        public SFMessageAnalyzer(LinkedBlockingQueue<String> inbox) throws UnknownHostException, IOException {
            this.inbox = inbox;
            String ccHost = AsterixClusterProperties.INSTANCE.getCluster().getMasterNode().getClusterIp();
            socket = null; //new Socket(ccHost, 2999);
            os = null; //socket.getOutputStream();
        }

        public void stop() {
            process = false;
        }

        public void run() {
            while (process) {
                try {
                    String message = inbox.take();
                    FeedReport report = new FeedReport(message);
                    FeedReportMessageType mesgType = report.getReportType();
                    switch (mesgType) {
                        case THROUGHPUT:
                            //send message to FeedHealthDataReceiver at CC (2999)
                            //          os.write(message.getBytes());
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.warning("SuperFeedManager received message " + message);
                            }
                            break;
                        case CONGESTION:
                            // congestionInbox.add(report);
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
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
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
                                        LOGGER.warning(" Need elasticity at " + sourceOfCongestion + " as per report "
                                                + lastMaxReport);
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

    public boolean isStarted() {
        return started;
    }

}