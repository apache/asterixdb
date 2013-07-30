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
import java.io.Serializable;
import java.net.Socket;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.metadata.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.metadata.feeds.MessageListener.IMessageAnalyzer;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.asterix.om.util.AsterixRuntimeUtil;

public class SuperFeedManager implements Serializable {

    private static final Logger LOGGER = Logger.getLogger(SuperFeedManager.class.getName());

    private static final long serialVersionUID = 1L;
    private String host;

    private final int port;

    private final String nodeId;

    private final FeedConnectionId feedConnectionId;

    private MessageListener listener;

    private boolean isLocal = false;

    private transient ExecutorService executorService;

    public enum FeedReportMessageType {
        CONGESTION,
        THROUGHPUT
    }

    public SuperFeedManager(FeedConnectionId feedId, String nodeId, int port) throws Exception {
        this.feedConnectionId = feedId;
        this.nodeId = nodeId;
        this.port = port;
        initialize();
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

    private void initialize() throws Exception {
        Map<String, Set<String>> ncs = AsterixRuntimeUtil.getNodeControllerMap();
        for (Entry<String, Set<String>> entry : ncs.entrySet()) {
            String ip = entry.getKey();
            Set<String> nc = entry.getValue();
            if (nc.contains(nodeId)) {
                host = ip;
                break;
            }
        }
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
        if (listener == null) {
            if (executorService == null) {
                executorService = Executors.newCachedThreadPool();
            }
            listener = new MessageListener(port, new SuperFeedManagerMessageAnalzer(executorService));
            listener.start();
        }
    }

    public void stop() throws IOException {
        if (listener != null) {
            listener.stop();
        }
        executorService.shutdownNow();
    }

    @Override
    public String toString() {
        return feedConnectionId + "[" + nodeId + "(" + host + ")" + ":" + port + "]";
    }

    private static class SuperFeedManagerMessageAnalzer implements IMessageAnalyzer {

        private String ccHost;
        private CongestionAnalyzer congestionAnalyzer;
        private LinkedBlockingQueue<FeedReport> congestionInbox = new LinkedBlockingQueue<FeedReport>();

        public SuperFeedManagerMessageAnalzer(ExecutorService executorService) {
            ccHost = AsterixClusterProperties.INSTANCE.getCluster().getMasterNode().getClusterIp();
            congestionAnalyzer = new CongestionAnalyzer(congestionInbox);
            executorService.execute(congestionAnalyzer);
        }

        @Override
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

}