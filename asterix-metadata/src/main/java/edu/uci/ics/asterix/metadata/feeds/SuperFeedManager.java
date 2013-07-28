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
import java.io.InputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.CharBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.om.util.AsterixRuntimeUtil;

public class SuperFeedManager implements Serializable {

    private static final Logger LOGGER = Logger.getLogger(SuperFeedManager.class.getName());

    private static final long serialVersionUID = 1L;
    private String host;

    private final int port;

    private final String nodeId;

    private final FeedConnectionId feedConnectionId;

    private SuperFeedManagerListener listener;

    private boolean isLocal = false;

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
            listener = new SuperFeedManagerListener(port);
            listener.start();
        }
    }

    public void stop() throws IOException {
        if (listener != null) {
            listener.stop();
        }
    }

    @Override
    public String toString() {
        return feedConnectionId + "[" + nodeId + "(" + host + ")" + ":" + port + "]";
    }

    private static class SuperFeedManagerListener implements Serializable {

        private static final long serialVersionUID = 1L;
        private ServerSocket server;
        private int port;
        private LinkedBlockingQueue<String> messages;
        private CongestionAnalyzer ca;

        private ExecutorService executorService = Executors.newFixedThreadPool(10);

        public SuperFeedManagerListener(int port) throws IOException {
            this.port = port;
            messages = new LinkedBlockingQueue<String>();
            ca = new CongestionAnalyzer(messages);
            executorService.execute(ca);
        }

        public void stop() {
            executorService.shutdown();
        }

        public void start() throws IOException {
            server = new ServerSocket(port);
            while (true) {
                Socket client = server.accept();
                executorService.execute(new MessageProcessor(client, this));
            }
        }

        public synchronized void notifyMessage(String s) {
            messages.add(s);
        }
    }

    private static class CongestionAnalyzer implements Runnable {

        private LinkedBlockingQueue<String> messages;

        public CongestionAnalyzer(LinkedBlockingQueue<String> messages) {
            this.messages = messages;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    try {
                        String message = messages.take();
                        String[] messageComponents = message.split("|");
                        SuperFeedManager.FeedReportMessageType mesgType = FeedReportMessageType
                                .valueOf(messageComponents[0]);
                        switch (mesgType) {
                            case THROUGHPUT:
                            case CONGESTION:
                        }
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning(message);
                        }
                    } catch (InterruptedException ie) {
                        throw ie;
                    } catch (Exception e) {

                    }

                }

            } catch (InterruptedException ie) {
                // do nothing
            }
        }
    }

    private static class MessageProcessor implements Runnable {

        private SuperFeedManagerListener listener;
        private Socket client;
        private static final char EOL = (char) "\n".getBytes()[0];

        public MessageProcessor(Socket client, SuperFeedManagerListener listener) {
            this.listener = listener;
            this.client = client;
        }

        @Override
        public void run() {
            CharBuffer buffer = CharBuffer.allocate(2000);
            char ch;
            try {
                InputStream in = client.getInputStream();
                ch = (char) in.read();
                while (ch != EOL) {
                    buffer.put(ch);
                    ch = (char) in.read();
                }
                buffer.flip();
                String s = new String(buffer.array());
                listener.notifyMessage(s);
            } catch (IOException ioe) {
            } finally {
                try {
                    client.close();
                } catch (IOException ioe) {
                    // do nothing
                }
            }
        }
    }
}
