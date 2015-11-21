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
package org.apache.asterix.metadata.feeds;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.CharBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RemoteSocketMessageListener {

    private static final Logger LOGGER = Logger.getLogger(RemoteSocketMessageListener.class.getName());

    private final String host;
    private final int port;
    private final LinkedBlockingQueue<String> outbox;
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    private RemoteMessageListenerServer listenerServer;

    public RemoteSocketMessageListener(String host, int port, LinkedBlockingQueue<String> outbox) {
        this.host = host;
        this.port = port;
        this.outbox = outbox;
    }

    public void stop() {
        if (!executorService.isShutdown()) {
            executorService.shutdownNow();
        }
        listenerServer.stop();

    }

    public void start() throws IOException {
        listenerServer = new RemoteMessageListenerServer(host, port, outbox);
        executorService.execute(listenerServer);
    }

    private static class RemoteMessageListenerServer implements Runnable {

        private final String host;
        private final int port;
        private final LinkedBlockingQueue<String> outbox;
        private Socket client;

        public RemoteMessageListenerServer(String host, int port, LinkedBlockingQueue<String> outbox) {
            this.host = host;
            this.port = port;
            this.outbox = outbox;
        }

        public void stop() {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            char EOL = (char) "\n".getBytes()[0];
            Socket client = null;
            try {
                client = new Socket(host, port);
                InputStream in = client.getInputStream();
                CharBuffer buffer = CharBuffer.allocate(5000);
                char ch;
                while (true) {
                    ch = (char) in.read();
                    if ((ch) == -1) {
                        break;
                    }
                    while (ch != EOL) {
                        buffer.put(ch);
                        ch = (char) in.read();
                    }
                    buffer.flip();
                    String s = new String(buffer.array());
                    synchronized (outbox) {
                        outbox.add(s + "\n");
                    }
                    buffer.position(0);
                    buffer.limit(5000);
                }

            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Unable to start Remote Message listener" + client);
                }
            } finally {
                if (client != null && !client.isClosed()) {
                    try {
                        client.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static interface IMessageAnalyzer {

        /**
         * @return
         */
        public LinkedBlockingQueue<String> getMessageQueue();

    }

}
