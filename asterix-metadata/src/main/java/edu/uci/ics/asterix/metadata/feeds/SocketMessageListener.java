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
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.CharBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.api.IMessageReceiver;

/**
 * Listens for messages at a configured port and redirects them to a
 * an instance of {@code IMessageReceiver}.
 * Messages may arrive in parallel from multiple senders. Each sender is handled by
 * a respective instance of {@code ClientHandler}.
 */
public class SocketMessageListener {

    private static final Logger LOGGER = Logger.getLogger(SocketMessageListener.class.getName());

    private final int port;
    private final IMessageReceiver<String> messageReceiver;
    private final MessageListenerServer listenerServer;

    private ExecutorService executorService = Executors.newFixedThreadPool(10);

    public SocketMessageListener(int port, IMessageReceiver<String> messageReceiver) {
        this.port = port;
        this.messageReceiver = messageReceiver;
        this.listenerServer = new MessageListenerServer(port, messageReceiver);
    }

    public void stop() throws IOException {
        listenerServer.stop();
        messageReceiver.close(false);
        if (!executorService.isShutdown()) {
            executorService.shutdownNow();
        }
    }

    public void start() {
        messageReceiver.start();
        executorService.execute(listenerServer);
    }

    private static class MessageListenerServer implements Runnable {

        private final int port;
        private final IMessageReceiver<String> messageReceiver;
        private ServerSocket server;
        private final Executor executor;

        public MessageListenerServer(int port, IMessageReceiver<String> messageReceiver) {
            this.port = port;
            this.messageReceiver = messageReceiver;
            this.executor = Executors.newCachedThreadPool();
        }

        public void stop() throws IOException {
            server.close();
        }

        @Override
        public void run() {
            Socket client = null;
            try {
                server = new ServerSocket(port);
                while (true) {
                    client = server.accept();
                    ClientHandler handler = new ClientHandler(client, messageReceiver);
                    executor.execute(handler);
                }
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Unable to start Message listener" + server);
                }
            } finally {
                if (server != null) {
                    try {
                        server.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        private static class ClientHandler implements Runnable {

            private static final char EOL = (char) "\n".getBytes()[0];

            private final Socket client;
            private final IMessageReceiver<String> messageReceiver;

            public ClientHandler(Socket client, IMessageReceiver<String> messageReceiver) {
                this.client = client;
                this.messageReceiver = messageReceiver;
            }

            @Override
            public void run() {
                try {
                    InputStream in = client.getInputStream();
                    CharBuffer buffer = CharBuffer.allocate(5000);
                    char ch;
                    while (true) {
                        ch = (char) in.read();
                        if (((int) ch) == -1) {
                            break;
                        }
                        while (ch != EOL) {
                            buffer.put(ch);
                            ch = (char) in.read();
                        }
                        buffer.flip();
                        String s = new String(buffer.array(), 0, buffer.limit());
                        messageReceiver.sendMessage(s + "\n");
                        buffer.position(0);
                        buffer.limit(5000);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Unable to process mesages from client" + client);
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

    }
}