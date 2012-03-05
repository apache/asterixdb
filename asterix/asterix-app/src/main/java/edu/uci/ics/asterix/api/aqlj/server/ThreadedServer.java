/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.api.aqlj.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Logger;

/**
 * This server is a multithreaded server that spawns one connection per client
 * up to MAX_CLIENTS total clients. The type of thread spawned to handle each
 * client request is delegated to a client thread factory that implements the
 * IClientThreadFactory interface.
 * NOTE: The "BE" in logging messages stands for "back-end". This is to
 * differentiate from the "FE" or "front-end" when reviewing log messages.
 * 
 * @author zheilbron
 */
public class ThreadedServer extends Thread {
    private static Logger LOGGER = Logger.getLogger(ThreadedServer.class.getName());

    private static final int MAX_CLIENTS = 10;

    private final int port;
    private final IClientThreadFactory factory;

    private ServerSocket serverSocket;
    private Socket clientSocket;
    private Socket[] clientSockets;
    private Thread[] threads;

    public ThreadedServer(int port, IClientThreadFactory factory) {
        this.port = port;
        this.factory = factory;
        this.clientSockets = new Socket[MAX_CLIENTS];
        this.threads = new Thread[MAX_CLIENTS];
        this.clientSocket = null;
    }

    public void run() {
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            LOGGER.severe("Error listening on port: " + port);
            LOGGER.severe(e.getMessage());
            return;
        }
        LOGGER.info("Server started. Listening on port: " + port);

        while (true) {
            try {
                clientSocket = serverSocket.accept();
            } catch (SocketException e) {
                // This is the normal path the server will take when exiting.
                //
                // In order to close the server down properly, the
                // serverSocket.accept() call must
                // be interrupted. The only way to interrupt the
                // serverSocket.accept() call in the loop
                // above is by calling serverSocket.close() (as is done in the
                // ThreadedServer.shutdown() method
                // below). The serverSocket.accept() then throws a
                // SocketException, so we catch it here
                // and assume that ThreadedServer.shutdown() was called.

                return;
            } catch (IOException e) {
                LOGGER.severe("Failed to accept() connection");
                LOGGER.severe(e.getMessage());
            }

            for (int i = 0; i < threads.length; i++) {
                if (threads[i] == null || !threads[i].isAlive()) {
                    try {
                        threads[i] = factory.createThread(clientSocket);
                    } catch (IOException e) {
                        LOGGER.severe("Failed to create client handler thread");
                        LOGGER.severe(e.getMessage());
                    }
                    clientSockets[i] = clientSocket;
                    threads[i].start();
                    clientSocket = null;
                    break;
                }
            }

            // setting the clientSocket to null is an indicator the there was
            // room for the
            // connection (i.e. the number of clients < MAX_CLIENTS). If it is
            // not set, then
            // there was no room for the connection, so the client is dropped.
            if (clientSocket != null) {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    LOGGER.severe("Error closing (dropped) client socket.");
                    LOGGER.severe(e.getMessage());
                }
                LOGGER.warning("Client was dropped. Maximum number of connections reached!");
            }
        }
    }

    public void shutdown() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.severe("Error closing server socket.");
            LOGGER.severe(e.getMessage());
        }

        try {
            for (int i = 0; i < threads.length; i++) {
                if (threads[i] != null && threads[i].isAlive()) {
                    clientSockets[i].close();
                    threads[i].interrupt();
                }
            }
        } catch (IOException e) {
            LOGGER.severe("Error closing client socket.");
            LOGGER.severe(e.getMessage());
        }
    }
}
