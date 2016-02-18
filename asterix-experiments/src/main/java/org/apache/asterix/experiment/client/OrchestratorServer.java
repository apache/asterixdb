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

package org.apache.asterix.experiment.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.experiment.action.base.IAction;

public class OrchestratorServer {

    private static final Logger LOGGER = Logger.getLogger(OrchestratorServer.class.getName());

    private final int port;

    private final int nDataGens;

    private final int nIntervals;

    private final AtomicBoolean running;

    private final IAction[] protocolActions;

    private final boolean flagStopResume;

    public OrchestratorServer(int port, int nDataGens, int nIntervals, IAction[] protocolActions) {
        this.port = port;
        this.nDataGens = nDataGens;
        this.nIntervals = nIntervals;
        running = new AtomicBoolean();
        this.protocolActions = protocolActions;
        this.flagStopResume = true;
    }
    
    public synchronized void start() throws IOException, InterruptedException {
        final AtomicBoolean bound = new AtomicBoolean();
        running.set(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    ServerSocket ss = new ServerSocket(port);
                    synchronized (bound) {
                        bound.set(true);
                        bound.notifyAll();
                    }
                    Socket[] conn = new Socket[nDataGens];
                    try {
                        for (int i = 0; i < nDataGens; i++) {
                            conn[i] = ss.accept();
                        }
                        for (int n = 0; n < nIntervals; ++n) {
                            //TODO refactor operations according to the protocol message
                            if (flagStopResume) {
                                for (int i = 0; i < nDataGens; i++) {
                                    receiveStopped(conn[i]);
                                }
                                protocolActions[n].perform();
                                if (n != nIntervals - 1) {
                                    for (int i = 0; i < nDataGens; i++) {
                                        sendResume(conn[i]);
                                    }
                                }
                            } else {
                                for (int i = 0; i < nDataGens; i++) {
                                    receiveReached(conn[i]);
                                }
                                protocolActions[n].perform();
                            }
                        }
                    } finally {
                        for (int i = 0; i < conn.length; ++i) {
                            if (conn[i] != null) {
                                conn[i].close();
                            }
                        }
                        ss.close();
                    }
                    running.set(false);
                    synchronized (OrchestratorServer.this) {
                        OrchestratorServer.this.notifyAll();
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }

        });
        t.start();
        synchronized (bound) {
            while (!bound.get()) {
                bound.wait();
            }
        }
    }

    private void sendResume(Socket conn) throws IOException {
        new DataOutputStream(conn.getOutputStream()).writeInt(OrchestratorDGProtocol.RESUME.ordinal());
        conn.getOutputStream().flush();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Sent " + OrchestratorDGProtocol.RESUME + " to " + conn.getRemoteSocketAddress());
        }
    }

    private void receiveStopped(Socket conn) throws IOException {
        int msg = new DataInputStream(conn.getInputStream()).readInt();
        OrchestratorDGProtocol msgType = OrchestratorDGProtocol.values()[msg];
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Received " + msgType + " from " + conn.getRemoteSocketAddress());
        }
        if (msgType != OrchestratorDGProtocol.STOPPED) {
            throw new IllegalStateException("Encounted unknown message type " + msgType);
        }
    }

    private void receiveReached(Socket conn) throws IOException {
        int msg = new DataInputStream(conn.getInputStream()).readInt();
        OrchestratorDGProtocol msgType = OrchestratorDGProtocol.values()[msg];
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Received " + msgType + " from " + conn.getRemoteSocketAddress());
        }
        if (msgType != OrchestratorDGProtocol.REACHED) {
            throw new IllegalStateException("Encounted unknown message type " + msgType);
        }

    }

    public synchronized void awaitFinished() throws InterruptedException {
        while (running.get()) {
            wait();
        }
    }

}
