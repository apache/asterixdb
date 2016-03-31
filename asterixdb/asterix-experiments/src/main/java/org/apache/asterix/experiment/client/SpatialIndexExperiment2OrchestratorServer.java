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

public class SpatialIndexExperiment2OrchestratorServer {

    private static final Logger LOGGER = Logger.getLogger(SpatialIndexExperiment2OrchestratorServer.class.getName());

    private final int dataGenPort;

    private final int queryGenPort;

    private final int nDataGens;

    private final int nQueryGens;

    private final int nIntervals;

    private final AtomicBoolean running;

    public SpatialIndexExperiment2OrchestratorServer(int dataGenPort, int nDataGens, int nIntervals, int queryGenPort,
            int nQueryGens) {
        this.dataGenPort = dataGenPort;
        this.nDataGens = nDataGens;
        this.queryGenPort = queryGenPort;
        this.nQueryGens = nQueryGens;
        this.nIntervals = nIntervals;
        running = new AtomicBoolean();
    }

    public synchronized void start() throws IOException, InterruptedException {
        final AtomicBoolean dataGenBound = new AtomicBoolean();
        final AtomicBoolean queryGenBound = new AtomicBoolean();
        running.set(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    ServerSocket dataGenSS = new ServerSocket(dataGenPort);
                    synchronized (dataGenBound) {
                        dataGenBound.set(true);
                        dataGenBound.notifyAll();
                    }
                    ServerSocket queryGenSS = new ServerSocket(queryGenPort);
                    synchronized (queryGenBound) {
                        queryGenBound.set(true);
                        queryGenBound.notifyAll();
                    }

                    Socket[] dataConn = new Socket[nDataGens];
                    Socket[] queryConn = new Socket[nQueryGens];
                    try {
                        //#.wait until all dataGens and queryGens have connected to the orchestrator
                        for (int i = 0; i < nDataGens; i++) {
                            dataConn[i] = dataGenSS.accept();
                        }
                        for (int i = 0; i < nQueryGens; i++) {
                            queryConn[i] = queryGenSS.accept();
                        }

                        //#.wait until queryGens are ready for generating query
                        for (int i = 0; i < nQueryGens; i++) {
                            receiveReached(queryConn[i]);
                        }

                        //#.wait until dataGens are ready for generating data after nIntervals of data were generated 
                        for (int i = 0; i < nIntervals; i++) {
                            for (int j = 0; j < nDataGens; j++) {
                                receiveReached(dataConn[j]);
                            }
                        }

                        //#.send signal to queryGens to start sending queries
                        for (int i = 0; i < nQueryGens; i++) {
                            sendResume(queryConn[i]);
                        }
                        //#.send signal to dataGens to start sending records
                        for (int i = 0; i < nDataGens; i++) {
                            sendResume(dataConn[i]);
                        }

                        //#.wait until both dataGen and queryGen's are done
                        for (int i = 0; i < nQueryGens; i++) {
                            receiveReached(queryConn[i]);
                        }
                        for (int i = 0; i < nDataGens; i++) {
                            receiveReached(dataConn[i]);
                        }

                    } finally {
                        for (int i = 0; i < nDataGens; ++i) {
                            if (dataConn[i] != null) {
                                dataConn[i].close();
                            }
                        }
                        dataGenSS.close();
                        for (int i = 0; i < nQueryGens; ++i) {
                            if (queryConn[i] != null) {
                                queryConn[i].close();
                            }
                        }
                        queryGenSS.close();
                    }
                    running.set(false);
                    synchronized (SpatialIndexExperiment2OrchestratorServer.this) {
                        SpatialIndexExperiment2OrchestratorServer.this.notifyAll();
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }

        });
        t.start();
        synchronized (dataGenBound) {
            while (!dataGenBound.get()) {
                dataGenBound.wait();
            }
        }
        synchronized (queryGenBound) {
            while (!queryGenBound.get()) {
                queryGenBound.wait();
            }    
        }
    }

    private static void receiveReached(Socket conn) throws IOException {
        int msg = new DataInputStream(conn.getInputStream()).readInt();
        OrchestratorDGProtocol msgType = OrchestratorDGProtocol.values()[msg];
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Received " + msgType + " from " + conn.getRemoteSocketAddress());
        }
        if (msgType != OrchestratorDGProtocol.REACHED) {
            throw new IllegalStateException("Encounted unknown message type " + msgType);
        }
    }

    private void sendResume(Socket s) throws IOException {
        new DataOutputStream(s.getOutputStream()).writeInt(OrchestratorDGProtocol.RESUME.ordinal());
        s.getOutputStream().flush();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Sent " + OrchestratorDGProtocol.RESUME + " to " + s.getRemoteSocketAddress());
        }
    }

    public synchronized void awaitFinished() throws InterruptedException {
        while (running.get()) {
            wait();
        }
    }

}
