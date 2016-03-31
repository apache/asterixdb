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
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.experiment.action.base.IAction;

public class OrchestratorServer9 {

    private static final Logger LOGGER = Logger.getLogger(OrchestratorServer9.class.getName());

    private final int port;

    private final int nDataGens;

    private final int nIntervals;

    private final AtomicBoolean running;

    private final IProtocolActionBuilder protoActionBuilder;

    public OrchestratorServer9(int port, int nDataGens, int nIntervals, IProtocolActionBuilder protoActionBuilder) {
        this.port = port;
        this.nDataGens = nDataGens;
        this.nIntervals = nIntervals;
        running = new AtomicBoolean();
        this.protoActionBuilder = protoActionBuilder;
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
                        AtomicInteger round = new AtomicInteger();
                        AtomicBoolean done = new AtomicBoolean(false);
                        Thread pct = new Thread(new ProtocolConsumer(conn, nIntervals, round, done));
                        pct.start();
                        while (!done.get()) {
                            protoActionBuilder.buildAction(round.get()).perform();
                        }
                        pct.join();
                    } finally {
                        for (int i = 0; i < conn.length; ++i) {
                            if (conn[i] != null) {
                                conn[i].close();
                            }
                        }
                        ss.close();
                    }
                    running.set(false);
                    synchronized (OrchestratorServer9.this) {
                        OrchestratorServer9.this.notifyAll();
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

    private static class ProtocolConsumer implements Runnable {

        private final Socket[] conn;

        private final int nIntervals;

        private final AtomicInteger interval;

        private final AtomicBoolean done;

        public ProtocolConsumer(Socket[] conn, int nIntervals, AtomicInteger interval, AtomicBoolean done) {
            this.conn = conn;
            this.nIntervals = nIntervals;
            this.interval = interval;
            this.done = done;
        }

        @Override
        public void run() {
            interval.set(0);
            try {
                for (int n = 0; n < nIntervals; ++n) {
                    for (int i = 0; i < conn.length; i++) {
                        receiveReached(conn[i]);
                    }
                    interval.getAndIncrement();
                }
                done.set(true);
            } catch (Exception e) {
                e.printStackTrace();
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

    public synchronized void awaitFinished() throws InterruptedException {
        while (running.get()) {
            wait();
        }
    }

    public interface IProtocolActionBuilder {
        public IAction buildAction(int round) throws Exception;
    }

}
