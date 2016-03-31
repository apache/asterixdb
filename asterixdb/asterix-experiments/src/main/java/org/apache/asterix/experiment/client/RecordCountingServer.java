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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecordCountingServer {

    private final ExecutorService threadPool;

    private final int port;

    private final long duration;

    private ServerSocket ss;

    private boolean stopped;

    private final Object o = new Object();

    final AtomicBoolean b = new AtomicBoolean(false);

    public RecordCountingServer(int port, long duration) {
        this.port = port;
        this.duration = duration;
        threadPool = Executors.newCachedThreadPool();
    }

    public void start() throws IOException, InterruptedException {
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    stopped = false;
                    ss = new ServerSocket(port);
                    while (true) {
                        Socket s = ss.accept();
                        if (stopped) {
                            break;
                        }
                        threadPool.execute(new RecordCountingThread(s, duration));
                        synchronized (o) {
                            b.set(true);
                            o.notifyAll();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
    }

    public void awaitFirstConnection() throws InterruptedException {
        synchronized (o) {
            if (!b.get()) {
                o.wait();
            }
        }
    }

    public void stop() throws IOException, InterruptedException {
        stopped = true;
        threadPool.shutdown();
        threadPool.awaitTermination(1000, TimeUnit.DAYS);
        ss.close();
    }

    private static class RecordCountingThread implements Runnable {
        private final Socket s;

        private final long duration;

        private final char[] buf;

        private int index;

        private int count;

        public RecordCountingThread(Socket s, long duration) {
            this.s = s;
            this.duration = duration;
            buf = new char[32 * 1024];
        }

        @Override
        public void run() {
            count = 0;
            index = 0;
            long start = System.currentTimeMillis();
            try {
                InputStreamReader r = new InputStreamReader(s.getInputStream());
                while (System.currentTimeMillis() - start < duration) {
                    fill(r);
                    countRecords();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            long end = System.currentTimeMillis();
            System.out.println("Read " + count + " records in " + (end - start) / 1000 + " seconds");
        }

        private void countRecords() {
            for (int i = 0; i < index; ++i) {
                if (buf[i] == '\n') {
                    ++count;
                }
            }
        }

        private void fill(Reader r) throws IOException {
            index = 0;
            int read = r.read(buf);
            if (read == -1) {
                index = 0;
                return;
            }
            index += read;
        }
    }

    public static void main(String[] args) throws Exception {
        long duration = Long.parseLong(args[0]);
        int port1 = Integer.parseInt(args[1]);
        int port2 = Integer.parseInt(args[2]);
        RecordCountingServer rcs1 = new RecordCountingServer(port1, duration * 1000);
        RecordCountingServer rcs2 = new RecordCountingServer(port2, duration * 1000);
        try {
            rcs1.start();
            rcs2.start();
            rcs1.awaitFirstConnection();
            rcs2.awaitFirstConnection();
        } finally {
            rcs1.stop();
            rcs2.stop();
        }
    }
}
