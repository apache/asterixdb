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
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.tools.external.data.TweetGeneratorForSpatialIndexEvaluation;
import org.apache.commons.lang3.tuple.Pair;

public class SocketTweetGenerator {

    private final ExecutorService threadPool;

    private final int partitionRangeStart;

    private final int dataGenDuration;

    private final int queryGenDuration;

    private final long startDataInterval;

    private final int nDataIntervals;

    private final String orchHost;

    private final int orchPort;

    private final List<Pair<String, Integer>> receiverAddresses;

    private final String openStreetMapFilePath;
    private final int locationSampleInterval;
    private final int recordCountPerBatchDuringIngestionOnly;
    private final int recordCountPerBatchDuringQuery;
    private final long dataGenSleepTimeDuringIngestionOnly;
    private final long dataGenSleepTimeDuringQuery;

    private final Mode mode;

    private enum Mode {
        TIME,
        DATA
    }

    public SocketTweetGenerator(SocketTweetGeneratorConfig config) {
        threadPool = Executors.newCachedThreadPool(new ThreadFactory() {

            private final AtomicInteger count = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                int tid = count.getAndIncrement();
                Thread t = new Thread(r, "DataGeneratorThread: " + tid);
                t.setDaemon(true);
                return t;
            }
        });

        partitionRangeStart = config.getPartitionRangeStart();
        dataGenDuration = config.getDataGenDuration();
        queryGenDuration = config.getQueryGenDuration();
        startDataInterval = config.getDataInterval();
        nDataIntervals = config.getNIntervals();
        orchHost = config.getOrchestratorHost();
        orchPort = config.getOrchestratorPort();
        receiverAddresses = config.getAddresses();
        mode = startDataInterval > 0 ? Mode.DATA : Mode.TIME;
        openStreetMapFilePath = config.getOpenStreetMapFilePath();
        locationSampleInterval = config.getLocationSampleInterval();
        recordCountPerBatchDuringIngestionOnly = config.getRecordCountPerBatchDuringIngestionOnly();
        recordCountPerBatchDuringQuery = config.getRecordCountPerBatchDuringQuery();
        dataGenSleepTimeDuringIngestionOnly = config.getDataGenSleepTimeDuringIngestionOnly();
        dataGenSleepTimeDuringQuery = config.getDataGenSleepTimeDuringQuery();
    }

    public void start() throws Exception {
        final Semaphore sem = new Semaphore((receiverAddresses.size() - 1) * -1);
        int i = 0;
        for (Pair<String, Integer> address : receiverAddresses) {
            threadPool.submit(new DataGenerator(mode, sem, address.getLeft(), address.getRight(), i
                    + partitionRangeStart, dataGenDuration, queryGenDuration, nDataIntervals, startDataInterval,
                    orchHost, orchPort, openStreetMapFilePath, locationSampleInterval,
                    recordCountPerBatchDuringIngestionOnly, recordCountPerBatchDuringQuery,
                    dataGenSleepTimeDuringIngestionOnly, dataGenSleepTimeDuringQuery));
            ++i;
        }
        sem.acquire();
    }

    public static class DataGenerator implements Runnable {

        private static final Logger LOGGER = Logger.getLogger(DataGenerator.class.getName());

        private final Mode m;
        private final Semaphore sem;
        private final String host;
        private final int port;
        private final int partition;
        private final int dataGenDuration;
        private final int queryGenDuration;
        private final int nDataIntervals;
        private final String orchHost;
        private final int orchPort;

        private int currentInterval;
        private long nextStopInterval;
        private final long dataSizeInterval;
        private final boolean flagStopResume;
        private final String openStreetMapFilePath;
        private final int locationSampleInterval;
        private final int recordCountPerBatchDuringIngestionOnly;
        private final int recordCountPerBatchDuringQuery;
        private final long dataGenSleepTimeDuringIngestionOnly;
        private final long dataGenSleepTimeDuringQuery;

        public DataGenerator(Mode m, Semaphore sem, String host, int port, int partition, int dataGenDuration,
                int queryGenDuration, int nDataIntervals, long dataSizeInterval, String orchHost, int orchPort,
                String openStreetMapFilePath, int locationSampleInterval, int recordCountPerBatchDuringIngestionOnly,
                int recordCountPerBatchDuringQuery, long dataGenSleepTimeDuringIngestionOnly,
                long dataGenSleepTimeDuringQuery) {
            this.m = m;
            this.sem = sem;
            this.host = host;
            this.port = port;
            this.partition = partition;
            this.dataGenDuration = dataGenDuration;
            this.queryGenDuration = queryGenDuration;
            this.nDataIntervals = nDataIntervals;
            currentInterval = 0;
            this.dataSizeInterval = dataSizeInterval;
            this.nextStopInterval = dataSizeInterval;
            this.orchHost = orchHost;
            this.orchPort = orchPort;
            this.flagStopResume = false;
            this.openStreetMapFilePath = openStreetMapFilePath;
            //simple heuristic to generate different data from different data generator.
            int lsi = locationSampleInterval + (partition + 1) * (partition <= 4 ? 7 : 9);
            this.locationSampleInterval = lsi;
            this.recordCountPerBatchDuringIngestionOnly = recordCountPerBatchDuringIngestionOnly;
            this.recordCountPerBatchDuringQuery = recordCountPerBatchDuringQuery;
            this.dataGenSleepTimeDuringIngestionOnly = dataGenSleepTimeDuringIngestionOnly;
            this.dataGenSleepTimeDuringQuery = dataGenSleepTimeDuringQuery;
        }

        @Override
        public void run() {
            LOGGER.info("\nDataGen[" + partition + "] running with the following parameters: \n" + "dataGenDuration : "
                    + dataGenDuration + "\n" + "queryGenDuration : " + queryGenDuration + "\n" + "nDataIntervals : "
                    + nDataIntervals + "\n" + "dataSizeInterval : " + dataSizeInterval + "\n"
                    + "recordCountPerBatchDuringIngestionOnly : " + recordCountPerBatchDuringIngestionOnly + "\n"
                    + "recordCountPerBatchDuringQuery : " + recordCountPerBatchDuringQuery + "\n"
                    + "dataGenSleepTimeDuringIngestionOnly : " + dataGenSleepTimeDuringIngestionOnly + "\n"
                    + "dataGenSleepTimeDuringQuery : " + dataGenSleepTimeDuringQuery + "\n"
                    + "locationSampleInterval : " + locationSampleInterval);

            try {
                Socket s = new Socket(host, port);
                try {
                    Socket orchSocket = null;
                    if (m == Mode.DATA && orchHost != null) {
                        orchSocket = new Socket(orchHost, orchPort);
                    }
                    TweetGeneratorForSpatialIndexEvaluation tg = null;
                    try {
                        Map<String, String> config = new HashMap<>();
                        String durationVal = m == Mode.TIME ? String.valueOf(dataGenDuration) : "0";
                        config.put(TweetGeneratorForSpatialIndexEvaluation.KEY_DURATION, String.valueOf(durationVal));
                        if (openStreetMapFilePath != null) {
                            config.put(TweetGeneratorForSpatialIndexEvaluation.KEY_OPENSTREETMAP_FILEPATH,
                                    openStreetMapFilePath);
                            config.put(TweetGeneratorForSpatialIndexEvaluation.KEY_LOCATION_SAMPLE_INTERVAL,
                                    String.valueOf(locationSampleInterval));
                        }
                        tg = new TweetGeneratorForSpatialIndexEvaluation(config, partition,
                                TweetGeneratorForSpatialIndexEvaluation.OUTPUT_FORMAT_ADM_STRING, s.getOutputStream());
                        long startTS = System.currentTimeMillis();
                        long prevTS = startTS;
                        long curTS = startTS;
                        int round = 0;
                        while (tg.setNextRecordBatch(recordCountPerBatchDuringIngestionOnly)) {
                            if (m == Mode.DATA) {
                                if (tg.getNumFlushedTweets() >= nextStopInterval) {
                                    //TODO stop/resume option
                                    if (orchSocket != null) {
                                        if (flagStopResume) {
                                            // send stop to orchestrator
                                            sendStopped(orchSocket);
                                        } else {
                                            sendReached(orchSocket);
                                        }
                                    }

                                    // update intervals
                                    // TODO give options: exponential/linear interval
                                    nextStopInterval += dataSizeInterval;
                                    if (++currentInterval >= nDataIntervals) {
                                        break;
                                    }

                                    if (orchSocket != null) {
                                        if (flagStopResume) {
                                            receiveResume(orchSocket);
                                        }
                                    }
                                }
                            }
                            curTS = System.currentTimeMillis();
                            if (LOGGER.isLoggable(Level.INFO)) {
                                round++;
                                if ((round * recordCountPerBatchDuringIngestionOnly) % 100000 == 0) {
                                    System.out.println("DataGen[" + partition
                                            + "][During ingestion only][TimeToInsert100000] " + (curTS - prevTS)
                                            + " in milliseconds");
                                    round = 0;
                                    prevTS = curTS;
                                }
                            }
                            //to prevent congestion in feed pipe line. 
                            if (dataGenSleepTimeDuringIngestionOnly > 0) {
                                Thread.sleep(dataGenSleepTimeDuringIngestionOnly);
                            }
                        }

                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("DataGen[" + partition
                                    + "][During ingestion only][InsertCount] Num tweets flushed = "
                                    + tg.getNumFlushedTweets() + " in "
                                    + ((System.currentTimeMillis() - startTS) / 1000) + " seconds from "
                                    + InetAddress.getLocalHost() + " to " + host + ":" + port);
                        }

                        if (orchSocket != null && queryGenDuration > 0) {
                            //wait until orchestrator server's resume message is received.
                            receiveResume(orchSocket);

                            //reset duration and flushed tweet count
                            tg.resetDurationAndFlushedTweetCount(queryGenDuration);

                            prevTS = System.currentTimeMillis();
                            round = 0;
                            //start sending record
                            while (tg.setNextRecordBatch(recordCountPerBatchDuringQuery)) {
                                curTS = System.currentTimeMillis();
                                if (LOGGER.isLoggable(Level.INFO)) {
                                    round++;
                                    if ((round * recordCountPerBatchDuringQuery) % 100000 == 0) {
                                        System.out.println("DataGen[" + partition
                                                + "][During ingestion + queries][TimeToInsert100000] "
                                                + (curTS - prevTS) + " in milliseconds");
                                        round = 0;
                                        prevTS = curTS;
                                    }
                                }
                                if (dataGenSleepTimeDuringQuery > 0) {
                                    Thread.sleep(dataGenSleepTimeDuringQuery);
                                }
                            }
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("DataGen[" + partition
                                        + "][During ingestion + queries][InsertCount] Num tweets flushed = "
                                        + tg.getNumFlushedTweets() + " in " + queryGenDuration + " seconds from "
                                        + InetAddress.getLocalHost() + " to " + host + ":" + port);
                            }
                            //send reached message to orchestrator server
                            sendReached(orchSocket);
                        }

                    } finally {
                        if (orchSocket != null) {
                            orchSocket.close();
                        }
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Num tweets flushed = " + tg.getNumFlushedTweets() + " in " + dataGenDuration
                                    + " seconds from " + InetAddress.getLocalHost() + " to " + host + ":" + port);
                        }
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                } finally {
                    s.close();
                }
            } catch (Throwable t) {
                System.err.println("Error connecting to " + host + ":" + port);
                t.printStackTrace();
            } finally {
                sem.release();
            }
        }

        private void sendReached(Socket s) throws IOException {
            new DataOutputStream(s.getOutputStream()).writeInt(OrchestratorDGProtocol.REACHED.ordinal());
            s.getOutputStream().flush();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Sent " + OrchestratorDGProtocol.REACHED + " to " + s.getRemoteSocketAddress());
            }
        }

        private void receiveResume(Socket s) throws IOException {
            int msg = new DataInputStream(s.getInputStream()).readInt();
            OrchestratorDGProtocol msgType = OrchestratorDGProtocol.values()[msg];
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Received " + msgType + " from " + s.getRemoteSocketAddress());
            }
            if (msgType != OrchestratorDGProtocol.RESUME) {
                throw new IllegalStateException("Unknown protocol message received: " + msgType);
            }
        }

        private void sendStopped(Socket s) throws IOException {
            new DataOutputStream(s.getOutputStream()).writeInt(OrchestratorDGProtocol.STOPPED.ordinal());
            s.getOutputStream().flush();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Sent " + OrchestratorDGProtocol.STOPPED + " to " + s.getRemoteSocketAddress());
            }
        }

    }

    private static class CircularByteArrayOutputStream extends OutputStream {

        private final byte[] buf;

        private int index;

        public CircularByteArrayOutputStream() {
            buf = new byte[32 * 1024];
            index = 0;
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            if (b == null) {
                throw new NullPointerException();
            } else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return;
            }

            int remain = len;
            int remainOff = off;
            while (remain > 0) {
                int avail = buf.length - index;
                System.arraycopy(b, remainOff, buf, index, avail);
                remainOff += avail;
                remain -= avail;
                index = (index + avail) % buf.length;
            }
        }

        @Override
        public void write(int b) throws IOException {
            buf[index] = (byte) b;
            index = (index + 1) % buf.length;
        }

    }
}
