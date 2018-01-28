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
package org.apache.asterix.external.input.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.generator.TweetGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TwitterFirehoseInputStream extends AsterixInputStream {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ExecutorService executorService;
    private final PipedOutputStream outputStream;
    private final PipedInputStream inputStream;
    private final DataProvider dataProvider;
    private boolean started;

    public TwitterFirehoseInputStream(Map<String, String> configuration, int partition) throws IOException {
        executorService = Executors.newCachedThreadPool();
        outputStream = new PipedOutputStream();
        inputStream = new PipedInputStream(outputStream);
        dataProvider = new DataProvider(configuration, partition, outputStream);
        started = false;
    }

    @Override
    public boolean stop() throws IOException {
        dataProvider.stop();
        return true;
    }

    public synchronized void start() {
        if (!started) {
            executorService.execute(dataProvider);
            started = true;
        }
    }

    @Override
    public int read() throws IOException {
        if (!started) {
            start();
        }
        return inputStream.read();
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (!started) {
            start();
        }
        return inputStream.read(b, off, len);
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }

    private static class DataProvider implements Runnable {

        public static final String KEY_MODE = "mode";

        private final TweetGenerator tweetGenerator;
        private boolean continuePush = true;
        private int batchSize;
        private final Mode mode;
        private final OutputStream os;

        public enum Mode {
            AGGRESSIVE,
            CONTROLLED
        }

        public DataProvider(Map<String, String> configuration, int partition, OutputStream os) {
            this.tweetGenerator = new TweetGenerator(configuration, partition);
            this.tweetGenerator.registerSubscriber(os);
            this.os = os;
            mode = configuration.get(KEY_MODE) != null ? Mode.valueOf(configuration.get(KEY_MODE).toUpperCase())
                    : Mode.AGGRESSIVE;
            switch (mode) {
                case CONTROLLED:
                    String tpsValue = configuration.get(TweetGenerator.KEY_TPS);
                    if (tpsValue == null) {
                        throw new IllegalArgumentException("TPS value not configured. use tps=<value>");
                    }
                    batchSize = Integer.parseInt(tpsValue);
                    break;
                case AGGRESSIVE:
                    batchSize = 5000;
                    break;
            }
        }

        @Override
        public void run() {
            boolean moreData = true;
            long startBatch;
            long endBatch;
            while (true) {
                try {
                    while (moreData && continuePush) {
                        switch (mode) {
                            case AGGRESSIVE:
                                moreData = tweetGenerator.generateNextBatch(batchSize);
                                break;
                            case CONTROLLED:
                                startBatch = System.currentTimeMillis();
                                moreData = tweetGenerator.generateNextBatch(batchSize);
                                endBatch = System.currentTimeMillis();
                                if ((endBatch - startBatch) < 1000) {
                                    Thread.sleep(1000 - (endBatch - startBatch));
                                }
                                break;
                        }
                    }
                    os.close();
                    break;
                } catch (Exception e) {
                    LOGGER.warn("Exception in adapter " + e.getMessage());
                }
            }
        }

        public void stop() {
            continuePush = false;
        }
    }
}
