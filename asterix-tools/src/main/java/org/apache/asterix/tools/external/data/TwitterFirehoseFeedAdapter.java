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
package org.apache.asterix.tools.external.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.feeds.api.IFeedAdapter;
import org.apache.asterix.external.dataset.adapter.StreamBasedAdapter;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * A simulator of the Twitter Firehose. Generates meaningful tweets
 * at a configurable rate
 */
public class TwitterFirehoseFeedAdapter extends StreamBasedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(TwitterFirehoseFeedAdapter.class.getName());

    private ExecutorService executorService = Executors.newCachedThreadPool();

    private PipedOutputStream outputStream = new PipedOutputStream();

    private PipedInputStream inputStream = new PipedInputStream(outputStream);

    private final TwitterServer twitterServer;

    public TwitterFirehoseFeedAdapter(Map<String, String> configuration, ITupleParserFactory parserFactory,
            ARecordType outputtype, IHyracksTaskContext ctx, int partition) throws Exception {
        super(parserFactory, outputtype, ctx, partition);
        this.twitterServer = new TwitterServer(configuration, partition, outputtype, outputStream, executorService);
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        twitterServer.start();
        super.start(partition, writer);
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        return inputStream;
    }

    private static class TwitterServer {
        private final DataProvider dataProvider;
        private final ExecutorService executorService;

        public TwitterServer(Map<String, String> configuration, int partition, ARecordType outputtype, OutputStream os,
                ExecutorService executorService) throws Exception {
            dataProvider = new DataProvider(configuration, outputtype, partition, os);
            this.executorService = executorService;
        }

        public void stop() throws IOException {
            dataProvider.stop();
        }

        public void start() {
            executorService.execute(dataProvider);
        }

    }

    private static class DataProvider implements Runnable {

        public static final String KEY_MODE = "mode";

        private TweetGenerator tweetGenerator;
        private boolean continuePush = true;
        private int batchSize;
        private final Mode mode;
        private final OutputStream os;

        public static enum Mode {
            AGGRESSIVE,
            CONTROLLED
        }

        public DataProvider(Map<String, String> configuration, ARecordType outputtype, int partition, OutputStream os)
                throws Exception {
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
                                if (endBatch - startBatch < 1000) {
                                    Thread.sleep(1000 - (endBatch - startBatch));
                                }
                                break;
                        }
                    }
                    os.close();
                    break;
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Exception in adaptor " + e.getMessage());
                    }
                }
            }
        }

        public void stop() {
            continuePush = false;
        }

    }

    @Override
    public void stop() throws Exception {
        twitterServer.stop();
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PUSH;
    }

    @Override
    public boolean handleException(Exception e) {
        try {
            twitterServer.stop();
        } catch (Exception re) {
            re.printStackTrace();
            return false;
        }
        twitterServer.start();
        return true;
    }

}