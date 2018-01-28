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
package org.apache.asterix.external.generator;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.generator.DataGenerator.TweetMessage;
import org.apache.asterix.external.generator.DataGenerator.TweetMessageIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TweetGenerator {
    private static final Logger LOGGER = LogManager.getLogger();

    public static final String KEY_DURATION = "duration";
    public static final String KEY_TPS = "tps";
    public static final String KEY_VERBOSE = "verbose";
    public static final String KEY_FIELDS = "fields";
    public static final int INFINITY = 0;

    private static final int DEFAULT_DURATION = INFINITY;

    private final int duration;
    private TweetMessageIterator tweetIterator = null;
    private final int partition;
    private long tweetCount = 0;
    private int frameTweetCount = 0;
    private int numFlushedTweets = 0;
    private DataGenerator dataGenerator = null;
    private final ByteBuffer outputBuffer = ByteBuffer.allocate(32 * 1024);
    private final String[] fields;
    private final List<OutputStream> subscribers;
    private final Object lock = new Object();
    private final List<OutputStream> subscribersForRemoval = new ArrayList<OutputStream>();

    public TweetGenerator(Map<String, String> configuration, int partition) {
        this.partition = partition;
        String value = configuration.get(KEY_DURATION);
        this.duration = value != null ? Integer.parseInt(value) : DEFAULT_DURATION;
        dataGenerator = new DataGenerator();
        tweetIterator = dataGenerator.new TweetMessageIterator(duration);
        this.fields = configuration.get(KEY_FIELDS) != null ? configuration.get(KEY_FIELDS).split(",") : null;
        this.subscribers = new ArrayList<OutputStream>();
    }

    private void writeTweetString(TweetMessage tweetMessage) throws IOException {
        String tweet = tweetMessage.getAdmEquivalent(fields) + "\n";
        tweetCount++;
        byte[] b = tweet.getBytes();
        if ((outputBuffer.position() + b.length) > outputBuffer.limit()) {
            flush();
            numFlushedTweets += frameTweetCount;
            frameTweetCount = 0;
            outputBuffer.put(b);
        } else {
            outputBuffer.put(b);
        }
        frameTweetCount++;
    }

    private void flush() throws IOException {
        outputBuffer.flip();
        synchronized (lock) {
            for (OutputStream os : subscribers) {
                try {
                    os.write(outputBuffer.array(), 0, outputBuffer.limit());
                } catch (Exception e) {
                    LOGGER.info("OutputStream failed. Add it into the removal list.");
                    subscribersForRemoval.add(os);
                }
            }
            if (!subscribersForRemoval.isEmpty()) {
                subscribers.removeAll(subscribersForRemoval);
                subscribersForRemoval.clear();
            }
        }
        outputBuffer.position(0);
        outputBuffer.limit(32 * 1024);
    }

    public boolean generateNextBatch(int numTweets) throws IOException {
        boolean moreData = tweetIterator.hasNext();
        if (!moreData) {
            if (outputBuffer.position() > 0) {
                flush();
            }
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Reached end of batch. Tweet Count: [" + partition + "]" + tweetCount);
            }
            return false;
        } else {
            int count = 0;
            while (count < numTweets) {
                writeTweetString(tweetIterator.next());
                count++;
            }
            return true;
        }
    }

    public int getNumFlushedTweets() {
        return numFlushedTweets;
    }

    public void registerSubscriber(OutputStream os) {
        synchronized (lock) {
            subscribers.add(os);
        }
    }

    public void deregisterSubscribers(OutputStream os) {
        synchronized (lock) {
            subscribers.remove(os);
        }
    }

    public void close() throws IOException {
        synchronized (lock) {
            for (OutputStream os : subscribers) {
                os.close();
            }
        }
    }

    public boolean isSubscribed() {
        return !subscribers.isEmpty();
    }

    public long getTweetCount() {
        return tweetCount;
    }

}
