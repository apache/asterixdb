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
package edu.uci.ics.asterix.tools.external.data;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.tools.external.data.DataGenerator.InitializationInfo;
import edu.uci.ics.asterix.tools.external.data.DataGenerator.TweetMessage;
import edu.uci.ics.asterix.tools.external.data.DataGenerator.TweetMessageIterator;

public class TweetGenerator {

    private static Logger LOGGER = Logger.getLogger(TweetGenerator.class.getName());

    public static final String KEY_DURATION = "duration";
    public static final String KEY_TPS = "tps";
    public static final String KEY_VERBOSE = "verbose";
    public static final String KEY_FIELDS = "fields";
    public static final int INFINITY = 0;

    private static final int DEFAULT_DURATION = INFINITY;

    private int duration;
    private TweetMessageIterator tweetIterator = null;
    private int partition;
    private long tweetCount = 0;
    private int frameTweetCount = 0;
    private int numFlushedTweets = 0;
    private DataGenerator dataGenerator = null;
    private ByteBuffer outputBuffer = ByteBuffer.allocate(32 * 1024);
    private String[] fields;
    private final List<OutputStream> subscribers;
    private final Object lock = new Object();
    private final List<OutputStream> subscribersForRemoval = new ArrayList<OutputStream>();

    public TweetGenerator(Map<String, String> configuration, int partition) throws Exception {
        this.partition = partition;
        String value = configuration.get(KEY_DURATION);
        this.duration = value != null ? Integer.parseInt(value) : DEFAULT_DURATION;
        dataGenerator = new DataGenerator(new InitializationInfo());
        tweetIterator = dataGenerator.new TweetMessageIterator(duration);
        this.fields = configuration.get(KEY_FIELDS) != null ? configuration.get(KEY_FIELDS).split(",") : null;
        this.subscribers = new ArrayList<OutputStream>();
    }

    private void writeTweetString(TweetMessage tweetMessage) throws IOException {
        String tweet = tweetMessage.getAdmEquivalent(fields) + "\n";
        System.out.println(tweet);
        tweetCount++;
        byte[] b = tweet.getBytes();
        if (outputBuffer.position() + b.length > outputBuffer.limit()) {
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

    public boolean generateNextBatch(int numTweets) throws Exception {
        boolean moreData = tweetIterator.hasNext();
        if (!moreData) {
            if (outputBuffer.position() > 0) {
                flush();
            }
            if (LOGGER.isLoggable(Level.INFO)) {
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