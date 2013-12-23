/*
x * Copyright 2009-2013 by The Regents of the University of California
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
    public static final String KEY_GUID_SEED = "guid-seed";

    public static final String OUTPUT_FORMAT = "output-format";
    public static final String OUTPUT_FORMAT_ARECORD = "arecord";
    public static final String OUTPUT_FORMAT_ADM_STRING = "adm-string";

    private static final int DEFAULT_DURATION = 60; //seconds
    private static final int DEFAULT_GUID_SEED = 0;

    private int duration;
    private TweetMessageIterator tweetIterator = null;
    private int partition;
    private int tweetCount = 0;
    private int frameTweetCount = 0;
    private int numFlushedTweets = 0;
    private OutputStream os;
    private DataGenerator dataGenerator = null;
    private ByteBuffer outputBuffer = ByteBuffer.allocate(32 * 1024);
    private GULongIDGenerator uidGenerator;

    public int getTweetCount() {
        return tweetCount;
    }

    public TweetGenerator(Map<String, String> configuration, int partition, String format, OutputStream os)
            throws Exception {
        this.partition = partition;
        String value = configuration.get(KEY_DURATION);
        this.duration = value != null ? Integer.parseInt(value) : DEFAULT_DURATION;
        int guidSeed = configuration.get(KEY_GUID_SEED) != null ? Integer.parseInt(configuration.get(KEY_GUID_SEED))
                : DEFAULT_GUID_SEED;
        uidGenerator = new GULongIDGenerator(partition, (byte) (guidSeed));
        dataGenerator = new DataGenerator(new InitializationInfo());
        tweetIterator = dataGenerator.new TweetMessageIterator(duration, uidGenerator);
        this.os = os;
    }

    private void writeTweetString(TweetMessage tweetMessage) throws IOException {
        String tweet = tweetMessage.toString() + "\n";
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

    public int getNumFlushedTweets() {
        return numFlushedTweets;
    }

    private void flush() throws IOException {
        outputBuffer.flip();
        os.write(outputBuffer.array(), 0, outputBuffer.limit());
        outputBuffer.position(0);
        outputBuffer.limit(32 * 1024);
    }

    public boolean setNextRecordBatch(int numTweetsInBatch) throws Exception {
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
            while (count < numTweetsInBatch) {
                writeTweetString(tweetIterator.next());
                count++;
            }
            return true;
        }
    }
}