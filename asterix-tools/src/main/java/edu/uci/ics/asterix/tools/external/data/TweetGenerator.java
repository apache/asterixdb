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
import java.util.Map;

import edu.uci.ics.asterix.tools.external.data.DataGenerator.InitializationInfo;
import edu.uci.ics.asterix.tools.external.data.DataGenerator.TweetMessage;
import edu.uci.ics.asterix.tools.external.data.DataGenerator.TweetMessageIterator;

public class TweetGenerator {

    public static final String KEY_DURATION = "duration";
    public static final String KEY_TPS = "tps";
    public static final String KEY_MIN_TPS = "tps-min";
    public static final String KEY_MAX_TPS = "tps-max";
    public static final String KEY_TPUT_DURATION = "tput-duration";
    public static final String KEY_GUID_SEED = "guid-seed";

    public static final String OUTPUT_FORMAT = "output-format";
    public static final String OUTPUT_FORMAT_ARECORD = "arecord";
    public static final String OUTPUT_FORMAT_ADM_STRING = "adm-string";

    private int duration;
    private TweetMessageIterator tweetIterator = null;
    private int frameTweetCount = 0;
    private int numFlushedTweets = 0;
    private OutputStream os;
    private DataGenerator dataGenerator = null;
    private ByteBuffer outputBuffer = ByteBuffer.allocate(32 * 1024);

    public TweetGenerator(Map<String, String> configuration, int partition, String format) throws Exception {
        String value = configuration.get(KEY_DURATION);
        duration = value != null ? Integer.parseInt(value) : 60;
        InitializationInfo info = new InitializationInfo();
        info.timeDurationInSecs = duration;
        dataGenerator = new DataGenerator(info);

        String seedValue = configuration.get(KEY_GUID_SEED);
        int seedInt = seedValue != null ? Integer.parseInt(seedValue) : 0;
        tweetIterator = dataGenerator.new TweetMessageIterator(duration, partition, (byte) seedInt);
    }

    private void writeTweetString(TweetMessage next) throws IOException {
        String tweet = next.toString() + "\n";
        byte[] b = tweet.getBytes();
        if (outputBuffer.position() + b.length > outputBuffer.limit()) {
            flush();
            numFlushedTweets += frameTweetCount;
            frameTweetCount = 0;
            outputBuffer.put(b);
            frameTweetCount++;
        } else {
            outputBuffer.put(b);
            frameTweetCount++;
        }
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
        int count = 0;
        if (tweetIterator.hasNext()) {
            while (count < numTweetsInBatch) {
                writeTweetString(tweetIterator.next());
                count++;
            }
            return true;
        }
        return false;
    }

    public void setOutputStream(OutputStream os) {
        this.os = os;
    }
}
