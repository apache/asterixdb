package edu.uci.ics.asterix.tools.external.data;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Logger;

import edu.uci.ics.asterix.tools.external.data.DataGenerator2.InitializationInfo;
import edu.uci.ics.asterix.tools.external.data.DataGenerator2.TweetMessage;
import edu.uci.ics.asterix.tools.external.data.DataGenerator2.TweetMessageIterator;

public class TweetGenerator2 {

    private static final Logger LOGGER = Logger.getLogger(TweetGenerator.class.getName());

    public static final String KEY_DURATION = "duration";
    public static final String KEY_TPS = "tps";
    public static final String KEY_MIN_TPS = "tps-min";
    public static final String KEY_MAX_TPS = "tps-max";


    public static final String KEY_TPUT_DURATION = "tput-duration";
  
    public static final String OUTPUT_FORMAT = "output-format";

    public static final String OUTPUT_FORMAT_ARECORD = "arecord";
    public static final String OUTPUT_FORMAT_ADM_STRING = "adm-string";

    private int duration;
    private long tweetInterval;
    private int numTweetsBeforeDelay;
    private TweetMessageIterator tweetIterator = null;
    private long exeptionInterval;

  

    private int partition;
    private int tweetCount = 0;
    private int frameTweetCount = 0;
    private int numFlushedTweets = 0;

    public int getTweetCount() {
        return tweetCount;
    }

    private int exceptionPeriod = -1;
    private boolean isOutputFormatRecord = false;
    private byte[] EOL = "\n".getBytes();
    private OutputStream os;
    private DataGenerator2 dataGenerator = null;
    private ByteBuffer outputBuffer = ByteBuffer.allocate(32 * 1024);
    private int flushedTweetCount = 0;

    public TweetGenerator2(Map<String, String> configuration, int partition, String format) throws Exception {
        String value = configuration.get(KEY_DURATION);
        duration = value != null ? Integer.parseInt(value) : 60;
        initializeTweetRate(configuration.get(KEY_TPS));
        if (value != null) {
            exceptionPeriod = Integer.parseInt(value);
        }
       
        isOutputFormatRecord = format.equalsIgnoreCase(OUTPUT_FORMAT_ARECORD);
        InitializationInfo info = new InitializationInfo();
        info.timeDurationInSecs = duration;
        dataGenerator = new DataGenerator2(info);
        tweetIterator = dataGenerator.new TweetMessageIterator(duration);
    }

    private void initializeTweetRate(String tps) {
        numTweetsBeforeDelay = 0;
        if (tps == null) {
            tweetInterval = 0;
        } else {
            int val = Integer.parseInt(tps);
            double interval = new Double(((double) 1000 / val));
            if (interval > 1) {
                tweetInterval = (long) interval;
                numTweetsBeforeDelay = 1;
            } else {
                tweetInterval = 1;
                Double numTweets = new Double(1 / interval);
                if (numTweets.intValue() != numTweets) {
                    tweetInterval = 5;
                    numTweetsBeforeDelay = (new Double(10 * numTweets * 1)).intValue();
                } else {
                    numTweetsBeforeDelay = new Double((numTweets * 1)).intValue();
                }
            }
        }

    }

    private void writeTweetString(TweetMessage next) throws IOException {
        String tweet = next.toString() + "\n";
        tweetCount++;
        byte[] b = tweet.getBytes();
        if (outputBuffer.position() + b.length > outputBuffer.limit()) {
            flush();
            numFlushedTweets += frameTweetCount;
            frameTweetCount = 0;
            flushedTweetCount += tweetCount - 1;
            outputBuffer.put(tweet.getBytes());
            frameTweetCount++;

        } else {
            outputBuffer.put(tweet.getBytes());
            frameTweetCount++;
        }
    }

    public int getNumFlushedTweets() {
        return numFlushedTweets;
    }

    public int getFrameTweetCount() {
        return frameTweetCount;
    }

    private void flush() throws IOException {
        outputBuffer.flip();
        os.write(outputBuffer.array(), 0, outputBuffer.limit());
        outputBuffer.position(0);
        outputBuffer.limit(32 * 1024);
    }

    private void writeTweetRecord(TweetMessage next) {
        throw new UnsupportedOperationException("Invalid format");
    }

    public boolean setNextRecord() throws Exception {
        boolean moreData = tweetIterator.hasNext();
        if (!moreData) {
            return false;
        }
        TweetMessage msg = tweetIterator.next();
        if (isOutputFormatRecord) {
            writeTweetRecord(msg);
        } else {
            writeTweetString(msg);
        }
        if (tweetInterval != 0) {
            tweetCount++;
            if (tweetCount == numTweetsBeforeDelay) {
                Thread.sleep(tweetInterval);
                tweetCount = 0;
            }
        }
        return true;
    }

    public boolean setNextRecordBatch(int numTweetsInBatch) throws Exception {
        int count = 0;
        // if (tweetIterator.hasNext()) {
        while (count < numTweetsInBatch) {
            writeTweetString(tweetIterator.next());
            count++;
        }
        // } else {
        //   System.out.println("Flushing last batch, count so far:" + tweetCount);
        // flush();
        /// }
        return true;
    }

    public void setOutputStream(OutputStream os) {
        this.os = os;
    }
}
