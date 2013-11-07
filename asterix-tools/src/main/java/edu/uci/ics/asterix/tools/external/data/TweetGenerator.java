package edu.uci.ics.asterix.tools.external.data;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import edu.uci.ics.asterix.tools.external.data.DataGenerator.InitializationInfo;
import edu.uci.ics.asterix.tools.external.data.DataGenerator.TweetMessage;
import edu.uci.ics.asterix.tools.external.data.DataGenerator.TweetMessageIterator;

public class TweetGenerator {

    public static final String NUM_KEY_SPACES = "num-key-spaces";
    public static final String KEY_DURATION = "duration";
    public static final String KEY_TPS = "tps";
    public static final String KEY_TPUT_DURATION = "tput-duration";
    public static final String KEY_GUID_SEED = "guid-seed";
    public static final String KEY_FRAME_WRITER_MODE = "frame-writer-mode";
    public static final String KEY_DATA_MODE = "data-mode";

    public static final String OUTPUT_FORMAT = "output-format";
    public static final String OUTPUT_FORMAT_ARECORD = "arecord";
    public static final String OUTPUT_FORMAT_ADM_STRING = "adm-string";

    private static final int DEFAULT_DURATION = 60;
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
    private GULongIDGenerator[] uidGenerators;
    private int numUidGenerators;
    private FrameWriterMode frameWriterMode;
    private DataMode dataMode;

    public int getTweetCount() {
        return tweetCount;
    }

    public enum DataMode {
        REUSE_DATA,
        NEW_DATA
    }

    public enum FrameWriterMode {
        DUMMY_NO_PARSING,
        PARSING
    }

    public TweetGenerator(Map<String, String> configuration, int partition, String format, OutputStream os)
            throws Exception {
        this.partition = partition;
        String value = configuration.get(KEY_DURATION);
        duration = value != null ? Integer.parseInt(value) : DEFAULT_DURATION;

        value = configuration.get(KEY_DATA_MODE);
        dataMode = value != null ? DataMode.valueOf(value) : DataMode.NEW_DATA;
        numUidGenerators = configuration.get(NUM_KEY_SPACES) != null ? Integer.parseInt(configuration
                .get(NUM_KEY_SPACES)) : 1;
        uidGenerators = new GULongIDGenerator[numUidGenerators];

        int guidSeed = configuration.get(KEY_GUID_SEED) != null ? Integer.parseInt(configuration.get(KEY_GUID_SEED))
                : DEFAULT_GUID_SEED;

        for (int i = 0; i < uidGenerators.length; i++) {
            uidGenerators[i] = new GULongIDGenerator(partition, (byte) (i + guidSeed));
        }

        InitializationInfo info = new InitializationInfo();
        dataGenerator = new DataGenerator(info);
        value = configuration.get(KEY_FRAME_WRITER_MODE);
        frameWriterMode = value != null ? FrameWriterMode.valueOf(value.toUpperCase()) : FrameWriterMode.PARSING;
        dataMode = configuration.get(KEY_DATA_MODE) != null ? DataMode.valueOf(configuration.get(KEY_DATA_MODE))
                : DataMode.NEW_DATA;
        tweetIterator = dataGenerator.new TweetMessageIterator(duration, uidGenerators, dataMode);
        this.os = os;
    }

    private void writeTweetString(TweetMessage tweetMessage) throws IOException {
        String tweet = tweetMessage.toString() + "\n";
        tweetCount++;
        if (frameWriterMode.equals(FrameWriterMode.PARSING)) {
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
    }

    public int getNumFlushedTweets() {
        return numFlushedTweets;
    }

    private void flush() throws IOException {
        outputBuffer.flip();
        os.write(outputBuffer.array(), 0, outputBuffer.limit());
        outputBuffer.position(0);
        outputBuffer.limit(32 * 1024);
        tweetIterator.toggleUidKeySpace();
    }

    public boolean setNextRecordBatch(int numTweetsInBatch) throws Exception {
        boolean moreData = tweetIterator.hasNext();
        if (!moreData) {
            System.out.println("TWEET COUNT: [" + partition + "]" + tweetCount);
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