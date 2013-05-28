package edu.uci.ics.asterix.tools.external.data;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.external.dataset.adapter.IPullBasedFeedClient;
import edu.uci.ics.asterix.external.dataset.adapter.PullBasedAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.PullBasedFeedClient;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutablePoint;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AMutableUnorderedList;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.tools.external.data.DataGenerator.InitializationInfo;
import edu.uci.ics.asterix.tools.external.data.DataGenerator.Message;
import edu.uci.ics.asterix.tools.external.data.DataGenerator.TweetMessage;
import edu.uci.ics.asterix.tools.external.data.DataGenerator.TweetMessageIterator;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * TPS can be configured between 1 and 20,000  
 * @author ramang
 *
 */
public class SyntheticTwitterFeedAdapter extends PullBasedAdapter {

    private static final long serialVersionUID = 1L;
    private Map<String, String> configuration;

    public SyntheticTwitterFeedAdapter(Map<String, String> configuration) throws AsterixException {
        this.configuration = configuration;

        String[] userFieldNames = new String[] { "screen-name", "lang", "friends_count", "statuses_count", "name",
                "followers_count" };

        IAType[] userFieldTypes = new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32,
                BuiltinType.AINT32, BuiltinType.ASTRING, BuiltinType.AINT32 };
        ARecordType userRecordType = new ARecordType("TwitterUserType", userFieldNames, userFieldTypes, false);

        String[] fieldNames = new String[] { "tweetid", "user", "sender-location", "send-time", "referred-topics",
                "message-text" };

        AUnorderedListType unorderedListType = new AUnorderedListType(BuiltinType.ASTRING, "referred-topics");
        IAType[] fieldTypes = new IAType[] { BuiltinType.ASTRING, userRecordType, BuiltinType.APOINT,
                BuiltinType.ADATETIME, unorderedListType, BuiltinType.ASTRING };
        adapterOutputType = new ARecordType("TweetMessageType", fieldNames, fieldTypes, false);
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.READ;
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        this.configuration = configuration;

    }

    @Override
    public void initialize(IHyracksTaskContext ctx) throws Exception {
        this.ctx = ctx;
    }

    @Override
    public IPullBasedFeedClient getFeedClient(int partition) throws Exception {
        return new SyntheticTwitterFeedClient(configuration, adapterOutputType, partition);
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(1);
    }

    private static class SyntheticTwitterFeedClient extends PullBasedFeedClient implements IPullBasedFeedClient {

        private static final Logger LOGGER = Logger.getLogger(SyntheticTwitterFeedClient.class.getName());

        public static final String KEY_DURATION = "duration";
        public static final String KEY_TPS = "tps";
        public static final String KEY_EXCEPTION_PERIOD = "exception-period";

        private int duration;
        private long tweetInterval;
        private int numTweetsBeforeDelay;
        private TweetMessageIterator tweetIterator = null;
        private long exeptionInterval;

        private IAObject[] mutableFields;
        private ARecordType outputRecordType;
        private int partition;
        private int tweetCount = 0;

        public SyntheticTwitterFeedClient(Map<String, String> configuration, ARecordType outputRecordType,
                int partition) throws AsterixException {
            this.outputRecordType = outputRecordType;
            String value = configuration.get(KEY_DURATION);
            duration = value != null ? Integer.parseInt(value) : 60;
            initializeTweetRate(configuration.get(KEY_TPS));
            InitializationInfo info = new InitializationInfo();
            info.timeDurationInSecs = duration;
            DataGenerator.initialize(info);
            tweetIterator = new TweetMessageIterator(duration);
            initialize();
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
                        tweetInterval = 10;
                        numTweetsBeforeDelay = (new Double(10 * numTweets * 1.4)).intValue();
                    } else {
                        numTweetsBeforeDelay = new Double((numTweets * 1.4)).intValue();
                    }
                }
            }

        }

        private void writeTweet(TweetMessage next) {

            //tweet id
            ((AMutableString) mutableFields[0]).setValue(next.getTweetid());
            mutableRecord.setValueAtPos(0, mutableFields[0]);

            // user 
            AMutableRecord userRecord = ((AMutableRecord) mutableFields[1]);
            ((AMutableString) userRecord.getValueByPos(0)).setValue(next.getUser().getScreenName());
            ((AMutableString) userRecord.getValueByPos(1)).setValue("en");
            ((AMutableInt32) userRecord.getValueByPos(2)).setValue(next.getUser().getFriendsCount());
            ((AMutableInt32) userRecord.getValueByPos(3)).setValue(next.getUser().getStatusesCount());
            ((AMutableString) userRecord.getValueByPos(4)).setValue(next.getUser().getName());
            ((AMutableInt32) userRecord.getValueByPos(5)).setValue(next.getUser().getFollowersCount());
            mutableRecord.setValueAtPos(1, userRecord);

            // location
            ((AMutablePoint) mutableFields[2]).setValue(next.getSenderLocation().getLatitude(), next
                    .getSenderLocation().getLongitude());
            mutableRecord.setValueAtPos(2, mutableFields[2]);

            // time
            ((AMutableDateTime) mutableFields[3]).setValue(next.getSendTime().getChrononTime());
            mutableRecord.setValueAtPos(3, mutableFields[3]);

            // referred topics
            ((AMutableUnorderedList) mutableFields[4]).clear();
            List<String> referredTopics = next.getReferredTopics();
            for (String topic : referredTopics) {
                ((AMutableUnorderedList) mutableFields[4]).add(new AMutableString(topic));
            }
            mutableRecord.setValueAtPos(4, mutableFields[4]);

            // text
            Message m = next.getMessageText();
            char[] content = m.getMessage();
            ((AMutableString) mutableFields[5]).setValue(new String(content, 0, m.getLength()));
            mutableRecord.setValueAtPos(5, mutableFields[5]);

        }

        @Override
        public void resetOnFailure(Exception e) throws AsterixException {
            // TODO Auto-generated method stub

        }

        @Override
        public boolean alter(Map<String, String> configuration) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public InflowState setNextRecord() throws Exception {
            boolean moreData = tweetIterator.hasNext();
            if (!moreData) {
                return InflowState.NO_MORE_DATA;
            }
            writeTweet(tweetIterator.next());
            if (tweetInterval != 0) {
                tweetCount++;
                if (tweetCount == numTweetsBeforeDelay) {
                    Thread.sleep(tweetInterval);
                    tweetCount = 0;
                }
            }
            return InflowState.DATA_AVAILABLE;
        }

        private void initialize() throws AsterixException {
            ARecordType userRecordType = (ARecordType) outputRecordType.getFieldTypes()[1];
            IAObject[] userMutableFields = new IAObject[] { new AMutableString(""), new AMutableString(""),
                    new AMutableInt32(0), new AMutableInt32(0), new AMutableString(""), new AMutableInt32(0) };
            AUnorderedListType unorderedListType = new AUnorderedListType(BuiltinType.ASTRING, "referred-topics");
            mutableFields = new IAObject[] { new AMutableString(""),
                    new AMutableRecord(userRecordType, userMutableFields), new AMutablePoint(0, 0),
                    new AMutableDateTime(0), new AMutableUnorderedList(unorderedListType), new AMutableString("") };
            recordSerDe = new ARecordSerializerDeserializer(outputRecordType);
            mutableRecord = new AMutableRecord(outputRecordType, mutableFields);

        }
    }
}
