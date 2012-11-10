package edu.uci.ics.asterix.feed.intake;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Tweet;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.external.dataset.adapter.PullBasedTwitterAdapter;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class PullBasedTwitterFeedClient extends PullBasedFeedClient {

    private String keywords;
    private Query query;
    private long id = 0;
    private String id_prefix;
    private Twitter twitter;
    private int requestInterval = 10; // seconds
    private Queue<Tweet> tweetBuffer = new LinkedList<Tweet>();

    IAObject[] mutableFields;
    String[] tupleFieldValues;
    private ARecordType recordType;

    public PullBasedTwitterFeedClient(IHyracksTaskContext ctx, PullBasedTwitterAdapter adapter) {
        this.id_prefix = ctx.getJobletContext().getApplicationContext().getNodeId();
        twitter = new TwitterFactory().getInstance();
        mutableFields = new IAObject[] { new AMutableString(null), new AMutableString(null), new AMutableString(null),
                new AMutableString(null), new AMutableString(null) };
        recordType = adapter.getAdapterOutputType();
        recordSerDe = new ARecordSerializerDeserializer(recordType);
        mutableRecord = new AMutableRecord(recordType, mutableFields);
        initialize(adapter.getConfiguration());
        tupleFieldValues = new String[recordType.getFieldNames().length];
    }

    public void initialize(Map<String, String> params) {
        this.keywords = params.get(PullBasedTwitterAdapter.QUERY);
        this.query = new Query(keywords);
        query.setRpp(100);
    }

    private Tweet getNextTweet() throws TwitterException, InterruptedException {
        if (tweetBuffer.isEmpty()) {
            QueryResult result;
            Thread.currentThread().sleep(1000 * requestInterval);
            result = twitter.search(query);
            tweetBuffer.addAll(result.getTweets());
        }
        return tweetBuffer.remove();
    }

    public ARecordType getRecordType() {
        return recordType;
    }

    public AMutableRecord getMutableRecord() {
        return mutableRecord;
    }

    @Override
    public boolean setNextRecord() throws Exception {
        Tweet tweet;
        tweet = getNextTweet();
        if (tweet == null) {
            return false;
        }
        int numFields = recordType.getFieldNames().length;

        tupleFieldValues[0] = id_prefix + ":" + id;
        tupleFieldValues[1] = tweet.getFromUser();
        tupleFieldValues[2] = tweet.getLocation() == null ? "" : tweet.getLocation();
        tupleFieldValues[3] = tweet.getText();
        tupleFieldValues[4] = tweet.getCreatedAt().toString();
        for (int i = 0; i < numFields; i++) {
            ((AMutableString) mutableFields[i]).setValue(tupleFieldValues[i]);
            mutableRecord.setValueAtPos(i, mutableFields[i]);
        }
        id++;
        return true;
    }

    @Override
    public void resetOnFailure(Exception e) throws AsterixException {
        // TOOO: implement resetting logic for Twitter
    }

}
