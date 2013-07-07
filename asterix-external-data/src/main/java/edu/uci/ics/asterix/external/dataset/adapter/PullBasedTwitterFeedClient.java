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
package edu.uci.ics.asterix.external.dataset.adapter;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Tweet;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * An implementation of @see {PullBasedFeedClient} for the Twitter service.
 * The feed client fetches data from Twitter service by sending request at
 * regular (configurable) interval.
 */
public class PullBasedTwitterFeedClient extends PullBasedFeedClient {

    private String keywords;
    private Query query;
    private String id_prefix;
    private Twitter twitter;
    private int requestInterval = 10; // seconds
    private QueryResult result;

    private IAObject[] mutableFields;
    private String[] tupleFieldValues;
    private ARecordType recordType;
    private int nextTweetIndex = 0;

    private static final Logger LOGGER = Logger.getLogger(PullBasedTwitterFeedClient.class.getName());

    public PullBasedTwitterFeedClient(IHyracksTaskContext ctx, PullBasedTwitterAdapter adapter) {
        this.id_prefix = ctx.getJobletContext().getApplicationContext().getNodeId();
        twitter = new TwitterFactory().getInstance();
        mutableFields = new IAObject[] { new AMutableString(null), new AMutableString(null), new AMutableString(null),
                new AMutableString(null), new AMutableString(null) };
        recordType = adapter.getAdapterOutputType();
        recordSerDe = new ARecordSerializerDeserializer(recordType);
        mutableRecord = new AMutableRecord(recordType, mutableFields);
        tupleFieldValues = new String[recordType.getFieldNames().length];
        initialize(adapter.getConfiguration());
    }

    public ARecordType getRecordType() {
        return recordType;
    }

    public AMutableRecord getMutableRecord() {
        return mutableRecord;
    }

    @Override
    public InflowState setNextRecord() throws Exception {
        Tweet tweet;
        tweet = getNextTweet();
        if (tweet == null) {
            return InflowState.DATA_NOT_AVAILABLE;
        }
        int numFields = recordType.getFieldNames().length;
        tupleFieldValues[0] = UUID.randomUUID().toString();
        tupleFieldValues[1] = tweet.getFromUser();
        tupleFieldValues[2] = tweet.getLocation() == null ? "" : tweet.getLocation();
        tupleFieldValues[3] = tweet.getText();
        tupleFieldValues[4] = tweet.getCreatedAt().toString();
        for (int i = 0; i < numFields; i++) {
            ((AMutableString) mutableFields[i]).setValue(tupleFieldValues[i]);
            mutableRecord.setValueAtPos(i, mutableFields[i]);
        }
        return InflowState.DATA_AVAILABLE;
    }

    @Override
    public void resetOnFailure(Exception e) throws AsterixException {
        // TOOO: implement resetting logic for Twitter
    }

    @Override
    public boolean alter(Map<String, String> configuration) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub
    }

    private void initialize(Map<String, String> params) {
        this.keywords = (String) params.get(PullBasedTwitterAdapter.QUERY);
        this.requestInterval = Integer.parseInt((String) params.get(PullBasedTwitterAdapter.INTERVAL));
        this.query = new Query(keywords);
        query.setRpp(100);
    }

    private Tweet getNextTweet() throws TwitterException, InterruptedException {
        if (result == null || nextTweetIndex >= result.getTweets().size()) {
            Thread.sleep(1000 * requestInterval);
            result = twitter.search(query);
            nextTweetIndex = 0;
        }
        List<Tweet> tw = result.getTweets();
        return tw.get(nextTweetIndex++);
    }

}
