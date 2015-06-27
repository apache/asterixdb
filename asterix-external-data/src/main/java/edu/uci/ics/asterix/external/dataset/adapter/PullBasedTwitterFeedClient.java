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

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.external.util.TweetProcessor;
import edu.uci.ics.asterix.external.util.TwitterUtil;
import edu.uci.ics.asterix.external.util.TwitterUtil.SearchAPIConstants;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * An implementation of @see {PullBasedFeedClient} for the Twitter service. The
 * feed client fetches data from Twitter service by sending request at regular
 * (configurable) interval.
 */
public class PullBasedTwitterFeedClient extends FeedClient {

    private String keywords;
    private Query query;
    private Twitter twitter;
    private int requestInterval = 5; // seconds
    private QueryResult result;

    private ARecordType recordType;
    private int nextTweetIndex = 0;
    private long lastTweetIdReceived = 0;
    private TweetProcessor tweetProcessor;

    public PullBasedTwitterFeedClient(IHyracksTaskContext ctx, ARecordType recordType, PullBasedTwitterAdapter adapter) {
        this.twitter = TwitterUtil.getTwitterService(adapter.getConfiguration());
        this.recordType = recordType;
        this.tweetProcessor = new TweetProcessor(recordType);
        this.recordSerDe = new ARecordSerializerDeserializer(recordType);
        this.mutableRecord = tweetProcessor.getMutableRecord();
        this.initialize(adapter.getConfiguration());
    }

    public ARecordType getRecordType() {
        return recordType;
    }

    @Override
    public InflowState retrieveNextRecord() throws Exception {
        Status tweet;
        tweet = getNextTweet();
        if (tweet == null) {
            return InflowState.DATA_NOT_AVAILABLE;
        }

        tweetProcessor.processNextTweet(tweet);
        return InflowState.DATA_AVAILABLE;
    }

    private void initialize(Map<String, String> params) {
        this.keywords = (String) params.get(SearchAPIConstants.QUERY);
        this.requestInterval = Integer.parseInt((String) params.get(SearchAPIConstants.INTERVAL));
        this.query = new Query(keywords);
        this.query.setCount(100);
    }

    private Status getNextTweet() throws TwitterException, InterruptedException {
        if (result == null || nextTweetIndex >= result.getTweets().size()) {
            Thread.sleep(1000 * requestInterval);
            query.setSinceId(lastTweetIdReceived);
            result = twitter.search(query);
            nextTweetIndex = 0;
        }
        if (result != null && !result.getTweets().isEmpty()) {
            List<Status> tw = result.getTweets();
            Status tweet = tw.get(nextTweetIndex++);
            if (lastTweetIdReceived < tweet.getId()) {
                lastTweetIdReceived = tweet.getId();
            }
            return tweet;
        } else {
            return null;
        }
    }

}
