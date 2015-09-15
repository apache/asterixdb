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
package org.apache.asterix.external.dataset.adapter;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.Query;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.external.util.TweetProcessor;
import org.apache.asterix.external.util.TwitterUtil;
import org.apache.asterix.external.util.TwitterUtil.SearchAPIConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;

/**
 * An implementation of @see {PullBasedFeedClient} for the Twitter service. The
 * feed client fetches data from Twitter service by sending request at regular
 * (configurable) interval.
 */
public class PushBasedTwitterFeedClient extends FeedClient {

    private String keywords;
    private Query query;

    private ARecordType recordType;
    private TweetProcessor tweetProcessor;
    private LinkedBlockingQueue<Status> inputQ;

    public PushBasedTwitterFeedClient(IHyracksTaskContext ctx, ARecordType recordType, PushBasedTwitterAdapter adapter) throws AsterixException {
        this.recordType = recordType;
        this.tweetProcessor = new TweetProcessor(recordType);
        this.recordSerDe = new ARecordSerializerDeserializer(recordType);
        this.mutableRecord = tweetProcessor.getMutableRecord();
        this.initialize(adapter.getConfiguration());
        this.inputQ = new LinkedBlockingQueue<Status>();
        TwitterStream twitterStream = TwitterUtil.getTwitterStream(adapter.getConfiguration());
        twitterStream.addListener(new TweetListener(inputQ));
        FilterQuery query = TwitterUtil.getFilterQuery(adapter.getConfiguration());
        if (query != null) {
            twitterStream.filter(query);
        } else {
            twitterStream.sample();
        }
    }

    public ARecordType getRecordType() {
        return recordType;
    }

    private class TweetListener implements StatusListener {

        private LinkedBlockingQueue<Status> inputQ;

        public TweetListener(LinkedBlockingQueue<Status> inputQ) {
            this.inputQ = inputQ;
        }

        @Override
        public void onStatus(Status tweet) {
            inputQ.add(tweet);
        }

        @Override
        public void onException(Exception arg0) {

        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice arg0) {
        }

        @Override
        public void onScrubGeo(long arg0, long arg1) {
        }

        @Override
        public void onStallWarning(StallWarning arg0) {
        }

        @Override
        public void onTrackLimitationNotice(int arg0) {
        }
    }

    @Override
    public InflowState retrieveNextRecord() throws Exception {
        Status tweet = inputQ.take();
        tweetProcessor.processNextTweet(tweet);
        return InflowState.DATA_AVAILABLE;
    }

    private void initialize(Map<String, String> params) {
        this.keywords = (String) params.get(SearchAPIConstants.QUERY);
        this.query = new Query(keywords);
        this.query.setCount(100);
    }

}
