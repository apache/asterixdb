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
package org.apache.asterix.external.input.record.reader.twitter;

import java.io.IOException;
import java.util.List;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

public class TwitterPullRecordReader implements IRecordReader<char[]> {

    private Query query;
    private Twitter twitter;
    private int requestInterval = 5; // seconds
    private QueryResult result;
    private int nextTweetIndex = 0;
    private long lastTweetIdReceived = 0;
    private CharArrayRecord record;
    private boolean stopped = false;

    public TwitterPullRecordReader(Twitter twitter, String keywords, int requestInterval) {
        this.twitter = twitter;
        this.requestInterval = requestInterval;
        this.query = new Query(keywords);
        this.query.setCount(100);
        this.record = new CharArrayRecord();
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public boolean hasNext() throws Exception {
        return !stopped;
    }

    @Override
    public IRawRecord<char[]> next() throws IOException, InterruptedException {
        if (result == null || nextTweetIndex >= result.getTweets().size()) {
            Thread.sleep(1000 * requestInterval);
            query.setSinceId(lastTweetIdReceived);
            try {
                result = twitter.search(query);
            } catch (TwitterException e) {
                throw HyracksDataException.create(e);
            }
            nextTweetIndex = 0;
        }
        if (result != null && !result.getTweets().isEmpty()) {
            List<Status> tw = result.getTweets();
            Status tweet = tw.get(nextTweetIndex++);
            if (lastTweetIdReceived < tweet.getId()) {
                lastTweetIdReceived = tweet.getId();
            }
            String jsonTweet = TwitterObjectFactory.getRawJSON(tweet); // transform tweet obj to json
            record.set(jsonTweet);
            return record;
        } else {
            return null;
        }
    }

    @Override
    public boolean stop() {
        stopped = true;
        return true;
    }

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) {
        // do nothing
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
        // do nothing
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }
}
