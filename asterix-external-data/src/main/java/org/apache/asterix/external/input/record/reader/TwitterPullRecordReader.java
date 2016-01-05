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
package org.apache.asterix.external.input.record.reader;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.util.TwitterUtil;
import org.apache.asterix.external.util.TwitterUtil.SearchAPIConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;

public class TwitterPullRecordReader implements IRecordReader<Status> {

    private String keywords;
    private Query query;
    private Twitter twitter;
    private int requestInterval = 5; // seconds
    private QueryResult result;
    private int nextTweetIndex = 0;
    private long lastTweetIdReceived = 0;
    private GenericRecord<Status> record;

    public TwitterPullRecordReader() {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        twitter = TwitterUtil.getTwitterService(configuration);
        keywords = configuration.get(SearchAPIConstants.QUERY);
        requestInterval = Integer.parseInt(configuration.get(SearchAPIConstants.INTERVAL));
        query = new Query(keywords);
        query.setCount(100);
        record = new GenericRecord<Status>();
    }

    @Override
    public boolean hasNext() throws Exception {
        return true;
    }

    @Override
    public IRawRecord<Status> next() throws IOException, InterruptedException {
        if (result == null || nextTweetIndex >= result.getTweets().size()) {
            Thread.sleep(1000 * requestInterval);
            query.setSinceId(lastTweetIdReceived);
            try {
                result = twitter.search(query);
            } catch (TwitterException e) {
                throw new HyracksDataException(e);
            }
            nextTweetIndex = 0;
        }
        if (result != null && !result.getTweets().isEmpty()) {
            List<Status> tw = result.getTweets();
            Status tweet = tw.get(nextTweetIndex++);
            if (lastTweetIdReceived < tweet.getId()) {
                lastTweetIdReceived = tweet.getId();
            }
            record.set(tweet);
            return record;
        } else {
            return null;
        }
    }

    @Override
    public Class<Status> getRecordClass() throws IOException {
        return Status.class;
    }

    @Override
    public boolean stop() {
        return false;
    }

}
