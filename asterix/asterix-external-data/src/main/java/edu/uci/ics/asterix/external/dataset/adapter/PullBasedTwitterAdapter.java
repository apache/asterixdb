/*
 * Copyright 2009-2011 by The Regents of the University of California
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Tweet;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import edu.uci.ics.asterix.external.data.adapter.api.IDatasourceReadAdapter;
import edu.uci.ics.asterix.external.data.parser.AbstractStreamDataParser;
import edu.uci.ics.asterix.external.data.parser.IDataParser;
import edu.uci.ics.asterix.external.data.parser.IDataStreamParser;
import edu.uci.ics.asterix.external.data.parser.IManagedDataParser;
import edu.uci.ics.asterix.external.data.parser.ManagedDelimitedDataStreamParser;
import edu.uci.ics.asterix.feed.intake.IFeedClient;
import edu.uci.ics.asterix.feed.managed.adapter.IManagedFeedAdapter;
import edu.uci.ics.asterix.feed.managed.adapter.IMutableFeedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class PullBasedTwitterAdapter extends AbstractDatasourceAdapter implements IDatasourceReadAdapter,
        IManagedFeedAdapter, IMutableFeedAdapter {

    private IDataStreamParser parser;
    private int parallelism = 1;
    private boolean stopRequested = false;
    private boolean alterRequested = false;
    private Map<String, String> alteredParams = new HashMap<String, String>();

    public static final String QUERY = "query";
    public static final String INTERVAL = "interval";

    @Override
    public void configure(Map<String, String> arguments, IAType atype) throws Exception {
        configuration = arguments;
        this.atype = atype;
        partitionConstraint = new AlgebricksCountPartitionConstraint(1);
    }

    @Override
    public AdapterDataFlowType getAdapterDataFlowType() {
        return dataFlowType.PULL;
    }

    @Override
    public AdapterType getAdapterType() {
        return adapterType.READ;
    }

    @Override
    public void initialize(IHyracksTaskContext ctx) throws Exception {
        this.ctx = ctx;
    }

    @Override
    public void beforeSuspend() throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void beforeResume() throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void beforeStop() throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public IDataParser getDataParser(int partition) throws Exception {
        if (parser == null) {
            parser = new ManagedDelimitedDataStreamParser();
            ((IManagedDataParser) parser).setAdapter(this);
            configuration.put(AbstractStreamDataParser.KEY_DELIMITER, "|");
            parser.configure(configuration);
            parser.initialize((ARecordType) atype, ctx);
            TweetClient tweetClient = new TweetClient(ctx.getJobletContext().getApplicationContext().getNodeId(), this);
            TweetStream tweetStream = new TweetStream(tweetClient, ctx);
            parser.setInputStream(tweetStream);
        }
        return parser;
    }

    @Override
    public void stop() throws Exception {
        stopRequested = true;
    }

    public boolean isStopRequested() {
        return stopRequested;
    }

    @Override
    public void alter(Map<String, String> properties) throws Exception {
        alterRequested = true;
        this.alteredParams = properties;
    }

    public boolean isAlterRequested() {
        return alterRequested;
    }

    public Map<String, String> getAlteredParams() {
        return alteredParams;
    }

    public void postAlteration() {
        alteredParams = null;
        alterRequested = false;
    }
}

class TweetStream extends InputStream {

    private ByteBuffer buffer;
    private int capacity;
    private TweetClient tweetClient;
    private List<String> tweets = new ArrayList<String>();

    public TweetStream(TweetClient tweetClient, IHyracksTaskContext ctx) throws Exception {
        capacity = ctx.getFrameSize();
        buffer = ByteBuffer.allocate(capacity);
        this.tweetClient = tweetClient;
        initialize();
    }

    private void initialize() throws Exception {
        boolean hasMore = tweetClient.next(tweets);
        if (!hasMore) {
            buffer.limit(0);
        } else {
            buffer.position(0);
            buffer.limit(capacity);
            for (String tweet : tweets) {
                buffer.put(tweet.getBytes());
                buffer.put("\n".getBytes());
            }
            buffer.flip();
        }
    }

    @Override
    public int read() throws IOException {
        if (!buffer.hasRemaining()) {

            boolean hasMore = tweetClient.next(tweets);
            if (!hasMore) {
                return -1;
            }
            buffer.position(0);
            buffer.limit(capacity);
            for (String tweet : tweets) {
                buffer.put(tweet.getBytes());
                buffer.put("\n".getBytes());
            }
            buffer.flip();
            return buffer.get();
        } else {
            return buffer.get();
        }

    }
}

class TweetClient implements IFeedClient {

    private String query;
    private int timeInterval = 5;
    private Character delimiter = '|';
    private long id = 0;
    private String id_prefix;

    private final PullBasedTwitterAdapter adapter;

    public TweetClient(String id_prefix, PullBasedTwitterAdapter adapter) {
        this.id_prefix = id_prefix;
        this.adapter = adapter;
        initialize(adapter.getConfiguration());
    }

    private void initialize(Map<String, String> params) {
        this.query = params.get(PullBasedTwitterAdapter.QUERY);
        if (params.get(PullBasedTwitterAdapter.INTERVAL) != null) {
            this.timeInterval = Integer.parseInt(params.get(PullBasedTwitterAdapter.INTERVAL));
        }
    }

    @Override
    public boolean next(List<String> tweets) {
        try {
            if (adapter.isStopRequested()) {
                return false;
            }
            if (adapter.isAlterRequested()) {
                initialize(((PullBasedTwitterAdapter) adapter).getAlteredParams());
                adapter.postAlteration();
            }
            Thread.currentThread().sleep(1000 * timeInterval);
            tweets.clear();
            Twitter twitter = new TwitterFactory().getInstance();
            QueryResult result = twitter.search(new Query(query));
            List<Tweet> sourceTweets = result.getTweets();
            for (Tweet tweet : sourceTweets) {
                String tweetContent = formFeedTuple(tweet);
                tweets.add(tweetContent);
                System.out.println(tweetContent);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public String formFeedTuple(Object tweetObject) {
        Tweet tweet = (Tweet) tweetObject;
        StringBuilder builder = new StringBuilder();
        builder.append(id_prefix + ":" + id);
        builder.append(delimiter);
        builder.append(tweet.getFromUserId());
        builder.append(delimiter);
        builder.append("Orange County");
        builder.append(delimiter);
        builder.append(escapeChars(tweet));
        builder.append(delimiter);
        builder.append(tweet.getCreatedAt().toString());
        id++;
        return new String(builder);
    }

    private String escapeChars(Tweet tweet) {
        if (tweet.getText().contains("\n")) {
            return tweet.getText().replace("\n", " ");
        }
        return tweet.getText();
    }

}
