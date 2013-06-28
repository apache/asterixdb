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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.feed.managed.adapter.IManagedFeedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * RSSFeedAdapter provides the functionality of fetching an RSS based feed.
 */
public class RSSFeedAdapter extends PullBasedAdapter implements IManagedFeedAdapter {

    private static final long serialVersionUID = 1L;

    private List<String> feedURLs = new ArrayList<String>();
    private boolean isStopRequested = false;
    private boolean isAlterRequested = false;
    private Map<String, String> alteredParams = new HashMap<String, String>();
    private String id_prefix = "";
    private ARecordType recordType;

    private IPullBasedFeedClient rssFeedClient;

    public static final String KEY_RSS_URL = "url";
    public static final String KEY_INTERVAL = "interval";

    public boolean isStopRequested() {
        return isStopRequested;
    }

    public void setStopRequested(boolean isStopRequested) {
        this.isStopRequested = isStopRequested;
    }

    @Override
    public void alter(Map<String, String> properties) {
        isAlterRequested = true;
        this.alteredParams = properties;
        reconfigure(properties);
    }

    @Override
    public void stop() {
        isStopRequested = true;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.READ;
    }

    @Override
    public void configure(Map<String, Object> arguments) throws Exception {
        configuration = arguments;
        String rssURLProperty = (String) configuration.get(KEY_RSS_URL);
        if (rssURLProperty == null) {
            throw new IllegalArgumentException("no rss url provided");
        }
        initializeFeedURLs(rssURLProperty);
        configurePartitionConstraints();
        recordType = new ARecordType("FeedRecordType", new String[] { "id", "title", "description", "link" },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                false);
    }

    private void initializeFeedURLs(String rssURLProperty) {
        feedURLs.clear();
        String[] feedURLProperty = rssURLProperty.split(",");
        for (String feedURL : feedURLProperty) {
            feedURLs.add(feedURL);
        }
    }

    protected void reconfigure(Map<String, String> arguments) {
        String rssURLProperty = (String) configuration.get(KEY_RSS_URL);
        if (rssURLProperty != null) {
            initializeFeedURLs(rssURLProperty);
        }
    }

    protected void configurePartitionConstraints() {
        partitionConstraint = new AlgebricksCountPartitionConstraint(feedURLs.size());
    }

    @Override
    public void initialize(IHyracksTaskContext ctx) throws Exception {
        this.ctx = ctx;
        id_prefix = ctx.getJobletContext().getApplicationContext().getNodeId();
    }

    public boolean isAlterRequested() {
        return isAlterRequested;
    }

    public Map<String, String> getAlteredParams() {
        return alteredParams;
    }

    @Override
    public IPullBasedFeedClient getFeedClient(int partition) throws Exception {
        if (rssFeedClient == null) {
            rssFeedClient = new RSSFeedClient(this, feedURLs.get(partition), id_prefix);
        }
        return rssFeedClient;
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return recordType;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        if (partitionConstraint == null) {
            configurePartitionConstraints();
        }
        return partitionConstraint;
    }

}
