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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.external.data.adapter.api.IDatasourceAdapter;
import edu.uci.ics.asterix.external.data.parser.IDataParser;
import edu.uci.ics.asterix.external.data.parser.IDataStreamParser;
import edu.uci.ics.asterix.external.data.parser.IManagedDataParser;
import edu.uci.ics.asterix.external.data.parser.ManagedDelimitedDataStreamParser;
import edu.uci.ics.asterix.feed.intake.FeedStream;
import edu.uci.ics.asterix.feed.intake.IFeedClient;
import edu.uci.ics.asterix.feed.intake.RSSFeedClient;
import edu.uci.ics.asterix.feed.managed.adapter.IManagedFeedAdapter;
import edu.uci.ics.asterix.feed.managed.adapter.IMutableFeedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class RSSFeedAdapter extends AbstractDatasourceAdapter implements IDatasourceAdapter, IManagedFeedAdapter,
        IMutableFeedAdapter {

    private List<String> feedURLs = new ArrayList<String>();
    private boolean isStopRequested = false;
    private boolean isAlterRequested = false;
    private Map<String, String> alteredParams = new HashMap<String, String>();
    private String id_prefix = "";

    public static final String KEY_RSS_URL = "url";
    public static final String KEY_INTERVAL = "interval";

    public boolean isStopRequested() {
        return isStopRequested;
    }

    public void setStopRequested(boolean isStopRequested) {
        this.isStopRequested = isStopRequested;
    }

    @Override
    public IDataParser getDataParser(int partition) throws Exception {
        IDataParser dataParser = new ManagedDelimitedDataStreamParser();
        ((IManagedDataParser) dataParser).setAdapter(this);
        dataParser.configure(configuration);
        dataParser.initialize((ARecordType) atype, ctx);
        IFeedClient feedClient = new RSSFeedClient(this, feedURLs.get(partition), id_prefix);
        FeedStream feedStream = new FeedStream(feedClient, ctx);
        ((IDataStreamParser) dataParser).setInputStream(feedStream);
        return dataParser;
    }

    @Override
    public void alter(Map<String, String> properties) throws Exception {
        isAlterRequested = true;
        this.alteredParams = properties;
        reconfigure(properties);
    }

    public void postAlteration() {
        alteredParams = null;
        isAlterRequested = false;
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
    public void stop() throws Exception {
        isStopRequested = true;
    }

    @Override
    public AdapterDataFlowType getAdapterDataFlowType() {
        return AdapterDataFlowType.PULL;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.READ;
    }

    @Override
    public void configure(Map<String, String> arguments, IAType atype) throws Exception {
        configuration = arguments;
        this.atype = atype;
        String rssURLProperty = configuration.get(KEY_RSS_URL);
        if (rssURLProperty == null) {
            throw new IllegalArgumentException("no rss url provided");
        }
        initializeFeedURLs(rssURLProperty);
        configurePartitionConstraints();

    }

    private void initializeFeedURLs(String rssURLProperty) {
        feedURLs.clear();
        String[] feedURLProperty = rssURLProperty.split(",");
        for (String feedURL : feedURLProperty) {
            feedURLs.add(feedURL);
        }
    }

    protected void reconfigure(Map<String, String> arguments) {
        String rssURLProperty = configuration.get(KEY_RSS_URL);
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

}
