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
package org.apache.asterix.external.adapter.factory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.feeds.FeedPolicyAccessor;
import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.common.feeds.api.IIntakeProgressTracker;
import org.apache.asterix.external.dataset.adapter.RSSFeedAdapter;
import org.apache.asterix.metadata.feeds.IFeedAdapterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;

/**
 * Factory class for creating an instance of @see {RSSFeedAdapter}.
 * RSSFeedAdapter provides the functionality of fetching an RSS based feed.
 */
public class RSSFeedAdapterFactory implements IFeedAdapterFactory {
    private static final long serialVersionUID = 1L;
    public static final String RSS_FEED_ADAPTER_NAME = "rss_feed";

    public static final String KEY_RSS_URL = "url";
    public static final String KEY_INTERVAL = "interval";

    private Map<String, String> configuration;
    private ARecordType outputType;
    private List<String> feedURLs = new ArrayList<String>();
    private FeedPolicyAccessor ingestionPolicy;

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        RSSFeedAdapter rssFeedAdapter = new RSSFeedAdapter(configuration, outputType, ctx);
        return rssFeedAdapter;
    }

    @Override
    public String getName() {
        return "rss_feed";
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.configuration = configuration;
        this.outputType = outputType;
        String rssURLProperty = (String) configuration.get(KEY_RSS_URL);
        if (rssURLProperty == null) {
            throw new IllegalArgumentException("no rss url provided");
        }
        initializeFeedURLs(rssURLProperty);
        configurePartitionConstraints();
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(feedURLs.size());
    }

    private void initializeFeedURLs(String rssURLProperty) {
        feedURLs.clear();
        String[] feedURLProperty = rssURLProperty.split(",");
        for (String feedURL : feedURLProperty) {
            feedURLs.add(feedURL);
        }
    }

    protected void configurePartitionConstraints() {

    }

    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }

    @Override
    public boolean isRecordTrackingEnabled() {
        return false;
    }

    @Override
    public IIntakeProgressTracker createIntakeProgressTracker() {
        return null;
    }

    public FeedPolicyAccessor getIngestionPolicy() {
        return ingestionPolicy;
    }

}
