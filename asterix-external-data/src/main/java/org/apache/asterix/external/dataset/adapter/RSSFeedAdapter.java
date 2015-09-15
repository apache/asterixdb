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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.api.IFeedAdapter;
import org.apache.asterix.common.parse.ITupleForwardPolicy;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;

/**
 * RSSFeedAdapter provides the functionality of fetching an RSS based feed.
 */
public class RSSFeedAdapter extends ClientBasedFeedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    private static final String KEY_RSS_URL = "url";

    private List<String> feedURLs = new ArrayList<String>();
    private String id_prefix = "";

    private IFeedClient rssFeedClient;

    private ARecordType recordType;

    public RSSFeedAdapter(Map<String, String> configuration, ARecordType recordType, IHyracksTaskContext ctx)
            throws AsterixException {
        super(configuration, ctx);
        id_prefix = ctx.getJobletContext().getApplicationContext().getNodeId();
        this.recordType = recordType;
        reconfigure(configuration);
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

    @Override
    public IFeedClient getFeedClient(int partition) throws Exception {
        if (rssFeedClient == null) {
            rssFeedClient = new RSSFeedClient(this, feedURLs.get(partition), id_prefix);
        }
        return rssFeedClient;
    }

    public ARecordType getRecordType() {
        return recordType;
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PULL;
    }

    @Override
    public boolean handleException(Exception e) {
        return false;
    }

    @Override
    public ITupleForwardPolicy getTupleParserPolicy() {
        return AsterixTupleParserFactory.getTupleParserPolicy(configuration);
    }

}
