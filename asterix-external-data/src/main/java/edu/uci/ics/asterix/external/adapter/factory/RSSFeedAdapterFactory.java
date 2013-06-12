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
package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.RSSFeedAdapter;

/**
 * Factory class for creating an instance of @see {RSSFeedAdapter}.
 * RSSFeedAdapter provides the functionality of fetching an RSS based feed.
 */
public class RSSFeedAdapterFactory implements ITypedDatasetAdapterFactory {
    private static final long serialVersionUID = 1L;
    public static final String RSS_FEED_ADAPTER_NAME = "rss_feed";

    @Override
    public IDatasourceAdapter createAdapter(Map<String, Object> configuration) throws Exception {
        RSSFeedAdapter rssFeedAdapter = new RSSFeedAdapter();
        rssFeedAdapter.configure(configuration);
        return rssFeedAdapter;
    }

    @Override
    public String getName() {
        return "rss_feed";
    }

}
