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
package org.apache.asterix.external.input.record.reader.rss;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rometools.fetcher.FeedFetcher;
import com.rometools.fetcher.FetcherEvent;
import com.rometools.fetcher.FetcherException;
import com.rometools.fetcher.FetcherListener;
import com.rometools.fetcher.impl.FeedFetcherCache;
import com.rometools.fetcher.impl.HashMapFeedInfoCache;
import com.rometools.fetcher.impl.HttpURLFeedFetcher;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;

public class RSSRecordReader implements IRecordReader<SyndEntry> {

    private static final Logger LOGGER = LogManager.getLogger();
    private boolean modified = false;
    private Queue<SyndEntry> rssFeedBuffer = new LinkedList<>();
    private FeedFetcherCache feedInfoCache;
    private FeedFetcher fetcher;
    private FetcherEventListenerImpl listener;
    private URL feedUrl;
    private GenericRecord<SyndEntry> record = new GenericRecord<>();
    private boolean done = false;

    public RSSRecordReader(String url) throws MalformedURLException {
        feedUrl = new URL(url);
        feedInfoCache = HashMapFeedInfoCache.getInstance();
        fetcher = new HttpURLFeedFetcher(feedInfoCache);
        listener = new FetcherEventListenerImpl(this, LOGGER);
        fetcher.addFetcherEventListener(listener);
    }

    public boolean isModified() {
        return modified;
    }

    @Override
    public void close() throws IOException {
        fetcher.removeFetcherEventListener(listener);
    }

    @Override
    public boolean hasNext() throws Exception {
        return !done;
    }

    @Override
    public IRawRecord<SyndEntry> next() throws IOException {
        if (done) {
            return null;
        }
        try {
            SyndEntry feedEntry;
            feedEntry = getNextRSSFeed();
            if (feedEntry == null) {
                return null;
            }
            record.set(feedEntry);
            return record;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean stop() {
        done = true;
        return true;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    private SyndEntry getNextRSSFeed() throws IOException, FeedException, FetcherException {
        if (rssFeedBuffer.isEmpty()) {
            fetchFeed();
        }
        if (rssFeedBuffer.isEmpty()) {
            return null;
        } else {
            return rssFeedBuffer.remove();
        }
    }

    @SuppressWarnings("unchecked")
    private void fetchFeed() throws IOException, FeedException, FetcherException {
        // Retrieve the feed.
        // We will get a Feed Polled Event and then a
        // Feed Retrieved event (assuming the feed is valid)
        SyndFeed feed = fetcher.retrieveFeed(feedUrl);
        if (modified) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(feedUrl + " retrieved");
                LOGGER.info(feedUrl + " has a title: " + feed.getTitle() + " and contains " + feed.getEntries().size()
                        + " entries.");
            }
            List<SyndEntry> fetchedFeeds = feed.getEntries();
            rssFeedBuffer.addAll(fetchedFeeds);
        }
    }

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) {
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }
}

class FetcherEventListenerImpl implements FetcherListener {

    private RSSRecordReader reader;
    private Logger LOGGER;

    public FetcherEventListenerImpl(RSSRecordReader reader, Logger LOGGER) {
        this.reader = reader;
        this.LOGGER = LOGGER;
    }

    /**
     * @see com.rometools.fetcher.FetcherListener#fetcherEvent(com.rometools.fetcher.FetcherEvent)
     */
    @Override
    public void fetcherEvent(FetcherEvent event) {
        String eventType = event.getEventType();
        if (FetcherEvent.EVENT_TYPE_FEED_POLLED.equals(eventType)) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("\tEVENT: Feed Polled. URL = " + event.getUrlString());
            }
        } else if (FetcherEvent.EVENT_TYPE_FEED_RETRIEVED.equals(eventType)) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("\tEVENT: Feed Retrieved. URL = " + event.getUrlString());
            }
            (reader).setModified(true);
        } else if (FetcherEvent.EVENT_TYPE_FEED_UNCHANGED.equals(eventType)) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("\tEVENT: Feed Unchanged. URL = " + event.getUrlString());
            }
            (reader).setModified(true);
        }
    }
}
