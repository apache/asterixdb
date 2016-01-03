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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.log4j.Logger;

import com.sun.syndication.feed.synd.SyndEntryImpl;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.fetcher.FeedFetcher;
import com.sun.syndication.fetcher.FetcherEvent;
import com.sun.syndication.fetcher.FetcherException;
import com.sun.syndication.fetcher.FetcherListener;
import com.sun.syndication.fetcher.impl.FeedFetcherCache;
import com.sun.syndication.fetcher.impl.HashMapFeedInfoCache;
import com.sun.syndication.fetcher.impl.HttpURLFeedFetcher;
import com.sun.syndication.io.FeedException;

public class RSSRecordReader implements IRecordReader<SyndEntryImpl> {

    private static final Logger LOGGER = Logger.getLogger(RSSRecordReader.class.getName());
    private boolean modified = false;
    private Queue<SyndEntryImpl> rssFeedBuffer = new LinkedList<SyndEntryImpl>();
    private FeedFetcherCache feedInfoCache;
    private FeedFetcher fetcher;
    private FetcherEventListenerImpl listener;
    private URL feedUrl;
    private GenericRecord<SyndEntryImpl> record = new GenericRecord<SyndEntryImpl>();
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
    public void configure(Map<String, String> configurations) throws Exception {
    }

    @Override
    public boolean hasNext() throws Exception {
        return !done;
    }

    @Override
    public IRawRecord<SyndEntryImpl> next() throws IOException {
        if (done) {
            return null;
        }
        try {
            SyndEntryImpl feedEntry;
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
    public Class<SyndEntryImpl> getRecordClass() throws IOException {
        return SyndEntryImpl.class;
    }

    @Override
    public boolean stop() {
        done = true;
        return true;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    private SyndEntryImpl getNextRSSFeed() throws Exception {
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
    private void fetchFeed() throws IllegalArgumentException, IOException, FeedException, FetcherException {
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
            List<? extends SyndEntryImpl> fetchedFeeds = feed.getEntries();
            rssFeedBuffer.addAll(fetchedFeeds);
        }
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
     * @see com.sun.syndication.fetcher.FetcherListener#fetcherEvent(com.sun.syndication.fetcher.FetcherEvent)
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
