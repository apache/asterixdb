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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.sun.syndication.feed.synd.SyndEntryImpl;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.fetcher.FeedFetcher;
import com.sun.syndication.fetcher.FetcherEvent;
import com.sun.syndication.fetcher.FetcherListener;
import com.sun.syndication.fetcher.impl.FeedFetcherCache;
import com.sun.syndication.fetcher.impl.HashMapFeedInfoCache;
import com.sun.syndication.fetcher.impl.HttpURLFeedFetcher;

import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;

/**
 * An implementation of @see {PullBasedFeedClient} responsible for
 * fetching from an RSS feed source at regular interval.
 */
@SuppressWarnings("rawtypes")
public class RSSFeedClient extends PullBasedFeedClient {

    private final String feedURL;
    private long id = 0;
    private String idPrefix;
    private boolean feedModified = false;

    private Queue<SyndEntryImpl> rssFeedBuffer = new LinkedList<SyndEntryImpl>();

    IAObject[] mutableFields;

    private final FeedFetcherCache feedInfoCache;
    private final FeedFetcher fetcher;
    private final FetcherEventListenerImpl listener;
    private final URL feedUrl;
    private ARecordType recordType;
    String[] tupleFieldValues;

    public boolean isFeedModified() {
        return feedModified;
    }

    public void setFeedModified(boolean feedModified) {
        this.feedModified = feedModified;
    }

    public RSSFeedClient(RSSFeedAdapter adapter, String feedURL, String id_prefix) throws MalformedURLException {
        this.feedURL = feedURL;
        this.idPrefix = id_prefix;
        feedUrl = new URL(feedURL);
        feedInfoCache = HashMapFeedInfoCache.getInstance();
        fetcher = new HttpURLFeedFetcher(feedInfoCache);
        listener = new FetcherEventListenerImpl(this);
        fetcher.addFetcherEventListener(listener);
        mutableFields = new IAObject[] { new AMutableString(null), new AMutableString(null), new AMutableString(null),
                new AMutableString(null) };
        recordType = adapter.getAdapterOutputType();
        mutableRecord = new AMutableRecord(recordType, mutableFields);
        tupleFieldValues = new String[recordType.getFieldNames().length];
    }

    @Override
    public boolean setNextRecord() throws Exception {
        SyndEntryImpl feedEntry = getNextRSSFeed();
        if (feedEntry == null) {
            return false;
        }
        tupleFieldValues[0] = idPrefix + ":" + id;
        tupleFieldValues[1] = feedEntry.getTitle();
        tupleFieldValues[2] = feedEntry.getDescription().getValue();
        tupleFieldValues[3] = feedEntry.getLink();
        int numFields = recordType.getFieldNames().length;
        for (int i = 0; i < numFields; i++) {
            ((AMutableString) mutableFields[i]).setValue(tupleFieldValues[i]);
            mutableRecord.setValueAtPos(i, mutableFields[i]);
        }
        id++;
        return true;
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
    private void fetchFeed() {
        try {
            System.err.println("Retrieving feed " + feedURL);
            // Retrieve the feed.
            // We will get a Feed Polled Event and then a
            // Feed Retrieved event (assuming the feed is valid)
            SyndFeed feed = fetcher.retrieveFeed(feedUrl);
            if (feedModified) {
                System.err.println(feedUrl + " retrieved");
                System.err.println(feedUrl + " has a title: " + feed.getTitle() + " and contains "
                        + feed.getEntries().size() + " entries.");

                List fetchedFeeds = feed.getEntries();
                rssFeedBuffer.addAll(fetchedFeeds);
            }
        } catch (Exception ex) {
            System.out.println("ERROR: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    @Override
    public void resetOnFailure(Exception e) {
        // TODO Auto-generated method stub

    }

}

class FetcherEventListenerImpl implements FetcherListener {

    private final IPullBasedFeedClient feedClient;

    public FetcherEventListenerImpl(IPullBasedFeedClient feedClient) {
        this.feedClient = feedClient;
    }

    /**
     * @see com.sun.syndication.fetcher.FetcherListener#fetcherEvent(com.sun.syndication.fetcher.FetcherEvent)
     */
    public void fetcherEvent(FetcherEvent event) {
        String eventType = event.getEventType();
        if (FetcherEvent.EVENT_TYPE_FEED_POLLED.equals(eventType)) {
            System.err.println("\tEVENT: Feed Polled. URL = " + event.getUrlString());
        } else if (FetcherEvent.EVENT_TYPE_FEED_RETRIEVED.equals(eventType)) {
            System.err.println("\tEVENT: Feed Retrieved. URL = " + event.getUrlString());
            ((RSSFeedClient) feedClient).setFeedModified(true);
        } else if (FetcherEvent.EVENT_TYPE_FEED_UNCHANGED.equals(eventType)) {
            System.err.println("\tEVENT: Feed Unchanged. URL = " + event.getUrlString());
            ((RSSFeedClient) feedClient).setFeedModified(true);
        }
    }
}