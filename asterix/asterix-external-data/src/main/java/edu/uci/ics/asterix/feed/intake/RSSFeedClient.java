package edu.uci.ics.asterix.feed.intake;

import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.sun.syndication.feed.synd.SyndEntryImpl;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.fetcher.FeedFetcher;
import com.sun.syndication.fetcher.FetcherEvent;
import com.sun.syndication.fetcher.FetcherListener;
import com.sun.syndication.fetcher.impl.FeedFetcherCache;
import com.sun.syndication.fetcher.impl.HashMapFeedInfoCache;
import com.sun.syndication.fetcher.impl.HttpURLFeedFetcher;

import edu.uci.ics.asterix.external.dataset.adapter.RSSFeedAdapter;

@SuppressWarnings("rawtypes")
public class RSSFeedClient  implements IFeedClient {

	private final String feedURL;
	private int timeInterval = 1;
	private Character delimiter = '|';
	private long id = 0;
	private String id_prefix;
	private boolean feedModified = false;
	private RSSFeedAdapter adapter;

	public boolean isFeedModified() {
		return feedModified;
	}

	public void setFeedModified(boolean feedModified) {
		this.feedModified = feedModified;
	}

	public RSSFeedClient(RSSFeedAdapter adapter, String feedURL,
			String id_prefix) {
		this.adapter = adapter;
		this.feedURL = feedURL;
		this.id_prefix = id_prefix;
	}

	private void initialize(Map<String, String> params) {
		if (params.get(adapter.KEY_INTERVAL) != null) {
			this.timeInterval = Integer.parseInt(params
					.get(adapter.KEY_INTERVAL));
		}
	}

	@Override
	public boolean next(List<String> feeds) {
		try {
			if (adapter.isStopRequested()) {
				return false;
			}
			if (adapter.isAlterRequested()) {
				initialize(adapter.getAlteredParams());
				adapter.postAlteration();
			}
			Thread.sleep(timeInterval * 1000);
			feeds.clear();
			try {
				getFeed(feeds);
			} catch (Exception te) {
				te.printStackTrace();
				System.out.println("Failed to get feed: " + te.getMessage());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	public String formFeedTuple(Object entry) {
		StringBuilder builder = new StringBuilder();
		builder.append(id_prefix + ":" + id);
		builder.append(delimiter);
		builder.append(((SyndEntryImpl) entry).getTitle());
		builder.append(delimiter);
		builder.append(((SyndEntryImpl) entry).getLink());
		id++;
		return new String(builder);
	}

	private void getFeed(List<String> feeds) {
		try {
			URL feedUrl = new URL(feedURL);
			FeedFetcherCache feedInfoCache = HashMapFeedInfoCache.getInstance();
			FeedFetcher fetcher = new HttpURLFeedFetcher(feedInfoCache);

			FetcherEventListenerImpl listener = new FetcherEventListenerImpl(
					this);
			fetcher.addFetcherEventListener(listener);
			System.err.println("Retrieving feed " + feedUrl);
			// Retrieve the feed.
			// We will get a Feed Polled Event and then a
			// Feed Retrieved event (assuming the feed is valid)
			SyndFeed feed = fetcher.retrieveFeed(feedUrl);
			if (feedModified) {
				System.err.println(feedUrl + " retrieved");
				System.err.println(feedUrl + " has a title: " + feed.getTitle()
						+ " and contains " + feed.getEntries().size()
						+ " entries.");

				List fetchedFeeds = feed.getEntries();
				Iterator feedIterator = fetchedFeeds.iterator();
				while (feedIterator.hasNext()) {
					SyndEntryImpl feedEntry = (SyndEntryImpl) feedIterator
							.next();
					String feedContent = formFeedTuple(feedEntry);
					feeds.add(escapeChars(feedContent));
					System.out.println(feedContent);
				}
			}
		} catch (Exception ex) {
			System.out.println("ERROR: " + ex.getMessage());
			ex.printStackTrace();
		}
	}

	private String escapeChars(String content) {
		if (content.contains("\n")) {
			return content.replace("\n", " ");
		}
		return content;
	}

}

class FetcherEventListenerImpl implements FetcherListener {

	private final IFeedClient feedClient;

	public FetcherEventListenerImpl(IFeedClient feedClient) {
		this.feedClient = feedClient;
	}

	/**
	 * @see com.sun.syndication.fetcher.FetcherListener#fetcherEvent(com.sun.syndication.fetcher.FetcherEvent)
	 */
	public void fetcherEvent(FetcherEvent event) {
		String eventType = event.getEventType();
		if (FetcherEvent.EVENT_TYPE_FEED_POLLED.equals(eventType)) {
			System.err.println("\tEVENT: Feed Polled. URL = "
					+ event.getUrlString());
		} else if (FetcherEvent.EVENT_TYPE_FEED_RETRIEVED.equals(eventType)) {
			System.err.println("\tEVENT: Feed Retrieved. URL = "
					+ event.getUrlString());
			((RSSFeedClient) feedClient).setFeedModified(true);
		} else if (FetcherEvent.EVENT_TYPE_FEED_UNCHANGED.equals(eventType)) {
			System.err.println("\tEVENT: Feed Unchanged. URL = "
					+ event.getUrlString());
			((RSSFeedClient) feedClient).setFeedModified(true);
		}
	}
}