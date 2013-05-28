package edu.uci.ics.asterix.metadata.feeds;

import java.util.concurrent.atomic.AtomicInteger;

public class FeedActivityIdFactory {
	private static AtomicInteger id = new AtomicInteger();
	private static boolean isInitialized = false;

	public static boolean isInitialized() {
		return isInitialized;
	}

	public static void initialize(int initialId) {
		id.set(initialId);
		isInitialized = true;
	}

	public static int generateFeedActivityId() {
		return id.incrementAndGet();
	}

	public static int getMostRecentFeedActivityId() {
		return id.get();
	}
}