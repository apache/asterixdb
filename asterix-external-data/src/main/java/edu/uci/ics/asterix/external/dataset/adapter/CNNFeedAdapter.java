package edu.uci.ics.asterix.external.dataset.adapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.feed.managed.adapter.IMutableFeedAdapter;

public class CNNFeedAdapter extends RSSFeedAdapter implements IDatasourceAdapter, IMutableFeedAdapter {

    private static final long serialVersionUID = 2523303758114582251L;
    private List<String> feedURLs = new ArrayList<String>();
    private String id_prefix = "";

    public static final String KEY_RSS_URL = "topic";
    public static final String KEY_INTERVAL = "interval";

    private static Map<String, String> topicFeeds = new HashMap<String, String>();

    public static final String TOP_STORIES = "topstories";
    public static final String WORLD = "world";
    public static final String US = "us";
    public static final String SPORTS = "sports";
    public static final String BUSINESS = "business";
    public static final String POLITICS = "politics";
    public static final String CRIME = "crime";
    public static final String TECHNOLOGY = "technology";
    public static final String HEALTH = "health";
    public static final String ENTERNTAINMENT = "entertainemnt";
    public static final String TRAVEL = "travel";
    public static final String LIVING = "living";
    public static final String VIDEO = "video";
    public static final String STUDENT = "student";
    public static final String POPULAR = "popular";
    public static final String RECENT = "recent";

    private void initTopics() {
        topicFeeds.put(TOP_STORIES, "http://rss.cnn.com/rss/cnn_topstories.rss");
        topicFeeds.put(WORLD, "http://rss.cnn.com/rss/cnn_world.rss");
        topicFeeds.put(US, "http://rss.cnn.com/rss/cnn_us.rss");
        topicFeeds.put(SPORTS, "http://rss.cnn.com/rss/si_topstories.rss");
        topicFeeds.put(BUSINESS, "http://rss.cnn.com/rss/money_latest.rss");
        topicFeeds.put(POLITICS, "http://rss.cnn.com/rss/cnn_allpolitics.rss");
        topicFeeds.put(CRIME, "http://rss.cnn.com/rss/cnn_crime.rss");
        topicFeeds.put(TECHNOLOGY, "http://rss.cnn.com/rss/cnn_tech.rss");
        topicFeeds.put(HEALTH, "http://rss.cnn.com/rss/cnn_health.rss");
        topicFeeds.put(ENTERNTAINMENT, "http://rss.cnn.com/rss/cnn_showbiz.rss");
        topicFeeds.put(LIVING, "http://rss.cnn.com/rss/cnn_living.rss");
        topicFeeds.put(VIDEO, "http://rss.cnn.com/rss/cnn_freevideo.rss");
        topicFeeds.put(TRAVEL, "http://rss.cnn.com/rss/cnn_travel.rss");
        topicFeeds.put(STUDENT, "http://rss.cnn.com/rss/cnn_studentnews.rss");
        topicFeeds.put(POPULAR, "http://rss.cnn.com/rss/cnn_mostpopular.rss");
        topicFeeds.put(RECENT, "http://rss.cnn.com/rss/cnn_latest.rss");
    }

    @Override
    public void configure(Map<String, String> arguments) throws Exception {
        configuration = arguments;
        String rssURLProperty = configuration.get(KEY_RSS_URL);
        if (rssURLProperty == null) {
            throw new IllegalArgumentException("no rss url provided");
        }
        initializeFeedURLs(rssURLProperty);
        configurePartitionConstraints();

    }

    private void initializeFeedURLs(String rssURLProperty) {
        feedURLs.clear();
        String[] rssTopics = rssURLProperty.split(",");
        initTopics();
        for (String topic : rssTopics) {
            String feedURL = topicFeeds.get(topic);
            if (feedURL == null) {
                throw new IllegalArgumentException(" unknown topic :" + topic + " please choose from the following "
                        + getValidTopics());
            }
            feedURLs.add(feedURL);
        }
    }

    private static String getValidTopics() {
        StringBuilder builder = new StringBuilder();
        for (String key : topicFeeds.keySet()) {
            builder.append(key);
            builder.append(" ");
        }
        return new String(builder);
    }

}
