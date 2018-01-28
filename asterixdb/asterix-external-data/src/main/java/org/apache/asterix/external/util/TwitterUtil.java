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
package org.apache.asterix.external.util;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import twitter4j.DirectMessage;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.UserList;
import twitter4j.UserStreamListener;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    public static class ConfigurationConstants {
        public static final String KEY_LOCATIONS = "locations"; // locations to track
        public static final String KEY_LOCATION = "location"; // location to track
        public static final String LOCATION_US = "US";
        public static final String LOCATION_EU = "Europe";
        public static final String LANGUAGES = "languages"; // languages to track
        public static final String TRACK = "keywords"; // terms to track
        public static final String FILTER_LEVEL = "filter-level";
    }

    public static class GeoConstants {
        private static final double[][] US = new double[][] { { -124.848974, 24.396308 }, { -66.885444, 49.384358 } };
        private static final double[][] EU = new double[][] { { -29.7, 36.7 }, { 79.2, 72.0 } };
        public static Map<String, double[][]> boundingBoxes = initializeBoundingBoxes();
    }

    private static Map<String, double[][]> initializeBoundingBoxes() {
        Map<String, double[][]> boundingBoxes = new HashMap<String, double[][]>();
        boundingBoxes.put(ConfigurationConstants.LOCATION_US, GeoConstants.US);
        boundingBoxes.put(ConfigurationConstants.LOCATION_EU, GeoConstants.EU);
        return boundingBoxes;
    }

    /**
     * Gets more than one bounding box from a sequences of coordinates
     * (following Twitter formats) + predefined location names, as US and EU.
     * E.g., for EU and US, we would use -29.7, 79.2, 36.7, 72.0; -124.848974,
     * -66.885444, 24.396308, 49.384358.
     *
     * @param locationValue
     *            String value of the location coordinates or names (comma-separated)
     * @return
     * @throws AsterixException
     */
    public static double[][] getBoundingBoxes(String locationValue) throws AsterixException {
        double[][] locations = null;

        String coordRegex =
                "^((((\\-?\\d+\\.\\d+),\\s*){3}(\\-?\\d+\\.\\d+)|\\w+);\\s*)*(((\\-?\\d+\\.\\d+),\\s*){3}(\\-?\\d+\\.\\d+)|\\w+)$";
        Pattern p = Pattern.compile(coordRegex);
        Matcher m = p.matcher(locationValue);

        if (m.matches()) {
            String[] locationStrings = locationValue.trim().split(";\\s*");
            locations = new double[locationStrings.length * 2][2];

            for (int i = 0; i < locationStrings.length; i++) {
                if (locationStrings[i].contains(",")) {
                    String[] coordinatesString = locationStrings[i].split(",");
                    for (int k = 0; k < 2; k++) {
                        for (int l = 0; l < 2; l++) {
                            try {
                                locations[2 * i + k][l] = Double.parseDouble(coordinatesString[2 * k + l]);
                            } catch (NumberFormatException ne) {
                                throw new AsterixException(
                                        "Incorrect coordinate value " + coordinatesString[2 * k + l]);
                            }
                        }
                    }
                } else if (GeoConstants.boundingBoxes.containsKey(locationStrings[i])) {
                    // Only add known locations
                    double loc[][] = GeoConstants.boundingBoxes.get(locationStrings[i]);
                    for (int k = 0; k < 2; k++) {
                        for (int l = 0; l < 2; l++) {
                            locations[2 * i + k][l] = loc[k][l];
                        }
                    }
                }
            }
        }
        return locations;
    }

    public static FilterQuery getFilterQuery(Map<String, String> configuration) throws AsterixException {
        String locationValue = null;

        // For backward compatibility
        if (configuration.containsKey(ConfigurationConstants.KEY_LOCATIONS)) {
            locationValue = configuration.get(ConfigurationConstants.KEY_LOCATIONS);
        } else {
            locationValue = configuration.get(ConfigurationConstants.KEY_LOCATION);
        }
        String langValue = configuration.get(ConfigurationConstants.LANGUAGES);
        String trackValue = configuration.get(ConfigurationConstants.TRACK);

        FilterQuery filterQuery = null;

        // Terms to track
        if (trackValue != null) {
            String keywords[] = null;
            filterQuery = new FilterQuery();
            if (trackValue.contains(",")) {
                keywords = trackValue.trim().split(",\\s*");
            } else {
                keywords = new String[] { trackValue };
            }
            filterQuery = filterQuery.track(keywords);
        }

        // Language filtering parameter
        if (langValue != null) {
            if (filterQuery == null) {
                filterQuery = new FilterQuery();
            }
            String languages[];
            if (langValue.contains(",")) {
                languages = langValue.trim().split(",\\s*");
            } else {
                languages = new String[] { langValue };
            }
            filterQuery = filterQuery.language(languages);
        }

        // Location filtering parameter
        if (locationValue != null) {
            double[][] locations = getBoundingBoxes(locationValue);
            if (locations != null) {
                if (filterQuery == null) {
                    filterQuery = new FilterQuery();
                }
                filterQuery = filterQuery.locations(locations);
            }
        }

        // Filtering level: none, low or medium (defaul=none)
        if (filterQuery != null) {
            String filterValue = configuration.get(ConfigurationConstants.FILTER_LEVEL);
            if (filterValue != null) {
                filterQuery = filterQuery.filterLevel(filterValue);
            }
        }

        return filterQuery;

    }

    public static Twitter getTwitterService(Map<String, String> configuration) {
        ConfigurationBuilder cb = getAuthConfiguration(configuration);
        TwitterFactory tf = null;
        try {
            tf = new TwitterFactory(cb.build());
        } catch (Exception e) {
            if (LOGGER.isWarnEnabled()) {
                StringBuilder builder = new StringBuilder();
                builder.append("Twitter Adapter requires the following config parameters\n");
                builder.append(AuthenticationConstants.OAUTH_CONSUMER_KEY + "\n");
                builder.append(AuthenticationConstants.OAUTH_CONSUMER_SECRET + "\n");
                builder.append(AuthenticationConstants.OAUTH_ACCESS_TOKEN + "\n");
                builder.append(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET + "\n");
                LOGGER.warn(builder.toString());
                LOGGER.warn(
                        "Unable to configure Twitter adapter due to incomplete/incorrect authentication credentials");
                LOGGER.warn(
                        "For details on how to obtain OAuth authentication token, visit https://dev.twitter.com/oauth"
                                + "/overview/application-owner-access-tokens");
            }
        }
        Twitter twitter = tf.getInstance();
        return twitter;
    }

    public static TwitterStream getTwitterStream(Map<String, String> configuration) {
        ConfigurationBuilder cb = getAuthConfiguration(configuration);
        TwitterStreamFactory factory = new TwitterStreamFactory(cb.build());
        return factory.getInstance();
    }

    private static ConfigurationBuilder getAuthConfiguration(Map<String, String> configuration) {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setJSONStoreEnabled(true);
        String oAuthConsumerKey = configuration.get(AuthenticationConstants.OAUTH_CONSUMER_KEY);
        String oAuthConsumerSecret = configuration.get(AuthenticationConstants.OAUTH_CONSUMER_SECRET);
        String oAuthAccessToken = configuration.get(AuthenticationConstants.OAUTH_ACCESS_TOKEN);
        String oAuthAccessTokenSecret = configuration.get(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET);

        cb.setOAuthConsumerKey(oAuthConsumerKey);
        cb.setOAuthConsumerSecret(oAuthConsumerSecret);
        cb.setOAuthAccessToken(oAuthAccessToken);
        cb.setOAuthAccessTokenSecret(oAuthAccessTokenSecret);
        configureProxy(cb, configuration);
        return cb;
    }

    private static void configureProxy(ConfigurationBuilder cb, Map<String, String> configuration) {
        final String httpProxyHost = configuration.get(ExternalDataConstants.KEY_HTTP_PROXY_HOST);
        final String httpProxyPort = configuration.get(ExternalDataConstants.KEY_HTTP_PROXY_PORT);
        if (httpProxyHost != null && httpProxyPort != null) {
            cb.setHttpProxyHost(httpProxyHost);
            cb.setHttpProxyPort(Integer.parseInt(httpProxyPort));
            final String httpProxyUser = configuration.get(ExternalDataConstants.KEY_HTTP_PROXY_USER);
            final String httpProxyPassword = configuration.get(ExternalDataConstants.KEY_HTTP_PROXY_PASSWORD);
            if (httpProxyUser != null && httpProxyPassword != null) {
                cb.setHttpProxyUser(httpProxyUser);
                cb.setHttpProxyPassword(httpProxyPassword);
            }
        }
    }

    public static void initializeConfigurationWithAuthInfo(Map<String, String> configuration) throws AsterixException {
        String authMode = configuration.get(AuthenticationConstants.AUTHENTICATION_MODE);
        if (authMode == null) {
            authMode = AuthenticationConstants.AUTHENTICATION_MODE_FILE;
        }
        try {
            switch (authMode) {
                case AuthenticationConstants.AUTHENTICATION_MODE_FILE:
                    Properties prop = new Properties();
                    String authFile = configuration.get(AuthenticationConstants.OAUTH_AUTHENTICATION_FILE);
                    if (authFile == null) {
                        authFile = AuthenticationConstants.DEFAULT_AUTH_FILE;
                    }
                    InputStream in = TwitterUtil.class.getResourceAsStream(authFile);
                    prop.load(in);
                    in.close();
                    configuration.put(AuthenticationConstants.OAUTH_CONSUMER_KEY,
                            prop.getProperty(AuthenticationConstants.OAUTH_CONSUMER_KEY));
                    configuration.put(AuthenticationConstants.OAUTH_CONSUMER_SECRET,
                            prop.getProperty(AuthenticationConstants.OAUTH_CONSUMER_SECRET));
                    configuration.put(AuthenticationConstants.OAUTH_ACCESS_TOKEN,
                            prop.getProperty(AuthenticationConstants.OAUTH_ACCESS_TOKEN));
                    configuration.put(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET,
                            prop.getProperty(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET));
                    break;
                case AuthenticationConstants.AUTHENTICATION_MODE_EXPLICIT:
                    break;
            }
        } catch (Exception e) {
            LOGGER.warn("unable to load authentication credentials from auth.properties file"
                    + "credential information will be obtained from adapter's configuration");
        }
    }

    public static final class AuthenticationConstants {
        public static final String OAUTH_CONSUMER_KEY = "consumer.key";
        public static final String OAUTH_CONSUMER_SECRET = "consumer.secret";
        public static final String OAUTH_ACCESS_TOKEN = "access.token";
        public static final String OAUTH_ACCESS_TOKEN_SECRET = "access.token.secret";
        public static final String OAUTH_AUTHENTICATION_FILE = "authentication.file";
        public static final String AUTHENTICATION_MODE = "authentication.mode";
        public static final String AUTHENTICATION_MODE_FILE = "file";
        public static final String AUTHENTICATION_MODE_EXPLICIT = "explicit";
        public static final String DEFAULT_AUTH_FILE = "/feed/twitter/auth.properties"; // default authentication file
    }

    public static final class SearchAPIConstants {
        public static final String QUERY = "query";
        public static final String INTERVAL = "interval";
    }

    public static UserTweetsListener getUserTweetsListener() {
        return new UserTweetsListener();
    }

    public static TweetListener getTweetListener() {
        return new TweetListener();
    }

    public static class UserTweetsListener implements UserStreamListener {

        private LinkedBlockingQueue<String> inputQ;

        public void setInputQ(LinkedBlockingQueue<String> inputQ) {
            this.inputQ = inputQ;
        }

        @Override
        public void onDeletionNotice(long l, long l1) {
            //do nothing
        }

        @Override
        public void onFriendList(long[] longs) {
            //do nothing
        }

        @Override
        public void onFavorite(User user, User user1, Status status) {
            //do nothing
        }

        @Override
        public void onUnfavorite(User user, User user1, Status status) {
            //do nothing
        }

        @Override
        public void onFollow(User user, User user1) {
            //do nothing
        }

        @Override
        public void onUnfollow(User user, User user1) {
            //do nothing
        }

        @Override
        public void onDirectMessage(DirectMessage directMessage) {
            //do nothing
        }

        @Override
        public void onUserListMemberAddition(User user, User user1, UserList userList) {
            //do nothing
        }

        @Override
        public void onUserListMemberDeletion(User user, User user1, UserList userList) {
            //do nothing
        }

        @Override
        public void onUserListSubscription(User user, User user1, UserList userList) {
            //do nothing
        }

        @Override
        public void onUserListUnsubscription(User user, User user1, UserList userList) {
            //do nothing
        }

        @Override
        public void onUserListCreation(User user, UserList userList) {
            //do nothing
        }

        @Override
        public void onUserListUpdate(User user, UserList userList) {
            //do nothing
        }

        @Override
        public void onUserListDeletion(User user, UserList userList) {
            //do nothing
        }

        @Override
        public void onUserProfileUpdate(User user) {
            //do nothing
        }

        @Override
        public void onUserSuspension(long l) {
            //do nothing
        }

        @Override
        public void onUserDeletion(long l) {
            //do nothing
        }

        @Override
        public void onBlock(User user, User user1) {
            //do nothing
        }

        @Override
        public void onUnblock(User user, User user1) {
            //do nothing
        }

        @Override
        public void onStatus(Status status) {
            String jsonTweet = TwitterObjectFactory.getRawJSON(status);
            inputQ.add(jsonTweet);
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            //do nothing
        }

        @Override
        public void onTrackLimitationNotice(int i) {
            //do nothing
        }

        @Override
        public void onScrubGeo(long l, long l1) {
            //do nothing
        }

        @Override
        public void onStallWarning(StallWarning stallWarning) {
            //do nothing
        }

        @Override
        public void onException(Exception e) {
            //do nothing
        }
    }

    public static class TweetListener implements StatusListener {

        private LinkedBlockingQueue<String> inputQ;

        public void setInputQ(LinkedBlockingQueue<String> inputQ) {
            this.inputQ = inputQ;
        }

        @Override
        public void onStatus(Status tweet) {
            String jsonTweet = TwitterObjectFactory.getRawJSON(tweet);
            inputQ.add(jsonTweet);
        }

        @Override
        public void onException(Exception arg0) {
            // do nothing
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice arg0) {
            // do nothing
        }

        @Override
        public void onScrubGeo(long arg0, long arg1) {
            // do nothing
        }

        @Override
        public void onStallWarning(StallWarning arg0) {
            // do nothing
        }

        @Override
        public void onTrackLimitationNotice(int arg0) {
            // do nothing
        }
    }

}
