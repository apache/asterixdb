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

import org.apache.asterix.common.exceptions.AsterixException;
import twitter4j.FilterQuery;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TwitterUtil {

    private static Logger LOGGER = Logger.getLogger(TwitterUtil.class.getName());

    public static class ConfigurationConstants {
        public static final String KEY_LOCATIONS = "locations"; // locations to track
        public static final String KEY_LOCATION = "location"; // location to track
        public static final String LOCATION_US = "US";
        public static final String LOCATION_EU = "Europe";
        public static final String LANGUAGES = "languages"; // languages to track
        public static final String TRACK = "keywords"; // terms to track
        public static final String  FILTER_LEVEL = "filter-level";
    }

    public static class GeoConstants {
        public static final double[][] US = new double[][] { { -124.848974, 24.396308 }, { -66.885444, 49.384358 } };
        public static final double[][] EU = new double[][]{{-29.7,36.7},{79.2,72.0}};
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
     *
     * E.g., for EU and US, we would use -29.7, 79.2, 36.7, 72.0; -124.848974,
     *      -66.885444, 24.396308, 49.384358.
     *
     * @param locationValue
     *          String value of the location coordinates or names (comma-separated)
     * @return
     * @throws AsterixException
     */
    public static double[][] getBoundingBoxes(String locationValue) throws AsterixException {
        double[][] locations = null;

        String coordRegex = "^((((\\-?\\d+\\.\\d+),\\s*){3}(\\-?\\d+\\.\\d+)|\\w+);\\s*)*(((\\-?\\d+\\.\\d+),\\s*){3}(\\-?\\d+\\.\\d+)|\\w+)$";
        Pattern p = Pattern.compile(coordRegex);
        Matcher m = p.matcher(locationValue);

        if (m.matches()) {
            String[] locationStrings = locationValue.trim().split(";\\s*");
            locations = new double[locationStrings.length*2][2];

            for(int i=0; i<locationStrings.length; i++) {
                if (locationStrings[i].contains(",")) {
                    String[] coordinatesString = locationStrings[i].split(",");
                    for(int k=0; k < 2; k++) {
                        for (int l = 0; l < 2; l++) {
                            try {
                                locations[2 * i + k][l] = Double.parseDouble(coordinatesString[2 * k + l]);
                            } catch (NumberFormatException ne) {
                                throw new AsterixException("Incorrect coordinate value " + coordinatesString[2 * k + l]);
                            }
                        }
                    }
                } else if (GeoConstants.boundingBoxes.containsKey(locationStrings[i])) {
                    // Only add known locations
                    double loc[][] = GeoConstants.boundingBoxes.get(locationStrings[i]);
                    for(int k=0; k < 2; k++) {
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
                keywords = new String[] {trackValue};
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
                languages = new String[] {langValue};
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
        if(filterQuery != null) {
            String filterValue = configuration.get(ConfigurationConstants.FILTER_LEVEL);
            if (filterValue!=null) {
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
            if (LOGGER.isLoggable(Level.WARNING)) {
                StringBuilder builder = new StringBuilder();
                builder.append("Twitter Adapter requires the following config parameters\n");
                builder.append(AuthenticationConstants.OAUTH_CONSUMER_KEY + "\n");
                builder.append(AuthenticationConstants.OAUTH_CONSUMER_SECRET + "\n");
                builder.append(AuthenticationConstants.OAUTH_ACCESS_TOKEN + "\n");
                builder.append(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET + "\n");
                LOGGER.warning(builder.toString());
                LOGGER.warning("Unable to configure Twitter adapter due to incomplete/incorrect authentication credentials");
                LOGGER.warning("For details on how to obtain OAuth authentication token, visit https://dev.twitter.com/oauth/overview/application-owner-access-tokens");
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
        String oAuthConsumerKey = configuration.get(AuthenticationConstants.OAUTH_CONSUMER_KEY);
        String oAuthConsumerSecret = configuration.get(AuthenticationConstants.OAUTH_CONSUMER_SECRET);
        String oAuthAccessToken = configuration.get(AuthenticationConstants.OAUTH_ACCESS_TOKEN);
        String oAuthAccessTokenSecret = configuration.get(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET);

        cb.setOAuthConsumerKey(oAuthConsumerKey);
        cb.setOAuthConsumerSecret(oAuthConsumerSecret);
        cb.setOAuthAccessToken(oAuthAccessToken);
        cb.setOAuthAccessTokenSecret(oAuthAccessTokenSecret);
        return cb;
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
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("unable to load authentication credentials from auth.properties file"
                        + "credential information will be obtained from adapter's configuration");
            }
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

}
