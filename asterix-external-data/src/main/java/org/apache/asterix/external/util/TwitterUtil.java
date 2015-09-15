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
import java.util.logging.Logger;
import java.util.logging.Level;

import twitter4j.FilterQuery;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import org.apache.asterix.common.exceptions.AsterixException;

public class TwitterUtil {

    private static Logger LOGGER = Logger.getLogger(TwitterUtil.class.getName());

    public static class ConfigurationConstants {
        public static final String KEY_LOCATION = "location";
        public static final String LOCATION_US = "US";
    }

    public static class GeoConstants {
        public static Map<String, double[][]> boundingBoxes = initializeBoundingBoxes();
        public static final double[][] US = new double[][] { { -124.848974, 24.396308 }, { -66.885444, 49.384358 } };
    }

    private static Map<String, double[][]> initializeBoundingBoxes() {
        Map<String, double[][]> boundingBoxes = new HashMap<String, double[][]>();
        boundingBoxes.put(ConfigurationConstants.LOCATION_US, new double[][] { { -124.848974, 24.396308 },
                { -66.885444, 49.384358 } });
        return boundingBoxes;
    }

    public static FilterQuery getFilterQuery(Map<String, String> configuration) throws AsterixException {
        String locationValue = configuration.get(ConfigurationConstants.KEY_LOCATION);
        double[][] locations = null;
        if (locationValue != null) {
            if (locationValue.contains(",")) {
                String[] coordinatesString = locationValue.trim().split(",");
                locations = new double[2][2];
                for (int i = 0; i < 2; i++) {
                    for (int j = 0; j < 2; j++) {
                        try {
                            locations[i][j] = Double.parseDouble(coordinatesString[2 * i + j]);
                        } catch (NumberFormatException ne) {
                            throw new AsterixException("Incorrect coordinate value " + coordinatesString[2 * i + j]);
                        }
                    }
                }
            } else {
                locations = GeoConstants.boundingBoxes.get(locationValue);
            }
            if (locations != null) {
                FilterQuery filterQuery = new FilterQuery();
                filterQuery.locations(locations);
                return filterQuery;
            }
        }
        return null;

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
