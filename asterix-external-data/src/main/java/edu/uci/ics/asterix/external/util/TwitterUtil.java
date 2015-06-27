package edu.uci.ics.asterix.external.util;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import twitter4j.FilterQuery;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class TwitterUtil {

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
        TwitterFactory tf = new TwitterFactory(cb.build());
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
            throw new AsterixException("Incorrect configuration! unable to load authentication credentials "
                    + e.getMessage());
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
