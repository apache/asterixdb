package edu.uci.ics.asterix.external.util;

public class Datatypes {

    public static final class Tweet {
        public static final String ID = "id";
        public static final String USER = "user";
        public static final String MESSAGE = "message_text";
        public static final String LATITUDE = "latitude";
        public static final String LONGITUDE = "longitude";
        public static final String CREATED_AT = "created_at";
        public static final String SCREEN_NAME = "screen_name";
        public static final String COUNTRY = "country";
    }

    public static final class ProcessedTweet {
        public static final String USER_NAME = "user_name";
        public static final String LOCATION = "location";
        public static final String TOPICS = "topics";
    }
}
