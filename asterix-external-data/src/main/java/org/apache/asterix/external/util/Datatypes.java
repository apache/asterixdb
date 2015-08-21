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
