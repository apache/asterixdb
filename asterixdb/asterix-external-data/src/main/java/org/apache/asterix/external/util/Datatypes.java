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

public class Datatypes {

    /*
        The following assumes this DDL (but ignoring the field name orders):
        create type TwitterUser if not exists as open{
            screen_name: string,
            language: string,
            friends_count: int32,
            status_count: int32,
            name: string,
            followers_count: int32
        };
        create type Tweet if not exists as open{
            id: string,
            user: TwitterUser,
            latitude:double,
            longitude:double,
            created_at:string,
            message_text:string
        };
    */
    public static class Tweet {
        public static final String ID = "id";
        public static final String USER = "user";
        public static final String GEOLOCATION = "geo";
        public static final String CREATED_AT = "created_at";
        public static final String TEXT = "text";
        public static final String COUNTRY = "country";
        public static final String PLACE = "place";
        public static final String SOURCE = "source";
        public static final String TRUNCATED = "truncated";
        public static final String IN_REPLY_TO_STATUS_ID = "in_reply_to_status_id";
        public static final String IN_REPLY_TO_USER_ID = "in_reply_to_user_id";
        public static final String IN_REPLY_TO_SCREENNAME = "in_reply_to_screen_name";
        public static final String FAVORITED = "favorited";
        public static final String RETWEETED = "retweeted";
        public static final String FAVORITE_COUNT = "favorite_count";
        public static final String RETWEET_COUNT = "retweet_count";
        public static final String CONTRIBUTORS = "contributors";
        public static final String LANGUAGE = "lang";
        public static final String FILTER_LEVEL = "filter_level";
        public static final String TIMESTAMP_MS = "timestamp_ms";
        public static final String IS_QUOTE_STATUS = "is_quote_status";
        // in API but not int JSON
        public static final String SENSITIVE = "sensitive";
        public static final String RETWEETED_BY_ME = "retweeted_by_me";
        public static final String CURRENT_USER_RETWEET_ID = "current_user_retweet_id";

        // consistency consider
        public static final String MESSAGE = "message_text";
        public static final String LATITUDE = "latitude";
        public static final String LONGITUDE = "longitude";
        // User fields (for the sub record "user")
        public static final String SCREEN_NAME = "screen_name";
        public static final String USER_PREFERRED_LANGUAGE = "user_preferred_language";
        public static final String FRIENDS_COUNT = "friends_count";
        public static final String STATUS_COUNT = "status_count";
        public static final String NAME = "name";
        public static final String FOLLOWERS_COUNT = "followers_count";

    }

    public static final class Tweet_Place {
        public static final String ID = "id";
        public static final String URL = "url";
        public static final String PLACE_TYPE = "place_type";
        public static final String NAME = "name";
        public static final String FULL_NAME = "full_name";
        public static final String COUNTRY_CODE = "country_code";
        public static final String COUNTRY = "country";
        public static final String BOUNDING_BOX = "bounding_box";
        public static final String ATTRIBUTES = "attributes";

        private Tweet_Place() {
        }
    }

    public static final class Tweet_User {
        public static final String ID = "id";
        public static final String NAME = "name";
        public static final String SCREEN_NAME = "screen_name";
        public static final String LOCATION = "location";
        public static final String DESCRIPTION = "description";
        public static final String CONTRIBUTORS_ENABLED = "contributors_enabled";
        public static final String PROFILE_IMAGE_URL = "profile_image_url";
        public static final String PROFILE_IMAGE_URL_HTTPS = "profile_image_url_https";
        public static final String URL = "url";
        public static final String PROTECTED = "protected";
        public static final String FOLLOWERS_COUNT = "followers_count";
        public static final String PROFILE_BACKGROUND_COLOR = "profile_background_color";
        public static final String PROFILE_TEXT_COLOR = "profile_text_color";
        public static final String PROFILE_LINK_COLOR = "profile_link_color";
        public static final String PROFILE_SIDEBAR_FILL_COLOR = "profile_sidebar_fill_color";
        public static final String PROFILE_SIDEBAR_BORDER_COLOR = "profile_sidebar_border_color";
        public static final String PROFILE_USE_BACKGROUND_IMAGE = "profile_use_background_image";
        public static final String DEFAULT_PROFILE = "default_profile";
        public static final String DEFAULT_PROFILE_IMAGE = "default_profile_image";
        public static final String FRIENDS_COUNT = "friends_count";
        public static final String CREATED_AT = "CREATED_AT";
        public static final String FAVOURITES_COUNT = "favourites_count";
        public static final String UTC_OFFSET = "utc_offset";
        public static final String TIME_ZONE = "time_zone";
        public static final String PROFILE_BACKGROUND_IMAGE_URL = "profile_background_image_url";
        public static final String PROFILE_BACKGROUND_IMAGE_URL_HTTPS = "profile_background_image_url_https";
        public static final String PROFILE_BANNER_URL = "profile_banner_url";
        public static final String LANG = "lang";
        public static final String STATUSES_COUNT = "statuses_count";
        public static final String GEO_ENABLED = "geo_enabled";
        public static final String VERIFIED = "verified";
        public static final String IS_TRANSLATOR = "is_translator";
        public static final String LISTED_COUNT = "listed_count";
        public static final String FOLLOW_REQUEST_SENT = "follow_request_sent";
        // skip Entities, attrs in API but not in JSON is as below
        public static final String WITHHELD_IN_COUNTRIES = "withheld_in_countries";
        public static final String BIGGER_PROFILE_IMAGE_URL = "bigger_profile_image_url";
        public static final String MINI_PROFILE_IMAGE_URL = "mini_profile_image_url";
        public static final String ORIGINAL_PROFILE_IMAGE_URL = "original_profile_image_url";
        public static final String SHOW_ALL_INLINE_MEDIA = "show_all_inline_media";
        public static final String PROFILE_BANNER_RETINA_URL = "profile_banner_retina_url";
        public static final String PROFILE_BANNER_IPAD_URL = "profile_banner_ipad_url";
        public static final String PROFILE_BANNER_IPAD_RETINA_URL = "profile_banner_ipad_retina_url";
        public static final String PROFILE_BANNER_MOBILE_URL = "profile_banner_mobile_url";
        public static final String PROFILE_BANNER_MOBILE_RETINA_URL = "profile_banner_mobile_retina_url";
        public static final String PROFILE_BACKGROUND_TILED = "profile_background_tiled";

        private Tweet_User() {
        }
    }
}
