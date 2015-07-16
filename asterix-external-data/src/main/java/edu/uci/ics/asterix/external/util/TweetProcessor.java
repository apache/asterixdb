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

import edu.uci.ics.asterix.external.library.java.JObjectUtil;
import twitter4j.Status;
import twitter4j.User;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;

public class TweetProcessor {

    private IAObject[] mutableTweetFields;
    private IAObject[] mutableUserFields;
    private AMutableRecord mutableRecord;
    private AMutableRecord mutableUser;

    public TweetProcessor(ARecordType recordType) {
        mutableUserFields = new IAObject[] { new AMutableString(null), new AMutableString(null), new AMutableInt32(0),
                new AMutableInt32(0), new AMutableString(null), new AMutableInt32(0) };
        mutableUser = new AMutableRecord((ARecordType) recordType.getFieldTypes()[1], mutableUserFields);

        mutableTweetFields = new IAObject[] { new AMutableString(null), mutableUser, new AMutableDouble(0),
                new AMutableDouble(0), new AMutableString(null), new AMutableString(null) };
        mutableRecord = new AMutableRecord(recordType, mutableTweetFields);

    }

    public AMutableRecord processNextTweet(Status tweet) {
        User user = tweet.getUser();
        ((AMutableString) mutableUserFields[0]).setValue(JObjectUtil.getNormalizedString(user.getScreenName()));
        ((AMutableString) mutableUserFields[1]).setValue(JObjectUtil.getNormalizedString(user.getLang()));
        ((AMutableInt32) mutableUserFields[2]).setValue(user.getFriendsCount());
        ((AMutableInt32) mutableUserFields[3]).setValue(user.getStatusesCount());
        ((AMutableString) mutableUserFields[4]).setValue(JObjectUtil.getNormalizedString(user.getName()));
        ((AMutableInt32) mutableUserFields[5]).setValue(user.getFollowersCount());

        ((AMutableString) mutableTweetFields[0]).setValue(tweet.getId() + "");

        for (int i = 0; i < 6; i++) {
            ((AMutableRecord) mutableTweetFields[1]).setValueAtPos(i, mutableUserFields[i]);
        }
        if (tweet.getGeoLocation() != null) {
            ((AMutableDouble) mutableTweetFields[2]).setValue(tweet.getGeoLocation().getLatitude());
            ((AMutableDouble) mutableTweetFields[3]).setValue(tweet.getGeoLocation().getLongitude());
        } else {
            ((AMutableDouble) mutableTweetFields[2]).setValue(0);
            ((AMutableDouble) mutableTweetFields[3]).setValue(0);
        }
        ((AMutableString) mutableTweetFields[4]).setValue(JObjectUtil.getNormalizedString(
                tweet.getCreatedAt().toString()));
        ((AMutableString) mutableTweetFields[5]).setValue(JObjectUtil.getNormalizedString(tweet.getText()));

        for (int i = 0; i < 6; i++) {
            mutableRecord.setValueAtPos(i, mutableTweetFields[i]);
        }

        return mutableRecord;

    }

    public AMutableRecord getMutableRecord() {
        return mutableRecord;
    }

}
