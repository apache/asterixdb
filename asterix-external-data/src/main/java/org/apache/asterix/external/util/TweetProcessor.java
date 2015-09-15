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

import org.apache.asterix.external.library.java.JObjectUtil;
import twitter4j.Status;
import twitter4j.User;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableRecord;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;

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
