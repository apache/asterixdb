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
package org.apache.asterix.external.parser;

import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.external.api.IDataParser;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.library.java.JObjectUtil;
import org.apache.asterix.external.util.Datatypes.Tweet;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableRecord;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import twitter4j.Status;
import twitter4j.User;

public class TweetParser implements IRecordDataParser<Status> {

    private IAObject[] mutableTweetFields;
    private IAObject[] mutableUserFields;
    private AMutableRecord mutableRecord;
    private AMutableRecord mutableUser;
    private final Map<String, Integer> userFieldNameMap = new HashMap<>();
    private final Map<String, Integer> tweetFieldNameMap = new HashMap<>();
    private RecordBuilder recordBuilder = new RecordBuilder();

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType recordType)
            throws HyracksDataException, IOException {
        initFieldNames(recordType);
        mutableUserFields = new IAObject[] { new AMutableString(null), new AMutableString(null), new AMutableInt32(0),
                new AMutableInt32(0), new AMutableString(null), new AMutableInt32(0) };
        mutableUser = new AMutableRecord((ARecordType) recordType.getFieldTypes()[tweetFieldNameMap.get(Tweet.USER)],
                mutableUserFields);

        mutableTweetFields = new IAObject[] { new AMutableString(null), mutableUser, new AMutableDouble(0),
                new AMutableDouble(0), new AMutableString(null), new AMutableString(null) };
        mutableRecord = new AMutableRecord(recordType, mutableTweetFields);

    }

    // Initialize the hashmap values for the field names and positions
    private void initFieldNames(ARecordType recordType) {
        String tweetFields[] = recordType.getFieldNames();
        for (int i = 0; i < tweetFields.length; i++) {
            tweetFieldNameMap.put(tweetFields[i], i);
            if (tweetFields[i].equals(Tweet.USER)) {
                IAType fieldType = recordType.getFieldTypes()[i];
                if (fieldType.getTypeTag() == ATypeTag.RECORD) {
                    String userFields[] = ((ARecordType) fieldType).getFieldNames();
                    for (int j = 0; j < userFields.length; j++) {
                        userFieldNameMap.put(userFields[j], j);
                    }
                }

            }
        }
    }

    @Override
    public void parse(IRawRecord<? extends Status> record, DataOutput out) throws Exception {
        Status tweet = record.get();
        User user = tweet.getUser();
        // Tweet user data
        ((AMutableString) mutableUserFields[userFieldNameMap.get(Tweet.SCREEN_NAME)])
                .setValue(JObjectUtil.getNormalizedString(user.getScreenName()));
        ((AMutableString) mutableUserFields[userFieldNameMap.get(Tweet.LANGUAGE)])
                .setValue(JObjectUtil.getNormalizedString(user.getLang()));
        ((AMutableInt32) mutableUserFields[userFieldNameMap.get(Tweet.FRIENDS_COUNT)]).setValue(user.getFriendsCount());
        ((AMutableInt32) mutableUserFields[userFieldNameMap.get(Tweet.STATUS_COUNT)]).setValue(user.getStatusesCount());
        ((AMutableString) mutableUserFields[userFieldNameMap.get(Tweet.NAME)])
                .setValue(JObjectUtil.getNormalizedString(user.getName()));
        ((AMutableInt32) mutableUserFields[userFieldNameMap.get(Tweet.FOLLOWERS_COUNT)])
                .setValue(user.getFollowersCount());

        // Tweet data
        ((AMutableString) mutableTweetFields[tweetFieldNameMap.get(Tweet.ID)]).setValue(String.valueOf(tweet.getId()));

        int userPos = tweetFieldNameMap.get(Tweet.USER);
        for (int i = 0; i < mutableUserFields.length; i++) {
            ((AMutableRecord) mutableTweetFields[userPos]).setValueAtPos(i, mutableUserFields[i]);
        }
        if (tweet.getGeoLocation() != null) {
            ((AMutableDouble) mutableTweetFields[tweetFieldNameMap.get(Tweet.LATITUDE)])
                    .setValue(tweet.getGeoLocation().getLatitude());
            ((AMutableDouble) mutableTweetFields[tweetFieldNameMap.get(Tweet.LONGITUDE)])
                    .setValue(tweet.getGeoLocation().getLongitude());
        } else {
            ((AMutableDouble) mutableTweetFields[tweetFieldNameMap.get(Tweet.LATITUDE)]).setValue(0);
            ((AMutableDouble) mutableTweetFields[tweetFieldNameMap.get(Tweet.LONGITUDE)]).setValue(0);
        }
        ((AMutableString) mutableTweetFields[tweetFieldNameMap.get(Tweet.CREATED_AT)])
                .setValue(JObjectUtil.getNormalizedString(tweet.getCreatedAt().toString()));
        ((AMutableString) mutableTweetFields[tweetFieldNameMap.get(Tweet.MESSAGE)])
                .setValue(JObjectUtil.getNormalizedString(tweet.getText()));

        for (int i = 0; i < mutableTweetFields.length; i++) {
            mutableRecord.setValueAtPos(i, mutableTweetFields[i]);
        }
        recordBuilder.reset(mutableRecord.getType());
        recordBuilder.init();
        IDataParser.writeRecord(mutableRecord, out, recordBuilder);
    }

    @Override
    public Class<? extends Status> getRecordClass() {
        return Status.class;
    }

}
