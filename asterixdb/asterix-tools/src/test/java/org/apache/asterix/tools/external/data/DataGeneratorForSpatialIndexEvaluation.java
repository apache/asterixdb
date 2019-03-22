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

package org.apache.asterix.tools.external.data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.asterix.external.generator.DataGenerator;

public class DataGeneratorForSpatialIndexEvaluation {

    private static final String DUMMY_SIZE_ADJUSTER =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    private RandomDateGenerator randDateGen;

    private RandomNameGenerator randNameGen;

    private RandomMessageGenerator randMessageGen;

    private RandomLocationGenerator randLocationGen;

    private LocationGeneratorFromOpenStreetMapData locationGenFromOpenStreetMapData;

    private Random random = new Random();

    private TwitterUser twUser = new TwitterUser();

    private TweetMessage twMessage = new TweetMessage();

    public DataGeneratorForSpatialIndexEvaluation(InitializationInfo info) {
        initialize(info, null, 0);
    }

    public DataGeneratorForSpatialIndexEvaluation(InitializationInfo info, String openStreetMapFilePath,
            int locationSampleInterval) {
        initialize(info, openStreetMapFilePath, locationSampleInterval);
    }

    public class TweetMessageIterator implements Iterator<TweetMessage> {

        private int duration;
        private final GULongIDGenerator idGen;
        private long startTime = 0;

        public TweetMessageIterator(int duration, GULongIDGenerator idGen) {
            this.duration = duration;
            this.idGen = idGen;
            this.startTime = System.currentTimeMillis();
        }

        @Override
        public boolean hasNext() {
            if (duration == 0) {
                return true;
            }
            return System.currentTimeMillis() - startTime <= duration * 1000;
        }

        @Override
        public TweetMessage next() {
            TweetMessage msg = null;
            getTwitterUser(null);
            Message message = randMessageGen.getNextRandomMessage();
            Point location = randLocationGen != null ? randLocationGen.getRandomPoint()
                    : locationGenFromOpenStreetMapData.getNextPoint();
            DateTime sendTime = randDateGen.getNextRandomDatetime();
            int btreeExtraFieldKey = random.nextInt();
            if (btreeExtraFieldKey == Integer.MIN_VALUE) {
                btreeExtraFieldKey = Integer.MIN_VALUE + 1;
            }
            twMessage.reset(idGen.getNextULong(), twUser, location, sendTime, message.getReferredTopics(), message,
                    btreeExtraFieldKey, DUMMY_SIZE_ADJUSTER);
            msg = twMessage;
            return msg;
        }

        @Override
        public void remove() {
            // TODO Auto-generated method stub
        }

        public void resetDuration(int duration) {
            this.duration = duration;
            startTime = System.currentTimeMillis();
        }

    }

    public static class InitializationInfo {
        public Date startDate = new Date(1, 1, 2005);
        public Date endDate = new Date(8, 20, 2012);
        public String[] lastNames = DataGenerator.lastNames;
        public String[] firstNames = DataGenerator.firstNames;
        public String[] vendors = DataGenerator.vendors;
        public String[] jargon = DataGenerator.jargon;
        public String[] org_list = DataGenerator.org_list;
    }

    public void initialize(InitializationInfo info, String openStreetMapFilePath, int locationSampleInterval) {
        randDateGen = new RandomDateGenerator(info.startDate, info.endDate);
        randNameGen = new RandomNameGenerator(info.firstNames, info.lastNames);
        if (openStreetMapFilePath == null) {
            randLocationGen = new RandomLocationGenerator(24, 49, 66, 98);
            locationGenFromOpenStreetMapData = null;
        } else {
            locationGenFromOpenStreetMapData = new LocationGeneratorFromOpenStreetMapData();
            locationGenFromOpenStreetMapData.intialize(openStreetMapFilePath, locationSampleInterval);
            randLocationGen = null;
        }
        randMessageGen = new RandomMessageGenerator(info.vendors, info.jargon);
    }

    public void getTwitterUser(String usernameSuffix) {
        String suggestedName = randNameGen.getRandomName();
        String[] nameComponents = suggestedName.split(" ");
        String screenName = nameComponents[0] + nameComponents[1] + randNameGen.getRandomNameSuffix();
        String name = suggestedName;
        if (usernameSuffix != null) {
            name = name + usernameSuffix;
        }
        int numFriends = random.nextInt((100)); // draw from Zipfian
        int statusesCount = random.nextInt(500); // draw from Zipfian
        int followersCount = random.nextInt((200));
        twUser.reset(screenName, numFriends, statusesCount, name, followersCount);
    }

    public static class RandomDateGenerator {

        private final Date startDate;
        private final Date endDate;
        private final Random random = new Random();
        private final int yearDifference;
        private Date workingDate;
        private Date recentDate;
        private DateTime dateTime;

        public RandomDateGenerator(Date startDate, Date endDate) {
            this.startDate = startDate;
            this.endDate = endDate;
            this.yearDifference = endDate.getYear() - startDate.getYear() + 1;
            this.workingDate = new Date();
            this.recentDate = new Date();
            this.dateTime = new DateTime();
        }

        public Date getStartDate() {
            return startDate;
        }

        public Date getEndDate() {
            return endDate;
        }

        public Date getNextRandomDate() {
            int year = random.nextInt(yearDifference) + startDate.getYear();
            int month;
            int day;
            if (year == endDate.getYear()) {
                month = random.nextInt(endDate.getMonth()) + 1;
                if (month == endDate.getMonth()) {
                    day = random.nextInt(endDate.getDay()) + 1;
                } else {
                    day = random.nextInt(28) + 1;
                }
            } else {
                month = random.nextInt(12) + 1;
                day = random.nextInt(28) + 1;
            }
            workingDate.reset(month, day, year);
            return workingDate;
        }

        public DateTime getNextRandomDatetime() {
            Date randomDate = getNextRandomDate();
            dateTime.reset(randomDate);
            return dateTime;
        }

        public Date getNextRecentDate(Date date) {
            int year = date.getYear()
                    + (date.getYear() == endDate.getYear() ? 0 : random.nextInt(endDate.getYear() - date.getYear()));
            int month = (year == endDate.getYear())
                    ? date.getMonth() == endDate.getMonth() ? (endDate.getMonth())
                            : (date.getMonth() + random.nextInt(endDate.getMonth() - date.getMonth()))
                    : random.nextInt(12) + 1;

            int day =
                    (year == endDate.getYear()) ? month == endDate.getMonth()
                            ? date.getDay() == endDate.getDay() ? endDate.getDay()
                                    : date.getDay() + random.nextInt(endDate.getDay() - date.getDay())
                            : random.nextInt(28) + 1 : random.nextInt(28) + 1;
            recentDate.reset(month, day, year);
            return recentDate;
        }

    }

    public static class DateTime extends Date {

        private String hour = "10";
        private String min = "10";
        private String sec = "00";

        public DateTime(int month, int day, int year, String hour, String min, String sec) {
            super(month, day, year);
            this.hour = hour;
            this.min = min;
            this.sec = sec;
        }

        public DateTime() {
        }

        public void reset(int month, int day, int year, String hour, String min, String sec) {
            super.setDay(month);
            super.setDay(day);
            super.setYear(year);
            this.hour = hour;
            this.min = min;
            this.sec = sec;
        }

        public DateTime(Date date) {
            super(date.getMonth(), date.getDay(), date.getYear());
        }

        public void reset(Date date) {
            reset(date.getMonth(), date.getDay(), date.getYear());
        }

        public DateTime(Date date, int hour, int min, int sec) {
            super(date.getMonth(), date.getDay(), date.getYear());
            this.hour = (hour < 10) ? "0" : "" + hour;
            this.min = (min < 10) ? "0" : "" + min;
            this.sec = (sec < 10) ? "0" : "" + sec;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("datetime");
            builder.append("(\"");
            builder.append(super.getYear());
            builder.append("-");
            builder.append(super.getMonth() < 10 ? "0" + super.getMonth() : super.getMonth());
            builder.append("-");
            builder.append(super.getDay() < 10 ? "0" + super.getDay() : super.getDay());
            builder.append("T");
            builder.append(hour + ":" + min + ":" + sec);
            builder.append("\")");
            return builder.toString();
        }
    }

    public static class Message {

        private char[] message = new char[500];
        private List<String> referredTopics;
        private int length;

        public Message(char[] m, List<String> referredTopics) {
            System.arraycopy(m, 0, message, 0, m.length);
            length = m.length;
            this.referredTopics = referredTopics;
        }

        public Message() {
            referredTopics = new ArrayList<String>();
            length = 0;
        }

        public List<String> getReferredTopics() {
            return referredTopics;
        }

        public void reset(char[] m, int offset, int length, List<String> referredTopics) {
            System.arraycopy(m, offset, message, 0, length);
            this.length = length;
            this.referredTopics = referredTopics;
        }

        public int getLength() {
            return length;
        }

        public char charAt(int index) {
            return message[index];
        }

    }

    public static class Point {

        private float latitude;
        private float longitude;

        public float getLatitude() {
            return latitude;
        }

        public float getLongitude() {
            return longitude;
        }

        public Point(float latitude, float longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }

        public void reset(float latitude, float longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }

        public Point() {
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("point(\"" + latitude + "," + longitude + "\")");
            return builder.toString();
        }
    }

    public static class RandomNameGenerator {

        private String[] firstNames;
        private String[] lastNames;

        private final Random random = new Random();

        private final String[] connectors = new String[] { "_", "#", "$", "@" };

        public RandomNameGenerator(String[] firstNames, String[] lastNames) {
            this.firstNames = firstNames;
            this.lastNames = lastNames;
        }

        public String getRandomName() {
            String name;
            name = getSuggestedName();
            return name;

        }

        private String getSuggestedName() {
            int firstNameIndex = random.nextInt(firstNames.length);
            int lastNameIndex = random.nextInt(lastNames.length);
            String suggestedName = firstNames[firstNameIndex] + " " + lastNames[lastNameIndex];
            return suggestedName;
        }

        public String getRandomNameSuffix() {
            return connectors[random.nextInt(connectors.length)] + random.nextInt(1000);
        }
    }

    public static class RandomMessageGenerator {

        private final MessageTemplate messageTemplate;

        public RandomMessageGenerator(String[] vendors, String[] jargon) {
            List<String> vendorList = new ArrayList<String>();
            for (String v : vendors) {
                vendorList.add(v);
            }
            List<String> jargonList = new ArrayList<String>();
            for (String j : jargon) {
                jargonList.add(j);
            }
            this.messageTemplate = new MessageTemplate(vendorList, jargonList);
        }

        public Message getNextRandomMessage() {
            return messageTemplate.getNextMessage();
        }
    }

    public static class AbstractMessageTemplate {

        protected final Random random = new Random();

        protected String[] positiveVerbs = new String[] { "like", "love" };
        protected String[] negativeVerbs = new String[] { "dislike", "hate", "can't stand" };

        protected String[] negativeAdjectives = new String[] { "horrible", "bad", "terrible", "OMG" };
        protected String[] postiveAdjectives = new String[] { "good", "awesome", "amazing", "mind-blowing" };

        protected String[] otherWords = new String[] { "the", "its" };
    }

    public static class MessageTemplate extends AbstractMessageTemplate {

        private List<String> vendors;
        private List<String> jargon;
        private CharBuffer buffer;
        private List<String> referredTopics;
        private Message message = new Message();

        public MessageTemplate(List<String> vendors, List<String> jargon) {
            this.vendors = vendors;
            this.jargon = jargon;
            buffer = CharBuffer.allocate(2500);
            referredTopics = new ArrayList<String>();
        }

        public Message getNextMessage() {
            buffer.position(0);
            buffer.limit(2500);
            referredTopics.clear();
            boolean isPositive = random.nextBoolean();
            String[] verbArray = isPositive ? positiveVerbs : negativeVerbs;
            String[] adjectiveArray = isPositive ? postiveAdjectives : negativeAdjectives;
            String verb = verbArray[random.nextInt(verbArray.length)];
            String adjective = adjectiveArray[random.nextInt(adjectiveArray.length)];

            buffer.put(" ");
            buffer.put(verb);
            buffer.put(" ");
            String vendor = vendors.get(random.nextInt(vendors.size()));
            referredTopics.add(vendor);
            buffer.append(vendor);
            buffer.append(" ");
            buffer.append(otherWords[random.nextInt(otherWords.length)]);
            buffer.append(" ");
            String jargonTerm = jargon.get(random.nextInt(jargon.size()));
            referredTopics.add(jargonTerm);
            buffer.append(jargonTerm);
            buffer.append(" is ");
            buffer.append(adjective);
            if (random.nextBoolean()) {
                buffer.append(isPositive ? ":)" : ":(");
            }

            buffer.flip();
            message.reset(buffer.array(), 0, buffer.limit(), referredTopics);
            return message;
        }
    }

    public static class RandomUtil {

        public static Random random = new Random();

        public static int[] getKFromN(int k, int n) {
            int[] result = new int[k];
            int cnt = 0;
            HashSet<Integer> values = new HashSet<Integer>();
            while (cnt < k) {
                int val = random.nextInt(n + 1);
                if (values.contains(val)) {
                    continue;
                }

                result[cnt++] = val;
                values.add(val);
            }
            return result;
        }
    }

    public static class RandomLocationGenerator {

        private Random random = new Random();

        private final int beginLat;
        private final int endLat;
        private final int beginLong;
        private final int endLong;

        private Point point;

        public RandomLocationGenerator(int beginLat, int endLat, int beginLong, int endLong) {
            this.beginLat = beginLat;
            this.endLat = endLat;
            this.beginLong = beginLong;
            this.endLong = endLong;
            this.point = new Point();
        }

        public Point getRandomPoint() {
            int latMajor = beginLat + random.nextInt(endLat - beginLat);
            int latMinor = random.nextInt(100);
            float latitude = latMajor + ((float) latMinor) / 100;

            int longMajor = beginLong + random.nextInt(endLong - beginLong);
            int longMinor = random.nextInt(100);
            float longitude = longMajor + ((float) longMinor) / 100;

            point.reset(latitude, longitude);
            return point;
        }

    }

    public static class LocationGeneratorFromOpenStreetMapData {
        /**
         * the source of gps data:
         * https://blog.openstreetmap.org/2012/04/01/bulk-gps-point-data/
         */
        private String openStreetMapFilePath;
        private long sampleInterval;
        private long lineCount = 0;
        private BufferedReader br;
        private String line;
        private String strPoints[] = null;
        private StringBuilder sb = new StringBuilder();
        private Point point = new Point();
        private float[] floatPoint = new float[2];

        public void intialize(String openStreetMapFilePath, int sampleInterval) {
            this.openStreetMapFilePath = openStreetMapFilePath;
            this.sampleInterval = sampleInterval;
            try {
                br = new BufferedReader(new FileReader(openStreetMapFilePath));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                throw new IllegalStateException(e);
            }
        }

        public Point getNextPoint() {
            try {
                while (true) {
                    if ((line = br.readLine()) == null) {
                        br = new BufferedReader(new FileReader(openStreetMapFilePath));
                        line = br.readLine(); //can't be null
                    }
                    if (lineCount++ % sampleInterval != 0) {
                        continue;
                    }
                    sb.setLength(0);
                    strPoints = line.split(",");
                    if (strPoints.length != 2) {
                        //ignore invalid point
                        continue;
                    } else {
                        break;
                    }
                }
                if (line == null) {
                    //roll over the data from the same file.
                    br.close();
                    br = null;
                    lineCount = 0;
                    br = new BufferedReader(new FileReader(openStreetMapFilePath));
                    while ((line = br.readLine()) != null) {
                        if (lineCount++ % sampleInterval != 0) {
                            continue;
                        }
                        sb.setLength(0);
                        strPoints = line.split(",");
                        if (strPoints.length != 2) {
                            //ignore invalid point
                            continue;
                        } else {
                            break;
                        }
                    }
                }
                floatPoint[0] = Float.parseFloat(strPoints[0]) / 10000000; //latitude (y value)
                floatPoint[1] = Float.parseFloat(strPoints[1]) / 10000000; //longitude (x value)
                point.reset(floatPoint[1], floatPoint[0]);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException(e);
            }
            return point;
        }

        @Override
        public void finalize() {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    public static class TweetMessage {

        private static int NUM_BTREE_EXTRA_FIELDS = 8;

        private long tweetid;
        private TwitterUser user;
        private Point senderLocation;
        private DateTime sendTime;
        private List<String> referredTopics;
        private Message messageText;
        private int[] btreeExtraFields;
        private String dummySizeAdjuster;

        public TweetMessage() {
            this.btreeExtraFields = new int[NUM_BTREE_EXTRA_FIELDS];
        }

        public TweetMessage(long tweetid, TwitterUser user, Point senderLocation, DateTime sendTime,
                List<String> referredTopics, Message messageText, int btreeExtraField, String dummySizeAdjuster) {
            this.tweetid = tweetid;
            this.user = user;
            this.senderLocation = senderLocation;
            this.sendTime = sendTime;
            this.referredTopics = referredTopics;
            this.messageText = messageText;
            this.btreeExtraFields = new int[NUM_BTREE_EXTRA_FIELDS];
            setBtreeExtraFields(btreeExtraField);
            this.dummySizeAdjuster = dummySizeAdjuster;
        }

        private void setBtreeExtraFields(int fVal) {
            for (int i = 0; i < btreeExtraFields.length; ++i) {
                btreeExtraFields[i] = fVal;
            }
        }

        public void reset(long tweetid, TwitterUser user, Point senderLocation, DateTime sendTime,
                List<String> referredTopics, Message messageText, int btreeExtraField, String dummySizeAdjuster) {
            this.tweetid = tweetid;
            this.user = user;
            this.senderLocation = senderLocation;
            this.sendTime = sendTime;
            this.referredTopics = referredTopics;
            this.messageText = messageText;
            setBtreeExtraFields(btreeExtraField);
            this.dummySizeAdjuster = dummySizeAdjuster;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            builder.append("\"tweetid\":");
            builder.append("int64(\"" + tweetid + "\")");
            builder.append(",");
            builder.append("\"user\":");
            builder.append(user);
            builder.append(",");
            builder.append("\"sender-location\":");
            builder.append(senderLocation);
            builder.append(",");
            builder.append("\"send-time\":");
            builder.append(sendTime);
            builder.append(",");
            builder.append("\"referred-topics\":");
            builder.append("{{");
            for (String topic : referredTopics) {
                builder.append("\"" + topic + "\"");
                builder.append(",");
            }
            if (referredTopics.size() > 0) {
                builder.deleteCharAt(builder.lastIndexOf(","));
            }
            builder.append("}}");
            builder.append(",");
            builder.append("\"message-text\":");
            builder.append("\"");
            for (int i = 0; i < messageText.getLength(); i++) {
                builder.append(messageText.charAt(i));
            }
            builder.append("\"");
            builder.append(",");
            for (int i = 0; i < btreeExtraFields.length; ++i) {
                builder.append("\"btree-extra-field" + (i + 1) + "\":");
                builder.append(btreeExtraFields[i]);
                if (i != btreeExtraFields.length - 1) {
                    builder.append(",");
                }
            }
            builder.append(",");
            builder.append("\"dummy-size-adjuster\":");
            builder.append("\"");
            builder.append(dummySizeAdjuster);
            builder.append("\"");
            builder.append("}");
            return new String(builder);
        }

        public long getTweetid() {
            return tweetid;
        }

        public void setTweetid(long tweetid) {
            this.tweetid = tweetid;
        }

        public TwitterUser getUser() {
            return user;
        }

        public void setUser(TwitterUser user) {
            this.user = user;
        }

        public Point getSenderLocation() {
            return senderLocation;
        }

        public void setSenderLocation(Point senderLocation) {
            this.senderLocation = senderLocation;
        }

        public DateTime getSendTime() {
            return sendTime;
        }

        public void setSendTime(DateTime sendTime) {
            this.sendTime = sendTime;
        }

        public List<String> getReferredTopics() {
            return referredTopics;
        }

        public void setReferredTopics(List<String> referredTopics) {
            this.referredTopics = referredTopics;
        }

        public Message getMessageText() {
            return messageText;
        }

        public void setMessageText(Message messageText) {
            this.messageText = messageText;
        }

    }

    public static class TwitterUser {

        private String screenName;
        private String lang = "en";
        private int friendsCount;
        private int statusesCount;
        private String name;
        private int followersCount;

        public TwitterUser() {

        }

        public TwitterUser(String screenName, int friendsCount, int statusesCount, String name, int followersCount) {
            this.screenName = screenName;
            this.friendsCount = friendsCount;
            this.statusesCount = statusesCount;
            this.name = name;
            this.followersCount = followersCount;
        }

        public void reset(String screenName, int friendsCount, int statusesCount, String name, int followersCount) {
            this.screenName = screenName;
            this.friendsCount = friendsCount;
            this.statusesCount = statusesCount;
            this.name = name;
            this.followersCount = followersCount;
        }

        public String getScreenName() {
            return screenName;
        }

        public int getFriendsCount() {
            return friendsCount;
        }

        public int getStatusesCount() {
            return statusesCount;
        }

        public String getName() {
            return name;
        }

        public int getFollowersCount() {
            return followersCount;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            builder.append("\"screen-name\":" + "\"" + screenName + "\"");
            builder.append(",");
            builder.append("\"lang\":" + "\"" + lang + "\"");
            builder.append(",");
            builder.append("\"friends_count\":" + friendsCount);
            builder.append(",");
            builder.append("\"statuses_count\":" + statusesCount);
            builder.append(",");
            builder.append("\"name\":" + "\"" + name + "\"");
            builder.append(",");
            builder.append("\"followers_count\":" + followersCount);
            builder.append("}");
            return builder.toString();
        }

    }

    public static class Date {

        private int day;
        private int month;
        private int year;

        public Date(int month, int day, int year) {
            this.month = month;
            this.day = day;
            this.year = year;
        }

        public void reset(int month, int day, int year) {
            this.month = month;
            this.day = day;
            this.year = year;
        }

        public int getDay() {
            return day;
        }

        public int getMonth() {
            return month;
        }

        public int getYear() {
            return year;
        }

        public Date() {
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("date");
            builder.append("(\"");
            builder.append(year);
            builder.append("-");
            builder.append(month < 10 ? "0" + month : "" + month);
            builder.append("-");
            builder.append(day < 10 ? "0" + day : "" + day);
            builder.append("\")");
            return builder.toString();
        }

        public void setDay(int day) {
            this.day = day;
        }

        public void setMonth(int month) {
            this.month = month;
        }

        public void setYear(int year) {
            this.year = year;
        }
    }

    public static void main(String[] args) throws Exception {
        DataGeneratorForSpatialIndexEvaluation dg =
                new DataGeneratorForSpatialIndexEvaluation(new InitializationInfo());
        TweetMessageIterator tmi = dg.new TweetMessageIterator(1, new GULongIDGenerator(0, (byte) 0));
        int len = 0;
        int count = 0;
        while (tmi.hasNext()) {
            String tm = tmi.next().toString();
            System.out.println(tm);
            len += tm.length();
            ++count;
        }
        System.out.println(DataGeneratorForSpatialIndexEvaluation.DUMMY_SIZE_ADJUSTER.length());
        System.out.println(len);
        System.out.println(count);
        System.out.println(len / count);
    }
}
