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
package edu.uci.ics.asterix.tools.external.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class DataGenerator {

    private RandomDateGenerator randDateGen;
    private RandomNameGenerator randNameGen;
    private RandomEmploymentGenerator randEmpGen;
    private RandomMessageGenerator randMessageGen;
    private RandomLocationGenerator randLocationGen;

    private DistributionHandler fbDistHandler;
    private DistributionHandler twDistHandler;

    private int totalFbMessages;
    private int numFbOnlyUsers;
    private int totalTwMessages;
    private int numTwOnlyUsers;

    private int numCommonUsers;

    private int fbUserId;
    private int twUserId;

    private int fbMessageId;
    private int twMessageId;

    private Random random = new Random();

    private String commonUserFbSuffix = "_fb";
    private String commonUserTwSuffix = "_tw";

    private String outputDir;

    private PartitionConfiguration partition;

    private FacebookUser fbUser = new FacebookUser();
    private TwitterUser twUser = new TwitterUser();

    private FacebookMessage fbMessage = new FacebookMessage();
    private TweetMessage twMessage = new TweetMessage();

    private int duration;

    public DataGenerator(String[] args) throws Exception {
        String controllerInstallDir = args[0];
        String partitionConfXML = controllerInstallDir + "/output/partition-conf.xml";
        String partitionName = args[1];
        partition = XMLUtil.getPartitionConfiguration(partitionConfXML, partitionName);

        // 1
        randDateGen = new RandomDateGenerator(new Date(1, 1, 2005), new Date(8, 20, 2012));

        String firstNameFile = controllerInstallDir + "/metadata/firstNames.txt";
        String lastNameFile = controllerInstallDir + "/metadata/lastNames.txt";
        String vendorFile = controllerInstallDir + "/metadata/vendors.txt";
        String jargonFile = controllerInstallDir + "/metadata/jargon.txt";
        String orgList = controllerInstallDir + "/metadata/org_list.txt";

        randNameGen = new RandomNameGenerator(firstNameFile, lastNameFile);
        randEmpGen = new RandomEmploymentGenerator(90, new Date(1, 1, 2000), new Date(8, 20, 2012), orgList);
        randLocationGen = new RandomLocationGenerator(24, 49, 66, 98);
        randMessageGen = new RandomMessageGenerator(vendorFile, jargonFile);

        totalFbMessages = partition.getTargetPartition().getFbMessageIdMax()
                - partition.getTargetPartition().getFbMessageIdMin() + 1;
        numFbOnlyUsers = (partition.getTargetPartition().getFbUserKeyMax()
                - partition.getTargetPartition().getFbUserKeyMin() + 1)
                - partition.getTargetPartition().getCommonUsers();

        totalTwMessages = partition.getTargetPartition().getTwMessageIdMax()
                - partition.getTargetPartition().getTwMessageIdMin() + 1;
        numTwOnlyUsers = (partition.getTargetPartition().getTwUserKeyMax()
                - partition.getTargetPartition().getTwUserKeyMin() + 1)
                - partition.getTargetPartition().getCommonUsers();

        numCommonUsers = partition.getTargetPartition().getCommonUsers();
        fbDistHandler = new DistributionHandler(totalFbMessages, 0.5, numFbOnlyUsers + numCommonUsers);
        twDistHandler = new DistributionHandler(totalTwMessages, 0.5, numTwOnlyUsers + numCommonUsers);

        fbUserId = partition.getTargetPartition().getFbUserKeyMin();
        twUserId = partition.getTargetPartition().getTwUserKeyMin();

        fbMessageId = partition.getTargetPartition().getFbMessageIdMin();
        twMessageId = partition.getTargetPartition().getTwMessageIdMin();

        outputDir = partition.getSourcePartition().getPath();
    }

    public DataGenerator(InitializationInfo info) {
        initialize(info);
    }

    private void generateFacebookOnlyUsers(int numFacebookUsers) throws IOException {
        FileAppender appender = FileUtil.getFileAppender(outputDir + "/" + "fb_users.adm", true, true);
        FileAppender messageAppender = FileUtil.getFileAppender(outputDir + "/" + "fb_message.adm", true, true);

        for (int i = 0; i < numFacebookUsers; i++) {
            getFacebookUser(null);
            appender.appendToFile(fbUser.toString());
            generateFacebookMessages(fbUser, messageAppender, -1);
        }
        appender.close();
        messageAppender.close();
    }

    private void generateTwitterOnlyUsers(int numTwitterUsers) throws IOException {
        FileAppender appender = FileUtil.getFileAppender(outputDir + "/" + "tw_users.adm", true, true);
        FileAppender messageAppender = FileUtil.getFileAppender(outputDir + "/" + "tw_message.adm", true, true);

        for (int i = 0; i < numTwitterUsers; i++) {
            getTwitterUser(null);
            appender.appendToFile(twUser.toString());
            generateTwitterMessages(twUser, messageAppender, -1);
        }
        appender.close();
        messageAppender.close();
    }

    private void generateCommonUsers() throws IOException {
        FileAppender fbAppender = FileUtil.getFileAppender(outputDir + "/" + "fb_users.adm", true, false);
        FileAppender twAppender = FileUtil.getFileAppender(outputDir + "/" + "tw_users.adm", true, false);
        FileAppender fbMessageAppender = FileUtil.getFileAppender(outputDir + "/" + "fb_message.adm", true, false);
        FileAppender twMessageAppender = FileUtil.getFileAppender(outputDir + "/" + "tw_message.adm", true, false);

        for (int i = 0; i < numCommonUsers; i++) {
            getFacebookUser(commonUserFbSuffix);
            fbAppender.appendToFile(fbUser.toString());
            generateFacebookMessages(fbUser, fbMessageAppender, -1);

            getCorrespondingTwitterUser(fbUser);
            twAppender.appendToFile(twUser.toString());
            generateTwitterMessages(twUser, twMessageAppender, -1);
        }

        fbAppender.close();
        twAppender.close();
        fbMessageAppender.close();
        twMessageAppender.close();
    }

    private void generateFacebookMessages(FacebookUser user, FileAppender appender, int numMsg) throws IOException {
        Message message;
        int numMessages = 0;
        if (numMsg == -1) {
            numMessages = fbDistHandler
                    .getFromDistribution(fbUserId - partition.getTargetPartition().getFbUserKeyMin());
        }
        for (int i = 0; i < numMessages; i++) {
            message = randMessageGen.getNextRandomMessage();
            Point location = randLocationGen.getRandomPoint();
            fbMessage.reset(fbMessageId++, user.getId(), random.nextInt(totalFbMessages + 1), location, message);
            appender.appendToFile(fbMessage.toString());
        }
    }

    private void generateTwitterMessages(TwitterUser user, FileAppender appender, int numMsg) throws IOException {
        Message message;
        int numMessages = 0;
        if (numMsg == -1) {
            numMessages = twDistHandler
                    .getFromDistribution(twUserId - partition.getTargetPartition().getTwUserKeyMin());
        }

        for (int i = 0; i < numMessages; i++) {
            message = randMessageGen.getNextRandomMessage();
            Point location = randLocationGen.getRandomPoint();
            DateTime sendTime = randDateGen.getNextRandomDatetime();
            twMessage.reset(twMessageId, user, location, sendTime, message.getReferredTopics(), message);
            twMessageId++;
            appender.appendToFile(twMessage.toString());
        }
    }

    public Iterator<TweetMessage> getTwitterMessageIterator(int partition, byte seed) {
        return new TweetMessageIterator(duration, partition, seed);
    }

    public class TweetMessageIterator implements Iterator<TweetMessage> {

        private final int duration;
        private long startTime = 0;
        private final GULongIDGenerator idGen;

        public TweetMessageIterator(int duration, int partition, byte seed) {
            this.duration = duration;
            this.idGen = new GULongIDGenerator(partition, seed);
        }

        @Override
        public boolean hasNext() {
            if (startTime == 0) {
                startTime = System.currentTimeMillis();
            }
            return System.currentTimeMillis() - startTime < duration * 1000;
        }

        @Override
        public TweetMessage next() {
            getTwitterUser(null);
            Message message = randMessageGen.getNextRandomMessage();
            Point location = randLocationGen.getRandomPoint();
            DateTime sendTime = randDateGen.getNextRandomDatetime();
            twMessage.reset(idGen.getNextULong(), twUser, location, sendTime, message.getReferredTopics(), message);
            twMessageId++;
            if (twUserId > numTwOnlyUsers) {
                twUserId = 1;
            }
            return twMessage;

        }

        @Override
        public void remove() {
            // TODO Auto-generated method stub
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
        public int percentEmployed = 90;
        public Date employmentStartDate = new Date(1, 1, 2000);
        public Date employmentEndDate = new Date(31, 12, 2012);
        public int totalFbMessages;
        public int numFbOnlyUsers;
        public int totalTwMessages;
        public int numTwOnlyUsers = 5000;
        public int numCommonUsers;
        public int fbUserIdMin;
        public int fbMessageIdMin;
        public int twUserIdMin;
        public int twMessageIdMin;
        public int timeDurationInSecs = 60;

    }

    public void initialize(InitializationInfo info) {
        randDateGen = new RandomDateGenerator(info.startDate, info.endDate);
        randNameGen = new RandomNameGenerator(info.firstNames, info.lastNames);
        randEmpGen = new RandomEmploymentGenerator(info.percentEmployed, info.employmentStartDate,
                info.employmentEndDate, info.org_list);
        randLocationGen = new RandomLocationGenerator(24, 49, 66, 98);
        randMessageGen = new RandomMessageGenerator(info.vendors, info.jargon);
        fbDistHandler = new DistributionHandler(info.totalFbMessages, 0.5, info.numFbOnlyUsers + info.numCommonUsers);
        twDistHandler = new DistributionHandler(info.totalTwMessages, 0.5, info.numTwOnlyUsers + info.numCommonUsers);
        fbUserId = info.fbUserIdMin;
        twUserId = info.twUserIdMin;

        fbMessageId = info.fbMessageIdMin;
        twMessageId = info.twMessageIdMin;
        duration = info.timeDurationInSecs;
    }

    public static void main(String args[]) throws Exception {
        if (args.length < 2) {
            printUsage();
            System.exit(1);
        }

        DataGenerator dataGenerator = new DataGenerator(args);
        dataGenerator.generateData();
    }

    public static void printUsage() {
        System.out.println(" Error: Invalid number of arguments ");
        System.out.println(" Usage :" + " DataGenerator <path to configuration file> <partition name> ");
    }

    public void generateData() throws IOException {
        generateFacebookOnlyUsers(numFbOnlyUsers);
        generateTwitterOnlyUsers(numTwOnlyUsers);
        generateCommonUsers();
        System.out.println("Partition :" + partition.getTargetPartition().getName() + " finished");
    }

    public void getFacebookUser(String usernameSuffix) {
        String suggestedName = randNameGen.getRandomName();
        String[] nameComponents = suggestedName.split(" ");
        String name = nameComponents[0] + nameComponents[1];
        if (usernameSuffix != null) {
            name = name + usernameSuffix;
        }
        String alias = nameComponents[0];
        String userSince = randDateGen.getNextRandomDatetime().toString();
        int numFriends = random.nextInt(25);
        int[] friendIds = RandomUtil.getKFromN(numFriends, (numFbOnlyUsers + numCommonUsers));
        Employment emp = randEmpGen.getRandomEmployment();
        fbUser.reset(fbUserId++, alias, name, userSince, friendIds, emp);
    }

    public void getTwitterUser(String usernameSuffix) {
        String suggestedName = randNameGen.getRandomName();
        String[] nameComponents = suggestedName.split(" ");
        String screenName = nameComponents[0] + nameComponents[1] + randNameGen.getRandomNameSuffix();
        String name = suggestedName;
        if (usernameSuffix != null) {
            name = name + usernameSuffix;
        }
        int numFriends = random.nextInt((int) (100)); // draw from Zipfian
        int statusesCount = random.nextInt(500); // draw from Zipfian
        int followersCount = random.nextInt((int) (200));
        twUser.reset(screenName, numFriends, statusesCount, name, followersCount);
        twUserId++;
    }

    public void getCorrespondingTwitterUser(FacebookUser fbUser) {
        String screenName = fbUser.getName().substring(0, fbUser.getName().lastIndexOf(commonUserFbSuffix))
                + commonUserTwSuffix;
        String name = screenName.split(" ")[0];
        int numFriends = random.nextInt((int) ((numTwOnlyUsers + numCommonUsers)));
        int statusesCount = random.nextInt(500); // draw from Zipfian
        int followersCount = random.nextInt((int) (numTwOnlyUsers + numCommonUsers));
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
            yearDifference = endDate.getYear() - startDate.getYear() + 1;
            workingDate = new Date();
            recentDate = new Date();
            dateTime = new DateTime();
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
            int month = (year == endDate.getYear()) ? date.getMonth() == endDate.getMonth() ? (endDate.getMonth())
                    : (date.getMonth() + random.nextInt(endDate.getMonth() - date.getMonth())) : random.nextInt(12) + 1;

            int day = (year == endDate.getYear()) ? month == endDate.getMonth() ? date.getDay() == endDate.getDay() ? endDate
                    .getDay() : date.getDay() + random.nextInt(endDate.getDay() - date.getDay())
                    : random.nextInt(28) + 1
                    : random.nextInt(28) + 1;
            recentDate.reset(month, day, year);
            return recentDate;
        }

        public static void main(String args[]) throws Exception {
            RandomDateGenerator dgen = new RandomDateGenerator(new Date(1, 1, 2005), new Date(8, 20, 2012));
            while (true) {
                Date nextDate = dgen.getNextRandomDate();
                if (nextDate.getDay() == 0) {
                    throw new Exception("invalid date " + nextDate);
                }
            }
        }
    }

    public static class DateTime extends Date {

        private String hour = "10";
        private String min = "10";
        private String sec = "00";
        private long chrononTime;

        public DateTime(int month, int day, int year, String hour, String min, String sec) {
            super(month, day, year);
            this.hour = hour;
            this.min = min;
            this.sec = sec;
            chrononTime = new java.util.Date(year, month, day, Integer.parseInt(hour), Integer.parseInt(min),
                    Integer.parseInt(sec)).getTime();
        }

        public void reset(int month, int day, int year, String hour, String min, String sec) {
            super.setDay(month);
            super.setDay(day);
            super.setYear(year);
            this.hour = hour;
            this.min = min;
            this.sec = sec;
            chrononTime = new java.util.Date(year, month, day, Integer.parseInt(hour), Integer.parseInt(min),
                    Integer.parseInt(sec)).getTime();
        }

        public DateTime() {
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

        public long getChrononTime() {
            return chrononTime;
        }

        public String getHour() {
            return hour;
        }

        public String getMin() {
            return min;
        }

        public String getSec() {
            return sec;
        }

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

        public char[] getMessage() {
            return message;
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

        public RandomNameGenerator(String firstNameFilePath, String lastNameFilePath) throws IOException {
            firstNames = FileUtil.listyFile(new File(firstNameFilePath)).toArray(new String[] {});
            lastNames = FileUtil.listyFile(new File(lastNameFilePath)).toArray(new String[] {});
        }

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

        public RandomMessageGenerator(String vendorFilePath, String jargonFilePath) throws IOException {
            List<String> vendors = FileUtil.listyFile(new File(vendorFilePath));
            List<String> jargon = FileUtil.listyFile(new File(jargonFilePath));
            this.messageTemplate = new MessageTemplate(vendors, jargon);
        }

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

    public static class FileUtil {

        public static List<String> listyFile(File file) throws IOException {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            List<String> list = new ArrayList<String>();
            while (true) {
                line = reader.readLine();
                if (line == null) {
                    break;
                }
                list.add(line);
            }
            reader.close();
            return list;
        }

        public static FileAppender getFileAppender(String filePath, boolean createIfNotExists, boolean overwrite)
                throws IOException {
            return new FileAppender(filePath, createIfNotExists, overwrite);
        }
    }

    public static class FileAppender {

        private final BufferedWriter writer;

        public FileAppender(String filePath, boolean createIfNotExists, boolean overwrite) throws IOException {
            File file = new File(filePath);
            if (!file.exists()) {
                if (createIfNotExists) {
                    new File(file.getParent()).mkdirs();
                } else {
                    throw new IOException("path " + filePath + " does not exists");
                }
            }
            this.writer = new BufferedWriter(new FileWriter(file, !overwrite));
        }

        public void appendToFile(String content) throws IOException {
            writer.append(content);
            writer.append("\n");
        }

        public void close() throws IOException {
            writer.close();
        }
    }

    public static class RandomEmploymentGenerator {

        private final int percentEmployed;
        private final Random random = new Random();
        private final RandomDateGenerator randDateGen;
        private final List<String> organizations;
        private Employment emp;

        public RandomEmploymentGenerator(int percentEmployed, Date beginEmpDate, Date endEmpDate, String orgListPath)
                throws IOException {
            this.percentEmployed = percentEmployed;
            this.randDateGen = new RandomDateGenerator(beginEmpDate, endEmpDate);
            organizations = FileUtil.listyFile(new File(orgListPath));
            emp = new Employment();
        }

        public RandomEmploymentGenerator(int percentEmployed, Date beginEmpDate, Date endEmpDate, String[] orgList) {
            this.percentEmployed = percentEmployed;
            this.randDateGen = new RandomDateGenerator(beginEmpDate, endEmpDate);
            organizations = new ArrayList<String>();
            for (String org : orgList) {
                organizations.add(org);
            }
            emp = new Employment();
        }

        public Employment getRandomEmployment() {
            int empployed = random.nextInt(100) + 1;
            boolean isEmployed = false;
            if (empployed <= percentEmployed) {
                isEmployed = true;
            }
            Date startDate = randDateGen.getNextRandomDate();
            Date endDate = null;
            if (!isEmployed) {
                endDate = randDateGen.getNextRecentDate(startDate);
            }
            String org = organizations.get(random.nextInt(organizations.size()));
            emp.reset(org, startDate, endDate);
            return emp;
        }
    }

    public static class RandomLocationGenerator {

        private final int beginLat;
        private final int endLat;
        private final int beginLong;
        private final int endLong;

        private Random random = new Random();
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

    public static class PartitionConfiguration {

        private final TargetPartition targetPartition;
        private final SourcePartition sourcePartition;

        public PartitionConfiguration(SourcePartition sourcePartition, TargetPartition targetPartition) {
            this.sourcePartition = sourcePartition;
            this.targetPartition = targetPartition;
        }

        public TargetPartition getTargetPartition() {
            return targetPartition;
        }

        public SourcePartition getSourcePartition() {
            return sourcePartition;
        }

    }

    public static class SourcePartition {

        private final String name;
        private final String host;
        private final String path;

        public SourcePartition(String name, String host, String path) {
            this.name = name;
            this.host = host;
            this.path = path;
        }

        public String getName() {
            return name;
        }

        public String getHost() {
            return host;
        }

        public String getPath() {
            return path;
        }
    }

    public static class TargetPartition {
        private final String name;
        private final String host;
        private final String path;
        private final int fbUserKeyMin;
        private final int fbUserKeyMax;
        private final int twUserKeyMin;
        private final int twUserKeyMax;
        private final int fbMessageIdMin;
        private final int fbMessageIdMax;
        private final int twMessageIdMin;
        private final int twMessageIdMax;
        private final int commonUsers;

        public TargetPartition(String partitionName, String host, String path, int fbUserKeyMin, int fbUserKeyMax,
                int twUserKeyMin, int twUserKeyMax, int fbMessageIdMin, int fbMessageIdMax, int twMessageIdMin,
                int twMessageIdMax, int commonUsers) {
            this.name = partitionName;
            this.host = host;
            this.path = path;
            this.fbUserKeyMin = fbUserKeyMin;
            this.fbUserKeyMax = fbUserKeyMax;
            this.twUserKeyMin = twUserKeyMin;
            this.twUserKeyMax = twUserKeyMax;
            this.twMessageIdMin = twMessageIdMin;
            this.twMessageIdMax = twMessageIdMax;
            this.fbMessageIdMin = fbMessageIdMin;
            this.fbMessageIdMax = fbMessageIdMax;
            this.commonUsers = commonUsers;
        }

        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(name);
            builder.append(" ");
            builder.append(host);
            builder.append("\n");
            builder.append(path);
            builder.append("\n");
            builder.append("fbUser:key:min");
            builder.append(fbUserKeyMin);

            builder.append("\n");
            builder.append("fbUser:key:max");
            builder.append(fbUserKeyMax);

            builder.append("\n");
            builder.append("twUser:key:min");
            builder.append(twUserKeyMin);

            builder.append("\n");
            builder.append("twUser:key:max");
            builder.append(twUserKeyMax);

            builder.append("\n");
            builder.append("fbMessage:key:min");
            builder.append(fbMessageIdMin);

            builder.append("\n");
            builder.append("fbMessage:key:max");
            builder.append(fbMessageIdMax);

            builder.append("\n");
            builder.append("twMessage:key:min");
            builder.append(twMessageIdMin);

            builder.append("\n");
            builder.append("twMessage:key:max");
            builder.append(twMessageIdMax);

            builder.append("\n");
            builder.append("twMessage:key:max");
            builder.append(twMessageIdMax);

            builder.append("\n");
            builder.append("commonUsers");
            builder.append(commonUsers);

            return new String(builder);
        }

        public String getName() {
            return name;
        }

        public String getHost() {
            return host;
        }

        public int getFbUserKeyMin() {
            return fbUserKeyMin;
        }

        public int getFbUserKeyMax() {
            return fbUserKeyMax;
        }

        public int getTwUserKeyMin() {
            return twUserKeyMin;
        }

        public int getTwUserKeyMax() {
            return twUserKeyMax;
        }

        public int getFbMessageIdMin() {
            return fbMessageIdMin;
        }

        public int getFbMessageIdMax() {
            return fbMessageIdMax;
        }

        public int getTwMessageIdMin() {
            return twMessageIdMin;
        }

        public int getTwMessageIdMax() {
            return twMessageIdMax;
        }

        public int getCommonUsers() {
            return commonUsers;
        }

        public String getPath() {
            return path;
        }
    }

    public static class Employment {

        private String organization;
        private Date startDate;
        private Date endDate;

        public Employment(String organization, Date startDate, Date endDate) {
            this.organization = organization;
            this.startDate = startDate;
            this.endDate = endDate;
        }

        public Employment() {
        }

        public void reset(String organization, Date startDate, Date endDate) {
            this.organization = organization;
            this.startDate = startDate;
            this.endDate = endDate;
        }

        public String getOrganization() {
            return organization;
        }

        public Date getStartDate() {
            return startDate;
        }

        public Date getEndDate() {
            return endDate;
        }

        public String toString() {
            StringBuilder builder = new StringBuilder("");
            builder.append("{");
            builder.append("\"organization-name\":");
            builder.append("\"" + organization + "\"");
            builder.append(",");
            builder.append("\"start-date\":");
            builder.append(startDate);
            if (endDate != null) {
                builder.append(",");
                builder.append("\"end-date\":");
                builder.append(endDate);
            }
            builder.append("}");
            return new String(builder);
        }

    }

    public static class FacebookMessage {

        private int messageId;
        private int authorId;
        private int inResponseTo;
        private Point senderLocation;
        private Message message;

        public int getMessageId() {
            return messageId;
        }

        public int getAuthorID() {
            return authorId;
        }

        public Point getSenderLocation() {
            return senderLocation;
        }

        public Message getMessage() {
            return message;
        }

        public int getInResponseTo() {
            return inResponseTo;
        }

        public FacebookMessage() {

        }

        public FacebookMessage(int messageId, int authorId, int inResponseTo, Point senderLocation, Message message) {
            this.messageId = messageId;
            this.authorId = authorId;
            this.inResponseTo = inResponseTo;
            this.senderLocation = senderLocation;
            this.message = message;
        }

        public void reset(int messageId, int authorId, int inResponseTo, Point senderLocation, Message message) {
            this.messageId = messageId;
            this.authorId = authorId;
            this.inResponseTo = inResponseTo;
            this.senderLocation = senderLocation;
            this.message = message;
        }

        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            builder.append("\"message-id\":");
            builder.append(messageId);
            builder.append(",");
            builder.append("\"author-id\":");
            builder.append(authorId);
            builder.append(",");
            builder.append("\"in-response-to\":");
            builder.append(inResponseTo);
            builder.append(",");
            builder.append("\"sender-location\":");
            builder.append(senderLocation);
            builder.append(",");
            builder.append("\"message\":");
            builder.append("\"");
            for (int i = 0; i < message.getLength(); i++) {
                builder.append(message.charAt(i));
            }
            builder.append("\"");
            builder.append("}");
            return new String(builder);
        }
    }

    public static class FacebookUser {

        private int id;
        private String alias;
        private String name;
        private String userSince;
        private int[] friendIds;
        private Employment employment;

        public FacebookUser() {

        }

        public FacebookUser(int id, String alias, String name, String userSince, int[] friendIds, Employment employment) {
            this.id = id;
            this.alias = alias;
            this.name = name;
            this.userSince = userSince;
            this.friendIds = friendIds;
            this.employment = employment;
        }

        public int getId() {
            return id;
        }

        public String getAlias() {
            return alias;
        }

        public String getName() {
            return name;
        }

        public String getUserSince() {
            return userSince;
        }

        public int[] getFriendIds() {
            return friendIds;
        }

        public Employment getEmployment() {
            return employment;
        }

        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            builder.append("\"id\":" + id);
            builder.append(",");
            builder.append("\"alias\":" + "\"" + alias + "\"");
            builder.append(",");
            builder.append("\"name\":" + "\"" + name + "\"");
            builder.append(",");
            builder.append("\"user-since\":" + userSince);
            builder.append(",");
            builder.append("\"friend-ids\":");
            builder.append("{{");
            for (int i = 0; i < friendIds.length; i++) {
                builder.append(friendIds[i]);
                builder.append(",");
            }
            if (friendIds.length > 0) {
                builder.deleteCharAt(builder.lastIndexOf(","));
            }
            builder.append("}}");
            builder.append(",");
            builder.append("\"employment\":");
            builder.append("[");
            builder.append(employment.toString());
            builder.append("]");
            builder.append("}");
            return builder.toString();
        }

        public void setId(int id) {
            this.id = id;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setUserSince(String userSince) {
            this.userSince = userSince;
        }

        public void setFriendIds(int[] friendIds) {
            this.friendIds = friendIds;
        }

        public void setEmployment(Employment employment) {
            this.employment = employment;
        }

        public void reset(int id, String alias, String name, String userSince, int[] friendIds, Employment employment) {
            this.id = id;
            this.alias = alias;
            this.name = name;
            this.userSince = userSince;
            this.friendIds = friendIds;
            this.employment = employment;
        }
    }

    public static class TweetMessage {

        private long tweetid;
        private TwitterUser user;
        private Point senderLocation;
        private DateTime sendTime;
        private List<String> referredTopics;
        private Message messageText;

        public TweetMessage() {

        }

        public TweetMessage(long tweetid, TwitterUser user, Point senderLocation, DateTime sendTime,
                List<String> referredTopics, Message messageText) {
            this.tweetid = tweetid;
            this.user = user;
            this.senderLocation = senderLocation;
            this.sendTime = sendTime;
            this.referredTopics = referredTopics;
            this.messageText = messageText;
        }

        public void reset(long tweetid, TwitterUser user, Point senderLocation, DateTime sendTime,
                List<String> referredTopics, Message messageText) {
            this.tweetid = tweetid;
            this.user = user;
            this.senderLocation = senderLocation;
            this.sendTime = sendTime;
            this.referredTopics = referredTopics;
            this.messageText = messageText;
        }

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

    public static class DistributionHandler {

        private final ZipfGenerator zipfGen;
        private final int totalUsers;
        private final int totalMessages;
        private Random random = new Random();

        public DistributionHandler(int totalMessages, double skew, int totalNumUsers) {
            zipfGen = new ZipfGenerator(totalMessages, skew);
            totalUsers = totalNumUsers;
            this.totalMessages = totalMessages;
        }

        public int getFromDistribution(int rank) {
            double prob = zipfGen.getProbability(rank);
            int numMessages = (int) (prob * totalMessages);
            return numMessages;
        }

        public static void main(String args[]) {
            int totalMessages = 1000 * 4070;
            double skew = 0.5;
            int totalUsers = 4070;
            DistributionHandler d = new DistributionHandler(totalMessages, skew, totalUsers);
            int sum = 0;
            for (int i = totalUsers; i >= 1; i--) {
                float contrib = d.getFromDistribution(i);
                sum += contrib;
                System.out.println(i + ":" + contrib);
            }

            System.out.println("SUM" + ":" + sum);

        }
    }

    public static class ZipfGenerator {

        private Random rnd = new Random(System.currentTimeMillis());
        private int size;
        private double skew;
        private double bottom = 0;

        public ZipfGenerator(int size, double skew) {
            this.size = size;
            this.skew = skew;
            for (int i = 1; i < size; i++) {
                this.bottom += (1 / Math.pow(i, this.skew));
            }
        }

        // the next() method returns an rank id. The frequency of returned rank
        // ids are follows Zipf distribution.
        public int next() {
            int rank;
            double friquency = 0;
            double dice;
            rank = rnd.nextInt(size);
            friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
            dice = rnd.nextDouble();
            while (!(dice < friquency)) {
                rank = rnd.nextInt(size);
                friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
                dice = rnd.nextDouble();
            }
            return rank;
        }

        // This method returns a probability that the given rank occurs.
        public double getProbability(int rank) {
            return (1.0d / Math.pow(rank, this.skew)) / this.bottom;
        }

        public static void main(String[] args) throws IOException {
            int total = (int) (3.7 * 1000 * 1000);
            int skew = 2;
            int numUsers = 1000 * 1000;
            /*
             * if (args.length != 2) { System.out.println("usage:" +
             * "./zipf size skew"); System.exit(-1); }
             */
            BufferedWriter buf = new BufferedWriter(new FileWriter(new File("/tmp/zip_output")));
            ZipfGenerator zipf = new ZipfGenerator(total, skew);
            double sum = 0;
            for (int i = 1; i <= numUsers; i++) {
                double prob = zipf.getProbability(i);
                double contribution = (double) (prob * total);
                String contrib = i + ":" + contribution;
                buf.write(contrib);
                buf.write("\n");
                System.out.println(contrib);
                sum += contribution;
            }
            System.out.println("sum is :" + sum);
        }
    }

    public static class PartitionElement implements ILibraryElement {
        private final String name;
        private final String host;
        private final int fbUserKeyMin;
        private final int fbUserKeyMax;
        private final int twUserKeyMin;
        private final int twUserKeyMax;
        private final int fbMessageIdMin;
        private final int fbMessageIdMax;
        private final int twMessageIdMin;
        private final int twMessageIdMax;

        public PartitionElement(String partitionName, String host, int fbUserKeyMin, int fbUserKeyMax,
                int twUserKeyMin, int twUserKeyMax, int fbMessageIdMin, int fbMessageIdMax, int twMessageIdMin,
                int twMessageIdMax) {
            this.name = partitionName;
            this.host = host;
            this.fbUserKeyMin = fbUserKeyMin;
            this.fbUserKeyMax = fbUserKeyMax;
            this.twUserKeyMin = twUserKeyMax;
            this.twUserKeyMax = twUserKeyMax;
            this.twMessageIdMin = twMessageIdMin;
            this.twMessageIdMax = twMessageIdMax;
            this.fbMessageIdMin = fbMessageIdMin;
            this.fbMessageIdMax = fbMessageIdMax;
        }

        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(name);
            builder.append(" ");
            builder.append(host);
            builder.append("\n");
            builder.append("fbUser:key:min");
            builder.append(fbUserKeyMin);

            builder.append("\n");
            builder.append("fbUser:key:max");
            builder.append(fbUserKeyMax);

            builder.append("\n");
            builder.append("twUser:key:min");
            builder.append(twUserKeyMin);

            builder.append("\n");
            builder.append("twUser:key:max");
            builder.append(twUserKeyMax);

            builder.append("\n");
            builder.append("fbMessage:key:min");
            builder.append(fbMessageIdMin);

            builder.append("\n");
            builder.append("fbMessage:key:max");
            builder.append(fbMessageIdMax);

            builder.append("\n");
            builder.append("twMessage:key:min");
            builder.append(twMessageIdMin);

            builder.append("\n");
            builder.append("twMessage:key:max");
            builder.append(twMessageIdMax);

            builder.append("\n");
            builder.append("twMessage:key:max");
            builder.append(twUserKeyMin);

            return new String(builder);
        }

        @Override
        public String getName() {
            return "Partition";
        }

    }

    interface ILibraryElement {

        public enum ElementType {
            PARTITION
        }

        public String getName();

    }

    public static class Configuration {

        private final float numMB;
        private final String unit;

        private final List<SourcePartition> sourcePartitions;
        private List<TargetPartition> targetPartitions;

        public Configuration(float numMB, String unit, List<SourcePartition> partitions) throws IOException {
            this.numMB = numMB;
            this.unit = unit;
            this.sourcePartitions = partitions;

        }

        public float getNumMB() {
            return numMB;
        }

        public String getUnit() {
            return unit;
        }

        public List<SourcePartition> getSourcePartitions() {
            return sourcePartitions;
        }

        public List<TargetPartition> getTargetPartitions() {
            return targetPartitions;
        }

        public void setTargetPartitions(List<TargetPartition> targetPartitions) {
            this.targetPartitions = targetPartitions;
        }

    }

    public static class XMLUtil {

        public static void writeToXML(Configuration conf, String filePath) throws IOException,
                ParserConfigurationException, TransformerException {

            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

            // root elements
            Document doc = docBuilder.newDocument();
            Element rootElement = doc.createElement("Partitions");
            doc.appendChild(rootElement);

            int index = 0;
            for (TargetPartition partition : conf.getTargetPartitions()) {
                writePartitionElement(conf.getSourcePartitions().get(index), partition, rootElement, doc);
            }

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();

            transformer.setOutputProperty(OutputKeys.ENCODING, "utf-8");
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(new File(filePath));

            transformer.transform(source, result);

        }

        public static void writePartitionInfo(Configuration conf, String filePath) throws IOException {
            BufferedWriter bw = new BufferedWriter(new FileWriter(filePath));
            for (SourcePartition sp : conf.getSourcePartitions()) {
                bw.write(sp.getHost() + ":" + sp.getName() + ":" + sp.getPath());
                bw.write("\n");
            }
            bw.close();
        }

        public static Document getDocument(String filePath) throws Exception {
            File inputFile = new File(filePath);
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(inputFile);
            doc.getDocumentElement().normalize();
            return doc;
        }

        public static Configuration getConfiguration(String filePath) throws Exception {
            Configuration conf = getConfiguration(getDocument(filePath));
            PartitionMetrics metrics = new PartitionMetrics(conf.getNumMB(), conf.getUnit(), conf.getSourcePartitions()
                    .size());
            List<TargetPartition> targetPartitions = getTargetPartitions(metrics, conf.getSourcePartitions());
            conf.setTargetPartitions(targetPartitions);
            return conf;
        }

        public static Configuration getConfiguration(Document document) throws IOException {
            Element rootEle = document.getDocumentElement();
            NodeList nodeList = rootEle.getChildNodes();
            float size = Float.parseFloat(getStringValue((Element) nodeList, "size"));
            String unit = getStringValue((Element) nodeList, "unit");
            List<SourcePartition> sourcePartitions = getSourcePartitions(document);
            return new Configuration(size, unit, sourcePartitions);
        }

        public static List<SourcePartition> getSourcePartitions(Document document) {
            Element rootEle = document.getDocumentElement();
            NodeList nodeList = rootEle.getElementsByTagName("partition");
            List<SourcePartition> sourcePartitions = new ArrayList<SourcePartition>();
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                sourcePartitions.add(getSourcePartition((Element) node));
            }
            return sourcePartitions;
        }

        public static SourcePartition getSourcePartition(Element functionElement) {
            String name = getStringValue(functionElement, "name");
            String host = getStringValue(functionElement, "host");
            String path = getStringValue(functionElement, "path");
            SourcePartition sp = new SourcePartition(name, host, path);
            return sp;
        }

        public static String getStringValue(Element element, String tagName) {
            String textValue = null;
            NodeList nl = element.getElementsByTagName(tagName);
            if (nl != null && nl.getLength() > 0) {
                Element el = (Element) nl.item(0);
                textValue = el.getFirstChild().getNodeValue();
            }
            return textValue;
        }

        public static PartitionConfiguration getPartitionConfiguration(String filePath, String partitionName)
                throws Exception {
            PartitionConfiguration pconf = getPartitionConfigurations(getDocument(filePath), partitionName);
            return pconf;
        }

        public static PartitionConfiguration getPartitionConfigurations(Document document, String partitionName)
                throws IOException {

            Element rootEle = document.getDocumentElement();
            NodeList nodeList = rootEle.getElementsByTagName("Partition");

            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                Element nodeElement = (Element) node;
                String name = getStringValue(nodeElement, "name");
                if (!name.equalsIgnoreCase(partitionName)) {
                    continue;
                }
                String host = getStringValue(nodeElement, "host");
                String path = getStringValue(nodeElement, "path");

                String fbUserKeyMin = getStringValue(nodeElement, "fbUserKeyMin");
                String fbUserKeyMax = getStringValue(nodeElement, "fbUserKeyMax");
                String twUserKeyMin = getStringValue(nodeElement, "twUserKeyMin");
                String twUserKeyMax = getStringValue(nodeElement, "twUserKeyMax");
                String fbMessageKeyMin = getStringValue(nodeElement, "fbMessageKeyMin");

                String fbMessageKeyMax = getStringValue(nodeElement, "fbMessageKeyMax");
                String twMessageKeyMin = getStringValue(nodeElement, "twMessageKeyMin");
                String twMessageKeyMax = getStringValue(nodeElement, "twMessageKeyMax");
                String numCommonUsers = getStringValue(nodeElement, "numCommonUsers");

                SourcePartition sp = new SourcePartition(name, host, path);

                TargetPartition tp = new TargetPartition(partitionName, host, path, Integer.parseInt(fbUserKeyMin),
                        Integer.parseInt(fbUserKeyMax), Integer.parseInt(twUserKeyMin), Integer.parseInt(twUserKeyMax),
                        Integer.parseInt(fbMessageKeyMin), Integer.parseInt(fbMessageKeyMax),
                        Integer.parseInt(twMessageKeyMin), Integer.parseInt(twMessageKeyMax),
                        Integer.parseInt(numCommonUsers));
                PartitionConfiguration pc = new PartitionConfiguration(sp, tp);
                return pc;
            }
            return null;
        }

        public static List<TargetPartition> getTargetPartitions(PartitionMetrics metrics,
                List<SourcePartition> sourcePartitions) {
            List<TargetPartition> partitions = new ArrayList<TargetPartition>();
            int fbUserKeyMin = 1;
            int twUserKeyMin = 1;
            int fbMessageIdMin = 1;
            int twMessageIdMin = 1;

            for (SourcePartition sp : sourcePartitions) {
                int fbUserKeyMax = fbUserKeyMin + metrics.getFbOnlyUsers() + metrics.getCommonUsers() - 1;
                int twUserKeyMax = twUserKeyMin + metrics.getTwitterOnlyUsers() + metrics.getCommonUsers() - 1;

                int fbMessageIdMax = fbMessageIdMin + metrics.getFbMessages() - 1;
                int twMessageIdMax = twMessageIdMin + metrics.getTwMessages() - 1;
                TargetPartition pe = new TargetPartition(sp.getName(), sp.getHost(), sp.getPath(), fbUserKeyMin,
                        fbUserKeyMax, twUserKeyMin, twUserKeyMax, fbMessageIdMin, fbMessageIdMax, twMessageIdMin,
                        twMessageIdMax, metrics.getCommonUsers());
                partitions.add(pe);

                fbUserKeyMin = fbUserKeyMax + 1;
                twUserKeyMin = twUserKeyMax + 1;

                fbMessageIdMin = fbMessageIdMax + 1;
                twMessageIdMin = twMessageIdMax + 1;
            }

            return partitions;
        }

        public static void writePartitionElement(SourcePartition sourcePartition, TargetPartition partition,
                Element rootElement, Document doc) {
            // staff elements
            Element pe = doc.createElement("Partition");
            rootElement.appendChild(pe);

            // name element
            Element name = doc.createElement("name");
            name.appendChild(doc.createTextNode("" + partition.getName()));
            pe.appendChild(name);

            // host element
            Element host = doc.createElement("host");
            host.appendChild(doc.createTextNode("" + partition.getHost()));
            pe.appendChild(host);

            // path element
            Element path = doc.createElement("path");
            path.appendChild(doc.createTextNode("" + partition.getPath()));
            pe.appendChild(path);

            // fbUserKeyMin element
            Element fbUserKeyMin = doc.createElement("fbUserKeyMin");
            fbUserKeyMin.appendChild(doc.createTextNode("" + partition.getFbUserKeyMin()));
            pe.appendChild(fbUserKeyMin);

            // fbUserKeyMax element
            Element fbUserKeyMax = doc.createElement("fbUserKeyMax");
            fbUserKeyMax.appendChild(doc.createTextNode("" + partition.getFbUserKeyMax()));
            pe.appendChild(fbUserKeyMax);

            // twUserKeyMin element
            Element twUserKeyMin = doc.createElement("twUserKeyMin");
            twUserKeyMin.appendChild(doc.createTextNode("" + partition.getTwUserKeyMin()));
            pe.appendChild(twUserKeyMin);

            // twUserKeyMax element
            Element twUserKeyMax = doc.createElement("twUserKeyMax");
            twUserKeyMax.appendChild(doc.createTextNode("" + partition.getTwUserKeyMax()));
            pe.appendChild(twUserKeyMax);

            // fbMessgeKeyMin element
            Element fbMessageKeyMin = doc.createElement("fbMessageKeyMin");
            fbMessageKeyMin.appendChild(doc.createTextNode("" + partition.getFbMessageIdMin()));
            pe.appendChild(fbMessageKeyMin);

            // fbMessgeKeyMin element
            Element fbMessageKeyMax = doc.createElement("fbMessageKeyMax");
            fbMessageKeyMax.appendChild(doc.createTextNode("" + partition.getFbMessageIdMax()));
            pe.appendChild(fbMessageKeyMax);

            // twMessgeKeyMin element
            Element twMessageKeyMin = doc.createElement("twMessageKeyMin");
            twMessageKeyMin.appendChild(doc.createTextNode("" + partition.getTwMessageIdMin()));
            pe.appendChild(twMessageKeyMin);

            // twMessgeKeyMin element
            Element twMessageKeyMax = doc.createElement("twMessageKeyMax");
            twMessageKeyMax.appendChild(doc.createTextNode("" + partition.getTwMessageIdMax()));
            pe.appendChild(twMessageKeyMax);

            // twMessgeKeyMin element
            Element numCommonUsers = doc.createElement("numCommonUsers");
            numCommonUsers.appendChild(doc.createTextNode("" + partition.getCommonUsers()));
            pe.appendChild(numCommonUsers);

        }

        public static void main(String args[]) throws Exception {
            String confFile = "/Users/rgrove1/work/research/asterix/icde/data-gen/conf/conf.xml";
            String outputPath = "/Users/rgrove1/work/research/asterix/icde/data-gen/output/conf-output.xml";
            Configuration conf = getConfiguration(confFile);
            writeToXML(conf, outputPath);
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

    public static class PartitionMetrics {

        private final int fbMessages;
        private final int twMessages;

        private final int fbOnlyUsers;
        private final int twitterOnlyUsers;
        private final int commonUsers;

        public PartitionMetrics(float number, String unit, int numPartitions) throws IOException {

            int factor = 0;
            if (unit.equalsIgnoreCase("MB")) {
                factor = 1024 * 1024;
            } else if (unit.equalsIgnoreCase("GB")) {
                factor = 1024 * 1024 * 1024;
            } else if (unit.equalsIgnoreCase("TB")) {
                factor = 1024 * 1024 * 1024 * 1024;
            } else
                throw new IOException("Invalid unit:" + unit);

            fbMessages = (int) (((number * factor * 0.80) / 258) / numPartitions);
            twMessages = (int) (fbMessages * 1.1 / 0.35);

            fbOnlyUsers = (int) ((number * factor * 0.20 * 0.0043)) / numPartitions;
            twitterOnlyUsers = (int) (0.25 * fbOnlyUsers);
            commonUsers = (int) (0.1 * fbOnlyUsers);
        }

        public int getFbMessages() {
            return fbMessages;
        }

        public int getTwMessages() {
            return twMessages;
        }

        public int getFbOnlyUsers() {
            return fbOnlyUsers;
        }

        public int getTwitterOnlyUsers() {
            return twitterOnlyUsers;
        }

        public int getCommonUsers() {
            return commonUsers;
        }

    }

    public static String[] lastNames = { "Hoopengarner", "Harrow", "Gardner", "Blyant", "Best", "Buttermore", "Gronko",
            "Mayers", "Countryman", "Neely", "Ruhl", "Taggart", "Bash", "Cason", "Hil", "Zalack", "Mingle", "Carr",
            "Rohtin", "Wardle", "Pullman", "Wire", "Kellogg", "Hiles", "Keppel", "Bratton", "Sutton", "Wickes",
            "Muller", "Friedline", "Llora", "Elizabeth", "Anderson", "Gaskins", "Rifler", "Vinsant", "Stanfield",
            "Black", "Guest", "Hujsak", "Carter", "Weidemann", "Hays", "Patton", "Hayhurst", "Paynter", "Cressman",
            "Fiddler", "Evans", "Sherlock", "Woodworth", "Jackson", "Bloise", "Schneider", "Ring", "Kepplinger",
            "James", "Moon", "Bennett", "Bashline", "Ryals", "Zeal", "Christman", "Milliron", "Nash", "Ewing", "Camp",
            "Mason", "Richardson", "Bowchiew", "Hahn", "Wilson", "Wood", "Toyley", "Williamson", "Lafortune", "Errett",
            "Saltser", "Hirleman", "Brindle", "Newbiggin", "Ulery", "Lambert", "Shick", "Kuster", "Moore", "Finck",
            "Powell", "Jolce", "Townsend", "Sauter", "Cowher", "Wolfe", "Cavalet", "Porter", "Laborde", "Ballou",
            "Murray", "Stoddard", "Pycroft", "Milne", "King", "Todd", "Staymates", "Hall", "Romanoff", "Keilbach",
            "Sandford", "Hamilton", "Fye", "Kline", "Weeks", "Mcelroy", "Mccullough", "Bryant", "Hill", "Moore",
            "Ledgerwood", "Prevatt", "Eckert", "Read", "Hastings", "Doverspike", "Allshouse", "Bryan", "Mccallum",
            "Lombardi", "Mckendrick", "Cattley", "Barkley", "Steiner", "Finlay", "Priebe", "Armitage", "Hall", "Elder",
            "Erskine", "Hatcher", "Walker", "Pearsall", "Dunkle", "Haile", "Adams", "Miller", "Newbern", "Basinger",
            "Fuhrer", "Brinigh", "Mench", "Blackburn", "Bastion", "Mccune", "Bridger", "Hynes", "Quinn", "Courtney",
            "Geddinge", "Field", "Seelig", "Cable", "Earhart", "Harshman", "Roby", "Beals", "Berry", "Reed", "Hector",
            "Pittman", "Haverrman", "Kalp", "Briner", "Joghs", "Cowart", "Close", "Wynne", "Harden", "Weldy",
            "Stephenson", "Hildyard", "Moberly", "Wells", "Mackendoerfer", "Fisher", "Oppie", "Oneal", "Churchill",
            "Keister", "Alice", "Tavoularis", "Fisher", "Hair", "Burns", "Veith", "Wile", "Fuller", "Fields", "Clark",
            "Randolph", "Stone", "Mcclymonds", "Holtzer", "Donkin", "Wilkinson", "Rosensteel", "Albright", "Stahl",
            "Fox", "Kadel", "Houser", "Hanseu", "Henderson", "Davis", "Bicknell", "Swain", "Mercer", "Holdeman",
            "Enderly", "Caesar", "Margaret", "Munshower", "Elless", "Lucy", "Feufer", "Schofield", "Graham",
            "Blatenberger", "Benford", "Akers", "Campbell", "Ann", "Sadley", "Ling", "Gongaware", "Schmidt", "Endsley",
            "Groah", "Flanders", "Reichard", "Lowstetter", "Sandblom", "Griffis", "Basmanoff", "Coveney", "Hawker",
            "Archibald", "Hutton", "Barnes", "Diegel", "Raybould", "Focell", "Breitenstein", "Murray", "Chauvin",
            "Busk", "Pheleps", "Teagarden", "Northey", "Baumgartner", "Fleming", "Harris", "Parkinson", "Carpenter",
            "Whirlow", "Bonner", "Wortman", "Rogers", "Scott", "Lowe", "Mckee", "Huston", "Bullard", "Throckmorton",
            "Rummel", "Mathews", "Dull", "Saline", "Tue", "Woolery", "Lalty", "Schrader", "Ramsey", "Eisenmann",
            "Philbrick", "Sybilla", "Wallace", "Fonblanque", "Paul", "Orbell", "Higgens", "Casteel", "Franks",
            "Demuth", "Eisenman", "Hay", "Robinson", "Fischer", "Hincken", "Wylie", "Leichter", "Bousum",
            "Littlefield", "Mcdonald", "Greif", "Rhodes", "Wall", "Steele", "Baldwin", "Smith", "Stewart", "Schere",
            "Mary", "Aultman", "Emrick", "Guess", "Mitchell", "Painter", "Aft", "Hasely", "Weldi", "Loewentsein",
            "Poorbaugh", "Kepple", "Noton", "Judge", "Jackson", "Style", "Adcock", "Diller", "Marriman", "Johnston",
            "Children", "Monahan", "Ehret", "Shaw", "Congdon", "Pinney", "Millard", "Crissman", "Tanner", "Rose",
            "Knisely", "Cypret", "Sommer", "Poehl", "Hardie", "Bender", "Overholt", "Gottwine", "Beach", "Leslie",
            "Trevithick", "Langston", "Magor", "Shotts", "Howe", "Hunter", "Cross", "Kistler", "Dealtry", "Christner",
            "Pennington", "Thorley", "Eckhardstein", "Van", "Stroh", "Stough", "Stall", "Beedell", "Shea", "Garland",
            "Mays", "Pritchard", "Frankenberger", "Rowley", "Lane", "Baum", "Alliman", "Park", "Jardine", "Butler",
            "Cherry", "Kooser", "Baxter", "Billimek", "Downing", "Hurst", "Wood", "Baird", "Watkins", "Edwards",
            "Kemerer", "Harding", "Owens", "Eiford", "Keener", "Garneis", "Fiscina", "Mang", "Draudy", "Mills",
            "Gibson", "Reese", "Todd", "Ramos", "Levett", "Wilks", "Ward", "Mosser", "Dunlap", "Kifer", "Christopher",
            "Ashbaugh", "Wynter", "Rawls", "Cribbs", "Haynes", "Thigpen", "Schreckengost", "Bishop", "Linton",
            "Chapman", "James", "Jerome", "Hook", "Omara", "Houston", "Maclagan", "Sandys", "Pickering", "Blois",
            "Dickson", "Kemble", "Duncan", "Woodward", "Southern", "Henley", "Treeby", "Cram", "Elsas", "Driggers",
            "Warrick", "Overstreet", "Hindman", "Buck", "Sulyard", "Wentzel", "Swink", "Butt", "Schaeffer",
            "Hoffhants", "Bould", "Willcox", "Lotherington", "Bagley", "Graff", "White", "Wheeler", "Sloan",
            "Rodacker", "Hanford", "Jowers", "Kunkle", "Cass", "Powers", "Gilman", "Mcmichaels", "Hobbs", "Herndon",
            "Prescott", "Smail", "Mcdonald", "Biery", "Orner", "Richards", "Mueller", "Isaman", "Bruxner", "Goodman",
            "Barth", "Turzanski", "Vorrasi", "Stainforth", "Nehling", "Rahl", "Erschoff", "Greene", "Mckinnon",
            "Reade", "Smith", "Pery", "Roose", "Greenwood", "Weisgarber", "Curry", "Holts", "Zadovsky", "Parrish",
            "Putnam", "Munson", "Mcindoe", "Nickolson", "Brooks", "Bollinger", "Stroble", "Siegrist", "Fulton",
            "Tomey", "Zoucks", "Roberts", "Otis", "Clarke", "Easter", "Johnson", "Fylbrigg", "Taylor", "Swartzbaugh",
            "Weinstein", "Gadow", "Sayre", "Marcotte", "Wise", "Atweeke", "Mcfall", "Napier", "Eisenhart", "Canham",
            "Sealis", "Baughman", "Gertraht", "Losey", "Laurence", "Eva", "Pershing", "Kern", "Pirl", "Rega",
            "Sanborn", "Kanaga", "Sanders", "Anderson", "Dickinson", "Osteen", "Gettemy", "Crom", "Snyder", "Reed",
            "Laurenzi", "Riggle", "Tillson", "Fowler", "Raub", "Jenner", "Koepple", "Soames", "Goldvogel", "Dimsdale",
            "Zimmer", "Giesen", "Baker", "Beail", "Mortland", "Bard", "Sanner", "Knopsnider", "Jenkins", "Bailey",
            "Werner", "Barrett", "Faust", "Agg", "Tomlinson", "Williams", "Little", "Greenawalt", "Wells", "Wilkins",
            "Gisiko", "Bauerle", "Harrold", "Prechtl", "Polson", "Faast", "Winton", "Garneys", "Peters", "Potter",
            "Porter", "Tennant", "Eve", "Dugger", "Jones", "Burch", "Cowper", "Whittier" };

    public static String[] firstNames = { "Albert", "Jacquelin", "Dona", "Alia", "Mayme", "Genoveva", "Emma", "Lena",
            "Melody", "Vilma", "Katelyn", "Jeremy", "Coral", "Leann", "Lita", "Gilda", "Kayla", "Alvina", "Maranda",
            "Verlie", "Khadijah", "Karey", "Patrice", "Kallie", "Corey", "Mollie", "Daisy", "Melanie", "Sarita",
            "Nichole", "Pricilla", "Terresa", "Berneice", "Arianne", "Brianne", "Lavinia", "Ulrike", "Lesha", "Adell",
            "Ardelle", "Marisha", "Laquita", "Karyl", "Maryjane", "Kendall", "Isobel", "Raeann", "Heike", "Barbera",
            "Norman", "Yasmine", "Nevada", "Mariam", "Edith", "Eugena", "Lovie", "Maren", "Bennie", "Lennie", "Tamera",
            "Crystal", "Randi", "Anamaria", "Chantal", "Jesenia", "Avis", "Shela", "Randy", "Laurena", "Sharron",
            "Christiane", "Lorie", "Mario", "Elizabeth", "Reina", "Adria", "Lakisha", "Brittni", "Azzie", "Dori",
            "Shaneka", "Asuncion", "Katheryn", "Laurice", "Sharita", "Krystal", "Reva", "Inger", "Alpha", "Makeda",
            "Anabel", "Loni", "Tiara", "Meda", "Latashia", "Leola", "Chin", "Daisey", "Ivory", "Amalia", "Logan",
            "Tyler", "Kyong", "Carolann", "Maryetta", "Eufemia", "Anya", "Doreatha", "Lorna", "Rutha", "Ehtel",
            "Debbie", "Chassidy", "Sang", "Christa", "Lottie", "Chun", "Karine", "Peggie", "Amina", "Melany", "Alayna",
            "Scott", "Romana", "Naomi", "Christiana", "Salena", "Taunya", "Mitsue", "Regina", "Chelsie", "Charity",
            "Dacia", "Aletha", "Latosha", "Lia", "Tamica", "Chery", "Bianca", "Shu", "Georgianne", "Myriam", "Austin",
            "Wan", "Mallory", "Jana", "Georgie", "Jenell", "Kori", "Vicki", "Delfina", "June", "Mellisa", "Catherina",
            "Claudie", "Tynisha", "Dayle", "Enriqueta", "Belen", "Pia", "Sarai", "Rosy", "Renay", "Kacie", "Frieda",
            "Cayla", "Elissa", "Claribel", "Sabina", "Mackenzie", "Raina", "Cira", "Mitzie", "Aubrey", "Serafina",
            "Maria", "Katharine", "Esperanza", "Sung", "Daria", "Billye", "Stefanie", "Kasha", "Holly", "Suzanne",
            "Inga", "Flora", "Andria", "Genevie", "Eladia", "Janet", "Erline", "Renna", "Georgeanna", "Delorse",
            "Elnora", "Rudy", "Rima", "Leanora", "Letisha", "Love", "Alverta", "Pinkie", "Domonique", "Jeannie",
            "Jose", "Jacqueline", "Tara", "Lily", "Erna", "Tennille", "Galina", "Tamala", "Kirby", "Nichelle",
            "Myesha", "Farah", "Santa", "Ludie", "Kenia", "Yee", "Micheline", "Maryann", "Elaina", "Ethelyn",
            "Emmaline", "Shanell", "Marina", "Nila", "Alane", "Shakira", "Dorris", "Belinda", "Elois", "Barbie",
            "Carita", "Gisela", "Lura", "Fransisca", "Helga", "Peg", "Leonarda", "Earlie", "Deetta", "Jacquetta",
            "Blossom", "Kayleigh", "Deloras", "Keshia", "Christinia", "Dulce", "Bernie", "Sheba", "Lashanda", "Tula",
            "Claretta", "Kary", "Jeanette", "Lupita", "Lenora", "Hisako", "Sherise", "Glynda", "Adela", "Chia",
            "Sudie", "Mindy", "Caroyln", "Lindsey", "Xiomara", "Mercedes", "Onie", "Loan", "Alexis", "Tommie",
            "Donette", "Monica", "Soo", "Camellia", "Lavera", "Valery", "Ariana", "Sophia", "Loris", "Ginette",
            "Marielle", "Tari", "Julissa", "Alesia", "Suzanna", "Emelda", "Erin", "Ladawn", "Sherilyn", "Candice",
            "Nereida", "Fairy", "Carl", "Joel", "Marilee", "Gracia", "Cordie", "So", "Shanita", "Drew", "Cassie",
            "Sherie", "Marget", "Norma", "Delois", "Debera", "Chanelle", "Catarina", "Aracely", "Carlene", "Tricia",
            "Aleen", "Katharina", "Marguerita", "Guadalupe", "Margorie", "Mandie", "Kathe", "Chong", "Sage", "Faith",
            "Maryrose", "Stephany", "Ivy", "Pauline", "Susie", "Cristen", "Jenifer", "Annette", "Debi", "Karmen",
            "Luci", "Shayla", "Hope", "Ocie", "Sharie", "Tami", "Breana", "Kerry", "Rubye", "Lashay", "Sondra",
            "Katrice", "Brunilda", "Cortney", "Yan", "Zenobia", "Penni", "Addie", "Lavona", "Noel", "Anika",
            "Herlinda", "Valencia", "Bunny", "Tory", "Victoria", "Carrie", "Mikaela", "Wilhelmina", "Chung",
            "Hortencia", "Gerda", "Wen", "Ilana", "Sibyl", "Candida", "Victorina", "Chantell", "Casie", "Emeline",
            "Dominica", "Cecila", "Delora", "Miesha", "Nova", "Sally", "Ronald", "Charlette", "Francisca", "Mina",
            "Jenna", "Loraine", "Felisa", "Lulu", "Page", "Lyda", "Babara", "Flor", "Walter", "Chan", "Sherika",
            "Kala", "Luna", "Vada", "Syreeta", "Slyvia", "Karin", "Renata", "Robbi", "Glenda", "Delsie", "Lizzie",
            "Genia", "Caitlin", "Bebe", "Cory", "Sam", "Leslee", "Elva", "Caren", "Kasie", "Leticia", "Shannan",
            "Vickey", "Sandie", "Kyle", "Chang", "Terrilyn", "Sandra", "Elida", "Marketta", "Elsy", "Tu", "Carman",
            "Ashlie", "Vernia", "Albertine", "Vivian", "Elba", "Bong", "Margy", "Janetta", "Xiao", "Teofila", "Danyel",
            "Nickole", "Aleisha", "Tera", "Cleotilde", "Dara", "Paulita", "Isela", "Maricela", "Rozella", "Marivel",
            "Aurora", "Melissa", "Carylon", "Delinda", "Marvella", "Candelaria", "Deidre", "Tawanna", "Myrtie",
            "Milagro", "Emilie", "Coretta", "Ivette", "Suzann", "Ammie", "Lucina", "Lory", "Tena", "Eleanor",
            "Cherlyn", "Tiana", "Brianna", "Myra", "Flo", "Carisa", "Kandi", "Erlinda", "Jacqulyn", "Fermina", "Riva",
            "Palmira", "Lindsay", "Annmarie", "Tamiko", "Carline", "Amelia", "Quiana", "Lashawna", "Veola", "Belva",
            "Marsha", "Verlene", "Alex", "Leisha", "Camila", "Mirtha", "Melva", "Lina", "Arla", "Cythia", "Towanda",
            "Aracelis", "Tasia", "Aurore", "Trinity", "Bernadine", "Farrah", "Deneen", "Ines", "Betty", "Lorretta",
            "Dorethea", "Hertha", "Rochelle", "Juli", "Shenika", "Yung", "Lavon", "Deeanna", "Nakia", "Lynnette",
            "Dinorah", "Nery", "Elene", "Carolee", "Mira", "Franchesca", "Lavonda", "Leida", "Paulette", "Dorine",
            "Allegra", "Keva", "Jeffrey", "Bernardina", "Maryln", "Yoko", "Faviola", "Jayne", "Lucilla", "Charita",
            "Ewa", "Ella", "Maggie", "Ivey", "Bettie", "Jerri", "Marni", "Bibi", "Sabrina", "Sarah", "Marleen",
            "Katherin", "Remona", "Jamika", "Antonina", "Oliva", "Lajuana", "Fonda", "Sigrid", "Yael", "Billi",
            "Verona", "Arminda", "Mirna", "Tesha", "Katheleen", "Bonita", "Kamilah", "Patrica", "Julio", "Shaina",
            "Mellie", "Denyse", "Deandrea", "Alena", "Meg", "Kizzie", "Krissy", "Karly", "Alleen", "Yahaira", "Lucie",
            "Karena", "Elaine", "Eloise", "Buena", "Marianela", "Renee", "Nan", "Carolynn", "Windy", "Avril", "Jane",
            "Vida", "Thea", "Marvel", "Rosaline", "Tifany", "Robena", "Azucena", "Carlota", "Mindi", "Andera", "Jenny",
            "Courtney", "Lyndsey", "Willette", "Kristie", "Shaniqua", "Tabatha", "Ngoc", "Una", "Marlena", "Louetta",
            "Vernie", "Brandy", "Jacquelyne", "Jenelle", "Elna", "Erminia", "Ida", "Audie", "Louis", "Marisol",
            "Shawana", "Harriette", "Karol", "Kitty", "Esmeralda", "Vivienne", "Eloisa", "Iris", "Jeanice", "Cammie",
            "Jacinda", "Shena", "Floy", "Theda", "Lourdes", "Jayna", "Marg", "Kati", "Tanna", "Rosalyn", "Maxima",
            "Soon", "Angelika", "Shonna", "Merle", "Kassandra", "Deedee", "Heidi", "Marti", "Renae", "Arleen",
            "Alfredia", "Jewell", "Carley", "Pennie", "Corina", "Tonisha", "Natividad", "Lilliana", "Darcie", "Shawna",
            "Angel", "Piedad", "Josefa", "Rebbeca", "Natacha", "Nenita", "Petrina", "Carmon", "Chasidy", "Temika",
            "Dennise", "Renetta", "Augusta", "Shirlee", "Valeri", "Casimira", "Janay", "Berniece", "Deborah", "Yaeko",
            "Mimi", "Digna", "Irish", "Cher", "Yong", "Lucila", "Jimmie", "Junko", "Lezlie", "Waneta", "Sandee",
            "Marquita", "Eura", "Freeda", "Annabell", "Laree", "Jaye", "Wendy", "Toshia", "Kylee", "Aleta", "Emiko",
            "Clorinda", "Sixta", "Audrea", "Juanita", "Birdie", "Reita", "Latanya", "Nia", "Leora", "Laurine",
            "Krysten", "Jerrie", "Chantel", "Ira", "Sena", "Andre", "Jann", "Marla", "Precious", "Katy", "Gabrielle",
            "Yvette", "Brook", "Shirlene", "Eldora", "Laura", "Milda", "Euna", "Jettie", "Debora", "Lise", "Edythe",
            "Leandra", "Shandi", "Araceli", "Johanne", "Nieves", "Denese", "Carmelita", "Nohemi", "Annice", "Natalie",
            "Yolande", "Jeffie", "Vashti", "Vickie", "Obdulia", "Youlanda", "Lupe", "Tomoko", "Monserrate", "Domitila",
            "Etsuko", "Adrienne", "Lakesha", "Melissia", "Odessa", "Meagan", "Veronika", "Jolyn", "Isabelle", "Leah",
            "Rhiannon", "Gianna", "Audra", "Sommer", "Renate", "Perla", "Thao", "Myong", "Lavette", "Mark", "Emilia",
            "Ariane", "Karl", "Dorie", "Jacquie", "Mia", "Malka", "Shenita", "Tashina", "Christine", "Cherri", "Roni",
            "Fran", "Mildred", "Sara", "Clarissa", "Fredia", "Elease", "Samuel", "Earlene", "Vernita", "Mae", "Concha",
            "Renea", "Tamekia", "Hye", "Ingeborg", "Tessa", "Kelly", "Kristin", "Tam", "Sacha", "Kanisha", "Jillian",
            "Tiffanie", "Ashlee", "Madelyn", "Donya", "Clementine", "Mickie", "My", "Zena", "Terrie", "Samatha",
            "Gertie", "Tarra", "Natalia", "Sharlene", "Evie", "Shalon", "Rosalee", "Numbers", "Jodi", "Hattie",
            "Naoma", "Valene", "Whitley", "Claude", "Alline", "Jeanne", "Camie", "Maragret", "Viola", "Kris", "Marlo",
            "Arcelia", "Shari", "Jalisa", "Corrie", "Eleonor", "Angelyn", "Merry", "Lauren", "Melita", "Gita",
            "Elenor", "Aurelia", "Janae", "Lyndia", "Margeret", "Shawanda", "Rolande", "Shirl", "Madeleine", "Celinda",
            "Jaleesa", "Shemika", "Joye", "Tisa", "Trudie", "Kathrine", "Clarita", "Dinah", "Georgia", "Antoinette",
            "Janis", "Suzette", "Sherri", "Herta", "Arie", "Hedy", "Cassi", "Audrie", "Caryl", "Jazmine", "Jessica",
            "Beverly", "Elizbeth", "Marylee", "Londa", "Fredericka", "Argelia", "Nana", "Donnette", "Damaris",
            "Hailey", "Jamee", "Kathlene", "Glayds", "Lydia", "Apryl", "Verla", "Adam", "Concepcion", "Zelda",
            "Shonta", "Vernice", "Detra", "Meghann", "Sherley", "Sheri", "Kiyoko", "Margarita", "Adaline", "Mariela",
            "Velda", "Ailene", "Juliane", "Aiko", "Edyth", "Cecelia", "Shavon", "Florance", "Madeline", "Rheba",
            "Deann", "Ignacia", "Odelia", "Heide", "Mica", "Jennette", "Maricruz", "Ouida", "Darcy", "Laure",
            "Justina", "Amada", "Laine", "Cruz", "Sunny", "Francene", "Roxanna", "Nam", "Nancie", "Deanna", "Letty",
            "Britni", "Kazuko", "Lacresha", "Simon", "Caleb", "Milton", "Colton", "Travis", "Miles", "Jonathan",
            "Logan", "Rolf", "Emilio", "Roberto", "Marcus", "Tim", "Delmar", "Devon", "Kurt", "Edward", "Jeffrey",
            "Elvis", "Alfonso", "Blair", "Wm", "Sheldon", "Leonel", "Michal", "Federico", "Jacques", "Leslie",
            "Augustine", "Hugh", "Brant", "Hong", "Sal", "Modesto", "Curtis", "Jefferey", "Adam", "John", "Glenn",
            "Vance", "Alejandro", "Refugio", "Lucio", "Demarcus", "Chang", "Huey", "Neville", "Preston", "Bert",
            "Abram", "Foster", "Jamison", "Kirby", "Erich", "Manual", "Dustin", "Derrick", "Donnie", "Jospeh", "Chris",
            "Josue", "Stevie", "Russ", "Stanley", "Nicolas", "Samuel", "Waldo", "Jake", "Max", "Ernest", "Reinaldo",
            "Rene", "Gale", "Morris", "Nathan", "Maximo", "Courtney", "Theodore", "Octavio", "Otha", "Delmer",
            "Graham", "Dean", "Lowell", "Myles", "Colby", "Boyd", "Adolph", "Jarrod", "Nick", "Mark", "Clinton", "Kim",
            "Sonny", "Dalton", "Tyler", "Jody", "Orville", "Luther", "Rubin", "Hollis", "Rashad", "Barton", "Vicente",
            "Ted", "Rick", "Carmine", "Clifton", "Gayle", "Christopher", "Jessie", "Bradley", "Clay", "Theo", "Josh",
            "Mitchell", "Boyce", "Chung", "Eugenio", "August", "Norbert", "Sammie", "Jerry", "Adan", "Edmundo",
            "Homer", "Hilton", "Tod", "Kirk", "Emmett", "Milan", "Quincy", "Jewell", "Herb", "Steve", "Carmen",
            "Bobby", "Odis", "Daron", "Jeremy", "Carl", "Hunter", "Tuan", "Thurman", "Asa", "Brenton", "Shane",
            "Donny", "Andreas", "Teddy", "Dario", "Cyril", "Hoyt", "Teodoro", "Vincenzo", "Hilario", "Daren",
            "Agustin", "Marquis", "Ezekiel", "Brendan", "Johnson", "Alden", "Richie", "Granville", "Chad", "Joseph",
            "Lamont", "Jordon", "Gilberto", "Chong", "Rosendo", "Eddy", "Rob", "Dewitt", "Andre", "Titus", "Russell",
            "Rigoberto", "Dick", "Garland", "Gabriel", "Hank", "Darius", "Ignacio", "Lazaro", "Johnie", "Mauro",
            "Edmund", "Trent", "Harris", "Osvaldo", "Marvin", "Judson", "Rodney", "Randall", "Renato", "Richard",
            "Denny", "Jon", "Doyle", "Cristopher", "Wilson", "Christian", "Jamie", "Roland", "Ken", "Tad", "Romeo",
            "Seth", "Quinton", "Byron", "Ruben", "Darrel", "Deandre", "Broderick", "Harold", "Ty", "Monroe", "Landon",
            "Mohammed", "Angel", "Arlen", "Elias", "Andres", "Carlton", "Numbers", "Tony", "Thaddeus", "Issac",
            "Elmer", "Antoine", "Ned", "Fermin", "Grover", "Benito", "Abdul", "Cortez", "Eric", "Maxwell", "Coy",
            "Gavin", "Rich", "Andy", "Del", "Giovanni", "Major", "Efren", "Horacio", "Joaquin", "Charles", "Noah",
            "Deon", "Pasquale", "Reed", "Fausto", "Jermaine", "Irvin", "Ray", "Tobias", "Carter", "Yong", "Jorge",
            "Brent", "Daniel", "Zane", "Walker", "Thad", "Shaun", "Jaime", "Mckinley", "Bradford", "Nathanial",
            "Jerald", "Aubrey", "Virgil", "Abel", "Philip", "Chester", "Chadwick", "Dominick", "Britt", "Emmitt",
            "Ferdinand", "Julian", "Reid", "Santos", "Dwain", "Morgan", "James", "Marion", "Micheal", "Eddie", "Brett",
            "Stacy", "Kerry", "Dale", "Nicholas", "Darrick", "Freeman", "Scott", "Newton", "Sherman", "Felton",
            "Cedrick", "Winfred", "Brad", "Fredric", "Dewayne", "Virgilio", "Reggie", "Edgar", "Heriberto", "Shad",
            "Timmy", "Javier", "Nestor", "Royal", "Lynn", "Irwin", "Ismael", "Jonas", "Wiley", "Austin", "Kieth",
            "Gonzalo", "Paris", "Earnest", "Arron", "Jarred", "Todd", "Erik", "Maria", "Chauncey", "Neil", "Conrad",
            "Maurice", "Roosevelt", "Jacob", "Sydney", "Lee", "Basil", "Louis", "Rodolfo", "Rodger", "Roman", "Corey",
            "Ambrose", "Cristobal", "Sylvester", "Benton", "Franklin", "Marcelo", "Guillermo", "Toby", "Jeramy",
            "Donn", "Danny", "Dwight", "Clifford", "Valentine", "Matt", "Jules", "Kareem", "Ronny", "Lonny", "Son",
            "Leopoldo", "Dannie", "Gregg", "Dillon", "Orlando", "Weston", "Kermit", "Damian", "Abraham", "Walton",
            "Adrian", "Rudolf", "Will", "Les", "Norberto", "Fred", "Tyrone", "Ariel", "Terry", "Emmanuel", "Anderson",
            "Elton", "Otis", "Derek", "Frankie", "Gino", "Lavern", "Jarod", "Kenny", "Dane", "Keenan", "Bryant",
            "Eusebio", "Dorian", "Ali", "Lucas", "Wilford", "Jeremiah", "Warner", "Woodrow", "Galen", "Bob",
            "Johnathon", "Amado", "Michel", "Harry", "Zachery", "Taylor", "Booker", "Hershel", "Mohammad", "Darrell",
            "Kyle", "Stuart", "Marlin", "Hyman", "Jeffery", "Sidney", "Merrill", "Roy", "Garrett", "Porter", "Kenton",
            "Giuseppe", "Terrance", "Trey", "Felix", "Buster", "Von", "Jackie", "Linwood", "Darron", "Francisco",
            "Bernie", "Diego", "Brendon", "Cody", "Marco", "Ahmed", "Antonio", "Vince", "Brooks", "Kendrick", "Ross",
            "Mohamed", "Jim", "Benny", "Gerald", "Pablo", "Charlie", "Antony", "Werner", "Hipolito", "Minh", "Mel",
            "Derick", "Armand", "Fidel", "Lewis", "Donnell", "Desmond", "Vaughn", "Guadalupe", "Keneth", "Rodrick",
            "Spencer", "Chas", "Gus", "Harlan", "Wes", "Carmelo", "Jefferson", "Gerard", "Jarvis", "Haywood", "Hayden",
            "Sergio", "Gene", "Edgardo", "Colin", "Horace", "Dominic", "Aldo", "Adolfo", "Juan", "Man", "Lenard",
            "Clement", "Everett", "Hal", "Bryon", "Mason", "Emerson", "Earle", "Laurence", "Columbus", "Lamar",
            "Douglas", "Ian", "Fredrick", "Marc", "Loren", "Wallace", "Randell", "Noble", "Ricardo", "Rory", "Lindsey",
            "Boris", "Bill", "Carlos", "Domingo", "Grant", "Craig", "Ezra", "Matthew", "Van", "Rudy", "Danial",
            "Brock", "Maynard", "Vincent", "Cole", "Damion", "Ellsworth", "Marcel", "Markus", "Rueben", "Tanner",
            "Reyes", "Hung", "Kennith", "Lindsay", "Howard", "Ralph", "Jed", "Monte", "Garfield", "Avery", "Bernardo",
            "Malcolm", "Sterling", "Ezequiel", "Kristofer", "Luciano", "Casey", "Rosario", "Ellis", "Quintin",
            "Trevor", "Miquel", "Jordan", "Arthur", "Carson", "Tyron", "Grady", "Walter", "Jonathon", "Ricky",
            "Bennie", "Terrence", "Dion", "Dusty", "Roderick", "Isaac", "Rodrigo", "Harrison", "Zack", "Dee", "Devin",
            "Rey", "Ulysses", "Clint", "Greg", "Dino", "Frances", "Wade", "Franklyn", "Jude", "Bradly", "Salvador",
            "Rocky", "Weldon", "Lloyd", "Milford", "Clarence", "Alec", "Allan", "Bobbie", "Oswaldo", "Wilfred",
            "Raleigh", "Shelby", "Willy", "Alphonso", "Arnoldo", "Robbie", "Truman", "Nicky", "Quinn", "Damien",
            "Lacy", "Marcos", "Parker", "Burt", "Carroll", "Denver", "Buck", "Dong", "Normand", "Billie", "Edwin",
            "Troy", "Arden", "Rusty", "Tommy", "Kenneth", "Leo", "Claud", "Joel", "Kendall", "Dante", "Milo", "Cruz",
            "Lucien", "Ramon", "Jarrett", "Scottie", "Deshawn", "Ronnie", "Pete", "Alonzo", "Whitney", "Stefan",
            "Sebastian", "Edmond", "Enrique", "Branden", "Leonard", "Loyd", "Olin", "Ron", "Rhett", "Frederic",
            "Orval", "Tyrell", "Gail", "Eli", "Antonia", "Malcom", "Sandy", "Stacey", "Nickolas", "Hosea", "Santo",
            "Oscar", "Fletcher", "Dave", "Patrick", "Dewey", "Bo", "Vito", "Blaine", "Randy", "Robin", "Winston",
            "Sammy", "Edwardo", "Manuel", "Valentin", "Stanford", "Filiberto", "Buddy", "Zachariah", "Johnnie",
            "Elbert", "Paul", "Isreal", "Jerrold", "Leif", "Owen", "Sung", "Junior", "Raphael", "Josef", "Donte",
            "Allen", "Florencio", "Raymond", "Lauren", "Collin", "Eliseo", "Bruno", "Martin", "Lyndon", "Kurtis",
            "Salvatore", "Erwin", "Michael", "Sean", "Davis", "Alberto", "King", "Rolland", "Joe", "Tory", "Chase",
            "Dallas", "Vernon", "Beau", "Terrell", "Reynaldo", "Monty", "Jame", "Dirk", "Florentino", "Reuben", "Saul",
            "Emory", "Esteban", "Michale", "Claudio", "Jacinto", "Kelley", "Levi", "Andrea", "Lanny", "Wendell",
            "Elwood", "Joan", "Felipe", "Palmer", "Elmo", "Lawrence", "Hubert", "Rudolph", "Duane", "Cordell",
            "Everette", "Mack", "Alan", "Efrain", "Trenton", "Bryan", "Tom", "Wilmer", "Clyde", "Chance", "Lou",
            "Brain", "Justin", "Phil", "Jerrod", "George", "Kris", "Cyrus", "Emery", "Rickey", "Lincoln", "Renaldo",
            "Mathew", "Luke", "Dwayne", "Alexis", "Jackson", "Gil", "Marty", "Burton", "Emil", "Glen", "Willian",
            "Clemente", "Keven", "Barney", "Odell", "Reginald", "Aurelio", "Damon", "Ward", "Gustavo", "Harley",
            "Peter", "Anibal", "Arlie", "Nigel", "Oren", "Zachary", "Scot", "Bud", "Wilbert", "Bart", "Josiah",
            "Marlon", "Eldon", "Darryl", "Roger", "Anthony", "Omer", "Francis", "Patricia", "Moises", "Chuck",
            "Waylon", "Hector", "Jamaal", "Cesar", "Julius", "Rex", "Norris", "Ollie", "Isaias", "Quentin", "Graig",
            "Lyle", "Jeffry", "Karl", "Lester", "Danilo", "Mike", "Dylan", "Carlo", "Ryan", "Leon", "Percy", "Lucius",
            "Jamel", "Lesley", "Joey", "Cornelius", "Rico", "Arnulfo", "Chet", "Margarito", "Ernie", "Nathanael",
            "Amos", "Cleveland", "Luigi", "Alfonzo", "Phillip", "Clair", "Elroy", "Alva", "Hans", "Shon", "Gary",
            "Jesus", "Cary", "Silas", "Keith", "Israel", "Willard", "Randolph", "Dan", "Adalberto", "Claude",
            "Delbert", "Garry", "Mary", "Larry", "Riley", "Robt", "Darwin", "Barrett", "Steven", "Kelly", "Herschel",
            "Darnell", "Scotty", "Armando", "Miguel", "Lawerence", "Wesley", "Garth", "Carol", "Micah", "Alvin",
            "Billy", "Earl", "Pat", "Brady", "Cory", "Carey", "Bernard", "Jayson", "Nathaniel", "Gaylord", "Archie",
            "Dorsey", "Erasmo", "Angelo", "Elisha", "Long", "Augustus", "Hobert", "Drew", "Stan", "Sherwood",
            "Lorenzo", "Forrest", "Shawn", "Leigh", "Hiram", "Leonardo", "Gerry", "Myron", "Hugo", "Alvaro", "Leland",
            "Genaro", "Jamey", "Stewart", "Elden", "Irving", "Olen", "Antone", "Freddy", "Lupe", "Joshua", "Gregory",
            "Andrew", "Sang", "Wilbur", "Gerardo", "Merlin", "Williams", "Johnny", "Alex", "Tommie", "Jimmy",
            "Donovan", "Dexter", "Gaston", "Tracy", "Jeff", "Stephen", "Berry", "Anton", "Darell", "Fritz", "Willis",
            "Noel", "Mariano", "Crawford", "Zoey", "Alex", "Brianna", "Carlie", "Lloyd", "Cal", "Astor", "Randolf",
            "Magdalene", "Trevelyan", "Terance", "Roy", "Kermit", "Harriett", "Crystal", "Laurinda", "Kiersten",
            "Phyllida", "Liz", "Bettie", "Rena", "Colten", "Berenice", "Sindy", "Wilma", "Amos", "Candi", "Ritchie",
            "Dirk", "Kathlyn", "Callista", "Anona", "Flossie", "Sterling", "Calista", "Regan", "Erica", "Jeana",
            "Keaton", "York", "Nolan", "Daniel", "Benton", "Tommie", "Serenity", "Deanna", "Chas", "Heron", "Marlyn",
            "Xylia", "Tristin", "Lyndon", "Andriana", "Madelaine", "Maddison", "Leila", "Chantelle", "Audrey",
            "Connor", "Daley", "Tracee", "Tilda", "Eliot", "Merle", "Linwood", "Kathryn", "Silas", "Alvina",
            "Phinehas", "Janis", "Alvena", "Zubin", "Gwendolen", "Caitlyn", "Bertram", "Hailee", "Idelle", "Homer",
            "Jannah", "Delbert", "Rhianna", "Cy", "Jefferson", "Wayland", "Nona", "Tempest", "Reed", "Jenifer",
            "Ellery", "Nicolina", "Aldous", "Prince", "Lexia", "Vinnie", "Doug", "Alberic", "Kayleen", "Woody",
            "Rosanne", "Ysabel", "Skyler", "Twyla", "Geordie", "Leta", "Clive", "Aaron", "Scottie", "Celeste", "Chuck",
            "Erle", "Lallie", "Jaycob", "Ray", "Carrie", "Laurita", "Noreen", "Meaghan", "Ulysses", "Andy", "Drogo",
            "Dina", "Yasmin", "Mya", "Luvenia", "Urban", "Jacob", "Laetitia", "Sherry", "Love", "Michaela", "Deonne",
            "Summer", "Brendon", "Sheena", "Mason", "Jayson", "Linden", "Salal", "Darrell", "Diana", "Hudson",
            "Lennon", "Isador", "Charley", "April", "Ralph", "James", "Mina", "Jolyon", "Laurine", "Monna", "Carita",
            "Munro", "Elsdon", "Everette", "Radclyffe", "Darrin", "Herbert", "Gawain", "Sheree", "Trudy", "Emmaline",
            "Kassandra", "Rebecca", "Basil", "Jen", "Don", "Osborne", "Lilith", "Hannah", "Fox", "Rupert", "Paulene",
            "Darius", "Wally", "Baptist", "Sapphire", "Tia", "Sondra", "Kylee", "Ashton", "Jepson", "Joetta", "Val",
            "Adela", "Zacharias", "Zola", "Marmaduke", "Shannah", "Posie", "Oralie", "Brittany", "Ernesta", "Raymund",
            "Denzil", "Daren", "Roosevelt", "Nelson", "Fortune", "Mariel", "Nick", "Jaden", "Upton", "Oz", "Margaux",
            "Precious", "Albert", "Bridger", "Jimmy", "Nicola", "Rosalynne", "Keith", "Walt", "Della", "Joanna",
            "Xenia", "Esmeralda", "Major", "Simon", "Rexana", "Stacy", "Calanthe", "Sherley", "Kaitlyn", "Graham",
            "Ramsey", "Abbey", "Madlyn", "Kelvin", "Bill", "Rue", "Monica", "Caileigh", "Laraine", "Booker", "Jayna",
            "Greta", "Jervis", "Sherman", "Kendrick", "Tommy", "Iris", "Geffrey", "Kaelea", "Kerr", "Garrick", "Jep",
            "Audley", "Nic", "Bronte", "Beulah", "Patricia", "Jewell", "Deidra", "Cory", "Everett", "Harper",
            "Charity", "Godfrey", "Jaime", "Sinclair", "Talbot", "Dayna", "Cooper", "Rosaline", "Jennie", "Eileen",
            "Latanya", "Corinna", "Roxie", "Caesar", "Charles", "Pollie", "Lindsey", "Sorrel", "Dwight", "Jocelyn",
            "Weston", "Shyla", "Valorie", "Bessie", "Josh", "Lessie", "Dayton", "Kathi", "Chasity", "Wilton", "Adam",
            "William", "Ash", "Angela", "Ivor", "Ria", "Jazmine", "Hailey", "Jo", "Silvestra", "Ernie", "Clifford",
            "Levi", "Matilda", "Quincey", "Camilla", "Delicia", "Phemie", "Laurena", "Bambi", "Lourdes", "Royston",
            "Chastity", "Lynwood", "Elle", "Brenda", "Phoebe", "Timothy", "Raschelle", "Lilly", "Burt", "Rina",
            "Rodney", "Maris", "Jaron", "Wilf", "Harlan", "Audra", "Vincent", "Elwyn", "Drew", "Wynter", "Ora",
            "Lissa", "Virgil", "Xavier", "Chad", "Ollie", "Leyton", "Karolyn", "Skye", "Roni", "Gladys", "Dinah",
            "Penny", "August", "Osmund", "Whitaker", "Brande", "Cornell", "Phil", "Zara", "Kilie", "Gavin", "Coty",
            "Randy", "Teri", "Keira", "Pru", "Clemency", "Kelcey", "Nevil", "Poppy", "Gareth", "Christabel", "Bastian",
            "Wynonna", "Roselyn", "Goddard", "Collin", "Trace", "Neal", "Effie", "Denys", "Virginia", "Richard",
            "Isiah", "Harrietta", "Gaylord", "Diamond", "Trudi", "Elaine", "Jemmy", "Gage", "Annabel", "Quincy", "Syd",
            "Marianna", "Philomena", "Aubree", "Kathie", "Jacki", "Kelley", "Bess", "Cecil", "Maryvonne", "Kassidy",
            "Anselm", "Dona", "Darby", "Jamison", "Daryl", "Darell", "Teal", "Lennie", "Bartholomew", "Katie",
            "Maybelline", "Kimball", "Elvis", "Les", "Flick", "Harley", "Beth", "Bidelia", "Montague", "Helen", "Ozzy",
            "Stef", "Debra", "Maxene", "Stefanie", "Russ", "Avril", "Johnathan", "Orson", "Chelsey", "Josephine",
            "Deshaun", "Wendell", "Lula", "Ferdinanda", "Greg", "Brad", "Kynaston", "Dena", "Russel", "Robertina",
            "Misti", "Leon", "Anjelica", "Bryana", "Myles", "Judi", "Curtis", "Davin", "Kristia", "Chrysanta",
            "Hayleigh", "Hector", "Osbert", "Eustace", "Cary", "Tansy", "Cayley", "Maryann", "Alissa", "Ike",
            "Tranter", "Reina", "Alwilda", "Sidony", "Columbine", "Astra", "Jillie", "Stephania", "Jonah", "Kennedy",
            "Ferdinand", "Allegria", "Donella", "Kelleigh", "Darian", "Eldreda", "Jayden", "Herbie", "Jake", "Winston",
            "Vi", "Annie", "Cherice", "Hugo", "Tricia", "Haydee", "Cassarah", "Darden", "Mallory", "Alton", "Hadley",
            "Romayne", "Lacey", "Ern", "Alayna", "Cecilia", "Seward", "Tilly", "Edgar", "Concordia", "Ibbie", "Dahlia",
            "Oswin", "Stu", "Brett", "Maralyn", "Kristeen", "Dotty", "Robyn", "Nessa", "Tresha", "Guinevere",
            "Emerson", "Haze", "Lyn", "Henderson", "Lexa", "Jaylen", "Gail", "Lizette", "Tiara", "Robbie", "Destiny",
            "Alice", "Livia", "Rosy", "Leah", "Jan", "Zach", "Vita", "Gia", "Micheal", "Rowina", "Alysha", "Bobbi",
            "Delores", "Osmond", "Karaugh", "Wilbur", "Kasandra", "Renae", "Kaety", "Dora", "Gaye", "Amaryllis",
            "Katelyn", "Dacre", "Prudence", "Ebony", "Camron", "Jerrold", "Vivyan", "Randall", "Donna", "Misty",
            "Damon", "Selby", "Esmund", "Rian", "Garry", "Julius", "Raelene", "Clement", "Dom", "Tibby", "Moss",
            "Millicent", "Gwendoline", "Berry", "Ashleigh", "Lilac", "Quin", "Vere", "Creighton", "Harriet", "Malvina",
            "Lianne", "Pearle", "Kizzie", "Kara", "Petula", "Jeanie", "Maria", "Pacey", "Victoria", "Huey", "Toni",
            "Rose", "Wallis", "Diggory", "Josiah", "Delma", "Keysha", "Channing", "Prue", "Lee", "Ryan", "Sidney",
            "Valerie", "Clancy", "Ezra", "Gilbert", "Clare", "Laz", "Crofton", "Mike", "Annabella", "Tara", "Eldred",
            "Arthur", "Jaylon", "Peronel", "Paden", "Dot", "Marian", "Amyas", "Alexus", "Esmond", "Abbie", "Stanley",
            "Brittani", "Vickie", "Errol", "Kimberlee", "Uland", "Ebenezer", "Howie", "Eveline", "Andrea", "Trish",
            "Hopkin", "Bryanna", "Temperance", "Valarie", "Femie", "Alix", "Terrell", "Lewin", "Lorrin", "Happy",
            "Micah", "Rachyl", "Sloan", "Gertrude", "Elizabeth", "Dorris", "Andra", "Bram", "Gary", "Jeannine",
            "Maurene", "Irene", "Yolonda", "Jonty", "Coleen", "Cecelia", "Chantal", "Stuart", "Caris", "Ros",
            "Kaleigh", "Mirabelle", "Kolby", "Primrose", "Susannah", "Ginny", "Jinny", "Dolly", "Lettice", "Sonny",
            "Melva", "Ernest", "Garret", "Reagan", "Trenton", "Gallagher", "Edwin", "Nikolas", "Corrie", "Lynette",
            "Ettie", "Sly", "Debbi", "Eudora", "Brittney", "Tacey", "Marius", "Anima", "Gordon", "Olivia", "Kortney",
            "Shantel", "Kolleen", "Nevaeh", "Buck", "Sera", "Liliana", "Aric", "Kalyn", "Mick", "Libby", "Ingram",
            "Alexandria", "Darleen", "Jacklyn", "Hughie", "Tyler", "Aida", "Ronda", "Deemer", "Taryn", "Laureen",
            "Samantha", "Dave", "Hardy", "Baldric", "Montgomery", "Gus", "Ellis", "Titania", "Luke", "Chase", "Haidee",
            "Mayra", "Isabell", "Trinity", "Milo", "Abigail", "Tacita", "Meg", "Hervey", "Natasha", "Sadie", "Holden",
            "Dee", "Mansel", "Perry", "Randi", "Frederica", "Georgina", "Kolour", "Debbie", "Seraphina", "Elspet",
            "Julyan", "Raven", "Zavia", "Jarvis", "Jaymes", "Grover", "Cairo", "Alea", "Jordon", "Braxton", "Donny",
            "Rhoda", "Tonya", "Bee", "Alyssia", "Ashlyn", "Reanna", "Lonny", "Arlene", "Deb", "Jane", "Nikole",
            "Bettina", "Harrison", "Tamzen", "Arielle", "Adelaide", "Faith", "Bridie", "Wilburn", "Fern", "Nan",
            "Shaw", "Zeke", "Alan", "Dene", "Gina", "Alexa", "Bailey", "Sal", "Tammy", "Maximillian", "America",
            "Sylvana", "Fitz", "Mo", "Marissa", "Cass", "Eldon", "Wilfrid", "Tel", "Joann", "Kendra", "Tolly",
            "Leanne", "Ferdie", "Haven", "Lucas", "Marlee", "Cyrilla", "Red", "Phoenix", "Jazmin", "Carin", "Gena",
            "Lashonda", "Tucker", "Genette", "Kizzy", "Winifred", "Melody", "Keely", "Kaylyn", "Radcliff", "Lettie",
            "Foster", "Lyndsey", "Nicholas", "Farley", "Louisa", "Dana", "Dortha", "Francine", "Doran", "Bonita",
            "Hal", "Sawyer", "Reginald", "Aislin", "Nathan", "Baylee", "Abilene", "Ladonna", "Maurine", "Shelly",
            "Deandre", "Jasmin", "Roderic", "Tiffany", "Amanda", "Verity", "Wilford", "Gayelord", "Whitney", "Demelza",
            "Kenton", "Alberta", "Kyra", "Tabitha", "Sampson", "Korey", "Lillian", "Edison", "Clayton", "Steph",
            "Maya", "Dusty", "Jim", "Ronny", "Adrianne", "Bernard", "Harris", "Kiley", "Alexander", "Kisha", "Ethalyn",
            "Patience", "Briony", "Indigo", "Aureole", "Makenzie", "Molly", "Sherilyn", "Barry", "Laverne", "Hunter",
            "Rocky", "Tyreek", "Madalyn", "Phyliss", "Chet", "Beatrice", "Faye", "Lavina", "Madelyn", "Tracey",
            "Gyles", "Patti", "Carlyn", "Stephanie", "Jackalyn", "Larrie", "Kimmy", "Isolda", "Emelina", "Lis",
            "Zillah", "Cody", "Sheard", "Rufus", "Paget", "Mae", "Rexanne", "Luvinia", "Tamsen", "Rosanna", "Greig",
            "Stacia", "Mabelle", "Quianna", "Lotus", "Delice", "Bradford", "Angus", "Cosmo", "Earlene", "Adrian",
            "Arlie", "Noelle", "Sabella", "Isa", "Adelle", "Innocent", "Kirby", "Trixie", "Kenelm", "Nelda", "Melia",
            "Kendal", "Dorinda", "Placid", "Linette", "Kam", "Sherisse", "Evan", "Ewart", "Janice", "Linton",
            "Jacaline", "Charissa", "Douglas", "Aileen", "Kemp", "Oli", "Amethyst", "Rosie", "Nigella", "Sherill",
            "Anderson", "Alanna", "Eric", "Claudia", "Jennifer", "Boniface", "Harriet", "Vernon", "Lucy", "Shawnee",
            "Gerard", "Cecily", "Romey", "Randall", "Wade", "Lux", "Dawson", "Gregg", "Kade", "Roxanne", "Melinda",
            "Rolland", "Rowanne", "Fannie", "Isidore", "Melia", "Harvie", "Salal", "Eleonor", "Jacquette", "Lavone",
            "Shanika", "Tarquin", "Janet", "Josslyn", "Maegan", "Augusta", "Aubree", "Francene", "Martie", "Marisa",
            "Tyreek", "Tatianna", "Caleb", "Sheridan", "Nellie", "Barbara", "Wat", "Jayla", "Esmaralda", "Graeme",
            "Lavena", "Jemima", "Nikolas", "Triston", "Portia", "Kyla", "Marcus", "Raeburn", "Jamison", "Earl", "Wren",
            "Leighton", "Lagina", "Lucasta", "Dina", "Amaranta", "Jessika", "Claud", "Bernard", "Winifred", "Ebba",
            "Sammi", "Gall", "Chloe", "Ottoline", "Herbert", "Janice", "Gareth", "Channing", "Caleigh", "Kailee",
            "Ralphie", "Tamzen", "Quincy", "Beaumont", "Albert", "Jadyn", "Violet", "Luanna", "Moriah", "Humbert",
            "Jed", "Leona", "Hale", "Mitch", "Marlin", "Nivek", "Darwin", "Dirk", "Liliana", "Meadow", "Bernadine",
            "Jorie", "Peyton", "Astra", "Roscoe", "Gina", "Lovell", "Jewel", "Romayne", "Rosy", "Imogene",
            "Margaretta", "Lorinda", "Hopkin", "Bobby", "Flossie", "Bennie", "Horatio", "Jonah", "Lyn", "Deana",
            "Juliana", "Blanch", "Wright", "Kendal", "Woodrow", "Tania", "Austyn", "Val", "Mona", "Charla", "Rudyard",
            "Pamela", "Raven", "Zena", "Nicola", "Kaelea", "Conor", "Virgil", "Sonnie", "Goodwin", "Christianne",
            "Linford", "Myron", "Denton", "Charita", "Brody", "Ginnie", "Harrison", "Jeanine", "Quin", "Isolda",
            "Zoie", "Pearce", "Margie", "Larrie", "Angelina", "Marcia", "Jessamine", "Delilah", "Dick", "Luana",
            "Delicia", "Lake", "Luvenia", "Vaughan", "Concordia", "Gayelord", "Cheyenne", "Felix", "Dorris", "Pen",
            "Kristeen", "Parris", "Everitt", "Josephina", "Amy", "Tommie", "Adrian", "April", "Rosaline", "Zachery",
            "Trace", "Phoebe", "Jenelle", "Kameron", "Katharine", "Media", "Colton", "Tad", "Quianna", "Kerenza",
            "Greta", "Luvinia", "Pete", "Tonya", "Beckah", "Barbra", "Jon", "Tetty", "Corey", "Sylvana", "Kizzy",
            "Korey", "Trey", "Haydee", "Penny", "Mandy", "Panda", "Coline", "Ramsey", "Sukie", "Annabel", "Sarina",
            "Corbin", "Suzanna", "Rob", "Duana", "Shell", "Jason", "Eddy", "Rube", "Roseann", "Celia", "Brianne",
            "Nerissa", "Jera", "Humphry", "Ashlynn", "Terrence", "Philippina", "Coreen", "Kolour", "Indiana", "Paget",
            "Marlyn", "Hester", "Isbel", "Ocean", "Harris", "Leslie", "Vere", "Monroe", "Isabelle", "Bertie", "Clitus",
            "Dave", "Alethea", "Lessie", "Louiza", "Madlyn", "Garland", "Wolf", "Lalo", "Donny", "Amabel", "Tianna",
            "Louie", "Susie", "Mackenzie", "Renie", "Tess", "Marmaduke", "Gwendolen", "Bettina", "Beatrix", "Esmund",
            "Minnie", "Carlie", "Barnabas", "Ruthie", "Honour", "Haylie", "Xavior", "Freddie", "Ericka", "Aretha",
            "Edie", "Madelina", "Anson", "Tabby", "Derrick", "Jocosa", "Deirdre", "Aislin", "Chastity", "Abigail",
            "Wynonna", "Zo", "Eldon", "Krystine", "Ghislaine", "Zavia", "Nolene", "Marigold", "Kelley", "Sylvester",
            "Odell", "George", "Laurene", "Franklyn", "Clarice", "Mo", "Dustin", "Debbi", "Lina", "Tony", "Acacia",
            "Hettie", "Natalee", "Marcie", "Brittany", "Elnora", "Rachel", "Dawn", "Basil", "Christal", "Anjelica",
            "Fran", "Tawny", "Delroy", "Tameka", "Lillie", "Ceara", "Deanna", "Deshaun", "Ken", "Bradford", "Justina",
            "Merle", "Draven", "Gretta", "Harriette", "Webster", "Nathaniel", "Anemone", "Coleen", "Ruth", "Chryssa",
            "Hortensia", "Saffie", "Deonne", "Leopold", "Harlan", "Lea", "Eppie", "Lucinda", "Tilda", "Fanny", "Titty",
            "Lockie", "Jepson", "Sherisse", "Maralyn", "Ethel", "Sly", "Ebenezer", "Canute", "Ella", "Freeman",
            "Reuben", "Olivette", "Nona", "Rik", "Amice", "Kristine", "Kathie", "Jayne", "Jeri", "Mckenna", "Bertram",
            "Kaylee", "Livia", "Gil", "Wallace", "Maryann", "Keeleigh", "Laurinda", "Doran", "Khloe", "Dakota",
            "Yaron", "Kimberleigh", "Gytha", "Doris", "Marylyn", "Benton", "Linnette", "Esther", "Jakki", "Rowina",
            "Marian", "Roselyn", "Norbert", "Maggie", "Caesar", "Phinehas", "Jerry", "Jasmine", "Antonette", "Miriam",
            "Monna", "Maryvonne", "Jacquetta", "Bernetta", "Napier", "Annie", "Gladwin", "Sheldon", "Aric", "Elouise",
            "Gawain", "Kristia", "Gabe", "Kyra", "Red", "Tod", "Dudley", "Lorraine", "Ryley", "Sabina", "Poppy",
            "Leland", "Aileen", "Eglantine", "Alicia", "Jeni", "Addy", "Tiffany", "Geffrey", "Lavina", "Collin",
            "Clover", "Vin", "Jerome", "Doug", "Vincent", "Florence", "Scarlet", "Celeste", "Desdemona", "Tiphanie",
            "Kassandra", "Ashton", "Madison", "Art", "Magdalene", "Iona", "Josepha", "Anise", "Ferne", "Derek",
            "Huffie", "Qiana", "Ysabel", "Tami", "Shannah", "Xavier", "Willard", "Winthrop", "Vickie", "Maura",
            "Placid", "Tiara", "Reggie", "Elissa", "Isa", "Chrysanta", "Jeff", "Bessie", "Terri", "Amilia", "Brett",
            "Daniella", "Damion", "Carolina", "Maximillian", "Travers", "Benjamin", "Oprah", "Darcy", "Yolanda",
            "Nicolina", "Crofton", "Jarrett", "Kaitlin", "Shauna", "Keren", "Bevis", "Kalysta", "Sharron", "Alyssa",
            "Blythe", "Zelma", "Caelie", "Norwood", "Billie", "Patrick", "Gary", "Cambria", "Tylar", "Mason", "Helen",
            "Melyssa", "Gene", "Gilberta", "Carter", "Herbie", "Harmonie", "Leola", "Eugenia", "Clint", "Pauletta",
            "Edwyna", "Georgina", "Teal", "Harper", "Izzy", "Dillon", "Kezia", "Evangeline", "Colene", "Madelaine",
            "Zilla", "Rudy", "Dottie", "Caris", "Morton", "Marge", "Tacey", "Parker", "Troy", "Liza", "Lewin",
            "Tracie", "Justine", "Dallas", "Linden", "Ray", "Loretta", "Teri", "Elvis", "Diane", "Julianna", "Manfred",
            "Denise", "Eireen", "Ann", "Kenith", "Linwood", "Kathlyn", "Bernice", "Shelley", "Oswald", "Amedeus",
            "Homer", "Tanzi", "Ted", "Ralphina", "Hyacinth", "Lotus", "Matthias", "Arlette", "Clark", "Cecil",
            "Elspeth", "Alvena", "Noah", "Millard", "Brenden", "Cole", "Philipa", "Nina", "Thelma", "Iantha", "Reid",
            "Jefferson", "Meg", "Elsie", "Shirlee", "Nathan", "Nancy", "Simona", "Racheal", "Carin", "Emory", "Delice",
            "Kristi", "Karaugh", "Kaety", "Tilly", "Em", "Alanis", "Darrin", "Jerrie", "Hollis", "Cary", "Marly",
            "Carita", "Jody", "Farley", "Hervey", "Rosalin", "Cuthbert", "Stewart", "Jodene", "Caileigh", "Briscoe",
            "Dolores", "Sheree", "Eustace", "Nigel", "Detta", "Barret", "Rowland", "Kenny", "Githa", "Zoey", "Adela",
            "Petronella", "Opal", "Coleman", "Niles", "Cyril", "Dona", "Alberic", "Allannah", "Jules", "Avalon",
            "Hadley", "Thomas", "Renita", "Calanthe", "Heron", "Shawnda", "Chet", "Malina", "Manny", "Rina", "Frieda",
            "Eveleen", "Deshawn", "Amos", "Raelene", "Paige", "Molly", "Nannie", "Ileen", "Brendon", "Milford",
            "Unice", "Rebeccah", "Caedmon", "Gae", "Doreen", "Vivian", "Louis", "Raphael", "Vergil", "Lise", "Glenn",
            "Karyn", "Terance", "Reina", "Jake", "Gordon", "Wisdom", "Isiah", "Gervase", "Fern", "Marylou", "Roddy",
            "Justy", "Derick", "Shantelle", "Adam", "Chantel", "Madoline", "Emmerson", "Lexie", "Mickey", "Stephen",
            "Dane", "Stacee", "Elwin", "Tracey", "Alexandra", "Ricky", "Ian", "Kasey", "Rita", "Alanna", "Georgene",
            "Deon", "Zavier", "Ophelia", "Deforest", "Lowell", "Zubin", "Hardy", "Osmund", "Tabatha", "Debby",
            "Katlyn", "Tallulah", "Priscilla", "Braden", "Wil", "Keziah", "Jen", "Aggie", "Korbin", "Lemoine",
            "Barnaby", "Tranter", "Goldie", "Roderick", "Trina", "Emery", "Pris", "Sidony", "Adelle", "Tate", "Wilf",
            "Zola", "Brande", "Chris", "Calanthia", "Lilly", "Kaycee", "Lashonda", "Jasmin", "Elijah", "Shantel",
            "Simon", "Rosalind", "Jarod", "Kaylie", "Corrine", "Joselyn", "Archibald", "Mariabella", "Winton",
            "Merlin", "Chad", "Ursula", "Kristopher", "Hewie", "Adrianna", "Lyndsay", "Jasmyn", "Tim", "Evette",
            "Margaret", "Samson", "Bronte", "Terence", "Leila", "Candice", "Tori", "Jamey", "Coriander", "Conrad",
            "Floyd", "Karen", "Lorin", "Maximilian", "Cairo", "Emily", "Yasmin", "Karolyn", "Bryan", "Lanny",
            "Kimberly", "Rick", "Chaz", "Krystle", "Lyric", "Laura", "Garrick", "Flip", "Monty", "Brendan",
            "Ermintrude", "Rayner", "Merla", "Titus", "Marva", "Patricia", "Leone", "Tracy", "Jaqueline", "Hallam",
            "Delores", "Cressida", "Carlyle", "Leann", "Kelcey", "Laurence", "Ryan", "Reynold", "Mark", "Collyn",
            "Audie", "Sammy", "Ellery", "Sallie", "Pamelia", "Adolph", "Lydia", "Titania", "Ron", "Bridger", "Aline",
            "Read", "Kelleigh", "Weldon", "Irving", "Garey", "Diggory", "Evander", "Kylee", "Deidre", "Ormond",
            "Laurine", "Reannon", "Arline", "Pat"

    };

    public static String[] jargon = { "wireless", "signal", "network", "3G", "plan", "touch-screen",
            "customer-service", "reachability", "voice-command", "shortcut-menu", "customization", "platform", "speed",
            "voice-clarity", "voicemail-service" };

    public static String[] vendors = { "at&t", "verizon", "t-mobile", "sprint", "motorola", "samsung", "iphone" };

    public static String[] org_list = { "Latsonity", "ganjalax", "Zuncan", "Lexitechno", "Hot-tech", "subtam",
            "Coneflex", "Ganjatax", "physcane", "Tranzap", "Qvohouse", "Zununoing", "jaydax", "Keytech", "goldendexon",
            "Villa-tech", "Trustbam", "Newcom", "Voltlane", "Ontohothex", "Ranhotfan", "Alphadax", "Transhigh",
            "kin-ron", "Doublezone", "Solophase", "Vivaace", "silfind", "Basecone", "sonstreet", "Freshfix",
            "Techitechi", "Kanelectrics", "linedexon", "Goldcity", "Newfase", "Technohow", "Zimcone", "Salthex",
            "U-ron", "Solfix", "whitestreet", "Xx-technology", "Hexviafind", "over-it", "Strongtone", "Tripplelane",
            "geomedia", "Scotcity", "Inchex", "Vaiatech", "Striptaxon", "Hatcom", "tresline", "Sanjodax", "freshdox",
            "Sumlane", "Quadlane", "Newphase", "overtech", "Voltbam", "Icerunin", "Fixdintex", "Hexsanhex", "Statcode",
            "Greencare", "U-electrics", "Zamcorporation", "Ontotanin", "Tanzimcare", "Groovetex", "Ganjastrip",
            "Redelectronics", "Dandamace", "Whitemedia", "strongex", "Streettax", "highfax", "Mathtech", "Xx-drill",
            "Sublamdox", "Unijobam", "Rungozoom", "Fixelectrics", "Villa-dox", "Ransaofan", "Plexlane", "itlab",
            "Lexicone", "Fax-fax", "Viatechi", "Inchdox", "Kongreen", "Doncare", "Y-geohex", "Opeelectronics",
            "Medflex", "Dancode", "Roundhex", "Labzatron", "Newhotplus", "Sancone", "Ronholdings", "Quoline",
            "zoomplus", "Fix-touch", "Codetechno", "Tanzumbam", "Indiex", "Canline" };
}
