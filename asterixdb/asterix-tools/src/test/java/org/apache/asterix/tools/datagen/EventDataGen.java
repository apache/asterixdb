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
package org.apache.asterix.tools.datagen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class EventDataGen {
    private static final String FIRST_NAMES_FILE_NAME = "/opt/us_census_names/dist.all.first.cleaned";
    private static final String LAST_NAMES_FILE_NAME = "/opt/us_census_names/dist.all.last.cleaned";

    private String[] firstNames = new String[10];
    private String[] lastNames = new String[10];

    private static final int MIN_USER_INTERESTS = 0;
    private static final int MAX_USER_INTERESTS = 7;
    private String[] INTERESTS = { "bass", "music", "databases", "fishing", "tennis", "squash", "computers", "books",
            "movies", "cigars", "wine", "running", "walking", "skiing", "basketball", "video games", "cooking",
            "coffee", "base jumping", "puzzles" };

    private static final String[] STREETS = { "Main St.", "Oak St.", "7th St.", "Washington St.", "Cedar St.",
            "Lake St.", "Hill St.", "Park St.", "View St." };
    private static final int MIN_STREET_NUM = 1;
    private static final int MAX_STREET_NUM = 10000;
    private static final String[] CITIES =
            { "Seattle", "Irvine", "Laguna Beach", "Los Angeles", "San Clemente", "Huntington Beach", "Portland" };
    private static final int MIN_ZIP = 100000;
    private static final int MAX_ZIP = 999999;
    private static final String[] LAT_LONGS =
            { "47,-122", "33,-117", "33,-117", "34,-118", "33,-117", "33,-117", "45,-122" };

    private static final int MIN_MEMBERSHIPS = 1;
    private static final int MAX_MEMBERSHIPS = 10;
    private static final int MIN_SIG_ID = 1;
    private static final int MAX_SIG_ID = 100;
    private static final String[] CHAPTER_NAMES = { "Seattle", "Irvine", "Laguna Beach", "Los Angeles", "San Clemente",
            "Huntington Beach", "Portland", "Newport Beach", "Kirkland" };
    private static final int MEMBER_SINCE_MIN_YEAR = 1970;
    private static final int MEMBER_SINCE_MAX_YEAR = 1998;

    private Random rndValue = new Random(50);
    private User user;

    private final class User {
        private int firstNameIdx;
        private int lastNameIdx;
        private int[] interests = new int[MAX_USER_INTERESTS - MIN_USER_INTERESTS];
        int numInterests;
        private int streetNumber;
        private String street;
        private String city;
        private int zip;
        private String latlong;
        int numMemberships;
        private int[] member_sigid = new int[MAX_MEMBERSHIPS];
        private String[] member_chap_name = new String[MAX_MEMBERSHIPS];
        private String[] member_since_date = new String[MAX_MEMBERSHIPS];

        public void generateFieldValues() {
            firstNameIdx = Math.abs(rndValue.nextInt()) % firstNames.length;
            lastNameIdx = Math.abs(rndValue.nextInt()) % lastNames.length;
            // name = firstNames[firstNameIx] + " " + lastNames[lastNameIx];
            numInterests =
                    Math.abs((rndValue.nextInt()) % (MAX_USER_INTERESTS - MIN_USER_INTERESTS)) + MIN_USER_INTERESTS;
            for (int i = 0; i < numInterests; i++) {
                interests[i] = Math.abs(rndValue.nextInt()) % INTERESTS.length;
            }
            streetNumber = Math.abs(rndValue.nextInt()) % (MAX_STREET_NUM - MIN_STREET_NUM) + MIN_STREET_NUM;
            street = STREETS[Math.abs(rndValue.nextInt()) % STREETS.length];
            int cityIdx = Math.abs(rndValue.nextInt()) % CITIES.length;
            city = CITIES[cityIdx];
            zip = Math.abs(rndValue.nextInt() % (MAX_ZIP - MIN_ZIP)) + MIN_ZIP;
            latlong = LAT_LONGS[cityIdx];
            numMemberships = Math.abs(rndValue.nextInt()) % (MAX_MEMBERSHIPS - MIN_MEMBERSHIPS) + MIN_MEMBERSHIPS;
            for (int i = 0; i < numMemberships; i++) {
                member_sigid[i] = Math.abs(rndValue.nextInt()) % (MAX_SIG_ID - MIN_SIG_ID) + MIN_SIG_ID;
                int cnIdx = Math.abs(rndValue.nextInt()) % CHAPTER_NAMES.length;
                member_chap_name[i] = CHAPTER_NAMES[cnIdx];
                int msYear = Math.abs(rndValue.nextInt()) % (MEMBER_SINCE_MAX_YEAR - MEMBER_SINCE_MIN_YEAR)
                        + MEMBER_SINCE_MIN_YEAR;
                int msMo = Math.abs(rndValue.nextInt()) % 12 + 1;
                int msDay = Math.abs(rndValue.nextInt()) % 28 + 1;
                member_since_date[i] =
                        msYear + "-" + (msMo < 10 ? "0" : "") + msMo + "-" + (msDay < 10 ? "0" : "") + msDay;
            }
        }

        public void write(Writer writer) throws IOException {
            writer.append("{");
            writer.append(" \"name\": \"");
            writer.append(firstNames[firstNameIdx]);
            writer.append(" ");
            writer.append(lastNames[lastNameIdx]);
            writer.append("\", ");

            writer.append(" \"email\": \"");
            writer.append(firstNames[firstNameIdx]);
            writer.append(".");
            writer.append(lastNames[lastNameIdx]);
            writer.append("@example.com\", ");

            writer.append(" \"interests\": <");
            for (int i = 0; i < numInterests; i++) {
                if (i > 0) {
                    writer.append(", ");
                }
                writer.append("\"");
                writer.append(INTERESTS[interests[i]]);
                writer.append("\"");
            }
            writer.append(">, ");

            writer.append(" \"address\": {");
            writer.append(" \"street\": \"");
            writer.append(streetNumber + " " + street);
            writer.append("\",");
            writer.append(" \"city\": \"");
            writer.append(city);
            writer.append("\",");
            writer.append(" \"zip\": \"");
            writer.append(zip + "\",");
            writer.append(" \"latlong\": point(\"");
            writer.append(latlong);
            writer.append("\")");
            writer.append("}, ");

            writer.append(" \"member_of\": <");
            for (int i = 0; i < numMemberships; i++) {
                if (i > 0) {
                    writer.append(", ");
                }
                writer.append("{");
                writer.append(" \"sig_id\": ");
                writer.append(member_sigid[i] + ",");
                writer.append(" \"chapter_name\": \"");
                writer.append(member_chap_name[i]);
                writer.append("\",");
                writer.append(" \"member_since\": date(\"");
                writer.append(member_since_date[i]);
                writer.append("\") }");
            }
            writer.append(">");

            writer.append(" }\n");
        }
    }

    public void init() throws IOException {
        {
            List<String> tmpFirstNames = new ArrayList<String>();

            FileInputStream fstream = new FileInputStream(FIRST_NAMES_FILE_NAME);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            while ((strLine = br.readLine()) != null) {
                String firstLetter = strLine.substring(0, 1);
                String remainder = strLine.substring(1);
                String capitalized = firstLetter.toUpperCase() + remainder.toLowerCase();
                tmpFirstNames.add(capitalized);
            }
            in.close();
            firstNames = tmpFirstNames.toArray(firstNames);
        }
        {
            List<String> tmpLastNames = new ArrayList<String>();

            FileInputStream fstream = new FileInputStream(LAST_NAMES_FILE_NAME);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            while ((strLine = br.readLine()) != null) {
                String firstLetter = strLine.substring(0, 1);
                String remainder = strLine.substring(1);
                String capitalized = firstLetter.toUpperCase() + remainder.toLowerCase();
                tmpLastNames.add(capitalized);
            }
            in.close();
            lastNames = tmpLastNames.toArray(firstNames);
        }
        user = new User();
    }

    public void generate() {
        user.generateFieldValues();
    }

    public void write(Writer w) throws IOException {
        user.write(w);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println(
                    "MUST PROVIDE 2 PARAMETERS, 1. output directory path and 2. number of records to generate.");
            System.exit(1);
        }
        String outputFile = args[0];
        int numRecords = Integer.parseInt(args[1]);
        Writer userFile = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(outputFile + File.separator + "user.adm")));
        EventDataGen dgen = new EventDataGen();
        dgen.init();
        for (int i = 0; i < numRecords; i++) {
            dgen.generate();
            dgen.write(userFile);
        }
        userFile.close();
    }
}
