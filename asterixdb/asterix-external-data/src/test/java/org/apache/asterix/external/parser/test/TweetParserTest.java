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

package org.apache.asterix.external.parser.test;

import static org.apache.asterix.om.types.BuiltinType.AFLOAT;
import static org.apache.asterix.om.types.BuiltinType.AINT64;
import static org.apache.asterix.om.types.BuiltinType.AMISSING;
import static org.apache.asterix.om.types.BuiltinType.ANULL;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.parser.TweetParser;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Assert;
import org.junit.Test;

import junit.extensions.PA;

public class TweetParserTest {

    @Test
    public void openRecordTypeTest() throws IOException, URISyntaxException {
        String[] ids = { "720549057849114629", "668950503552864257", "668945640186101761", "263602997047730177",
                "668948268605403136", "741701526859567104" };
        // contruct type
        IAType geoFieldType = new ARecordType("GeoType", new String[] { "coordinates" },
                new IAType[] { new AOrderedListType(AFLOAT, "point") }, true);
        List<IAType> unionTypeList = new ArrayList<>();
        unionTypeList.add(geoFieldType);
        unionTypeList.add(ANULL);
        unionTypeList.add(AMISSING);
        IAType geoUnionType = new AUnionType(unionTypeList, "GeoType?");
        ARecordType tweetRecordType =
                new ARecordType("TweetType", new String[] { "id", "geo" }, new IAType[] { AINT64, geoUnionType }, true);

        TweetParser parser = new TweetParser(tweetRecordType);
        CharArrayRecord record = new CharArrayRecord();

        List<String> lines = Files.readAllLines(Paths.get(getClass().getResource("/test_tweets.txt").toURI()));
        ByteArrayOutputStream is = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(is);
        for (int iter1 = 0; iter1 < lines.size(); iter1++) {
            record.set(lines.get(iter1));
            try {
                parser.parse(record, output);
            } catch (HyracksDataException e) {
                e.printStackTrace();
                Assert.fail("Unexpected failure in parser.");
            }
            Assert.assertTrue((PA.getValue(parser, "aInt64")).toString().equals(ids[iter1]));
        }
    }

    @Test
    public void closedRecordTypeTest() throws IOException, URISyntaxException {
        // contruct type
        IAType geoFieldType = new ARecordType("GeoType", new String[] { "coordinates" },
                new IAType[] { new AOrderedListType(AFLOAT, "point") }, true);
        ARecordType tweetRecordType =
                new ARecordType("TweetType", new String[] { "id", "geo" }, new IAType[] { AINT64, geoFieldType }, true);

        TweetParser parser = new TweetParser(tweetRecordType);
        List<String> lines = Files.readAllLines(Paths.get(getClass().getResource("/test_tweets.txt").toURI()));
        ByteArrayOutputStream is = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(is);
        CharArrayRecord record = new CharArrayRecord();
        int regularCount = 0;
        for (int iter1 = 0; iter1 < lines.size(); iter1++) {
            record.set(lines.get(iter1));
            try {
                parser.parse(record, output);
                regularCount++;
            } catch (HyracksDataException e) {
                Assert.assertTrue(e.toString().contains("Non-null") && (iter1 == 0 || iter1 == 1));
            }
        }
        Assert.assertTrue(regularCount == 4);
    }
}
