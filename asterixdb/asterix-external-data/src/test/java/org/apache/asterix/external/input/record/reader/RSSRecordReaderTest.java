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
package org.apache.asterix.external.input.record.reader;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.reader.rss.RSSRecordReader;
import org.junit.Assert;
import org.junit.Test;

import com.rometools.rome.feed.synd.SyndEntry;

public class RSSRecordReaderTest {
    @Test
    public void fetchFromDummyWebsite() throws MalformedURLException {
        String dummyRssFeedURL = "http://foobar";
        RSSRecordReader rssRecordReader = new RSSRecordReader(dummyRssFeedURL);
        Exception expectedException = null;
        try {
            rssRecordReader.next();
        } catch (IOException e) {
            expectedException = e;
        }
        Assert.assertNotNull(expectedException);
        Assert.assertTrue(expectedException.getMessage().contains("UnknownHostException"));
    }

    private static final int NO_RECORDS = 10;

    public void fetchFromLoremWebsite() throws MalformedURLException {
        String dummyRssFeedURL = "http://lorem-rss.herokuapp.com/feed";
        RSSRecordReader rssRecordReader = new RSSRecordReader(dummyRssFeedURL);
        Exception expectedException = null;
        int cnt = 0;
        try {
            while (rssRecordReader.hasNext() && cnt < NO_RECORDS) {
                IRawRecord<SyndEntry> rec = rssRecordReader.next();
                ++cnt;
                Assert.assertTrue(rec.get().getTitle().startsWith("Lorem ipsum"));
            }
        } catch (Exception e) {
            expectedException = e;
        }
        Assert.assertEquals(NO_RECORDS, cnt);
        Assert.assertNull(expectedException);
    }
}
