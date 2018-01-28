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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.external.parser.ADMDataParser;
import org.apache.asterix.om.base.AMutableDate;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.base.AMutableTime;
import org.junit.Assert;
import org.junit.Test;

import junit.extensions.PA;

public class ADMDataParserTest {

    @Test
    public void test() throws IOException {
        String[] dates = { "-9537-08-04", "9656-06-03", "-9537-04-04", "9656-06-04", "-9537-10-04", "9626-09-05" };
        AMutableDate[] parsedDates =
                new AMutableDate[] { new AMutableDate(-4202630), new AMutableDate(2807408), new AMutableDate(-4202752),
                        new AMutableDate(2807409), new AMutableDate(-4202569), new AMutableDate(2796544), };

        String[] times = { "12:04:45.689Z", "12:41:59.002Z", "12:10:45.169Z", "15:37:48.736Z", "04:16:42.321Z",
                "12:22:56.816Z" };
        AMutableTime[] parsedTimes =
                new AMutableTime[] { new AMutableTime(43485689), new AMutableTime(45719002), new AMutableTime(43845169),
                        new AMutableTime(56268736), new AMutableTime(15402321), new AMutableTime(44576816), };

        String[] dateTimes = { "-2640-10-11T17:32:15.675Z", "4104-02-01T05:59:11.902Z", "0534-12-08T08:20:31.487Z",
                "6778-02-16T22:40:21.653Z", "2129-12-12T13:18:35.758Z", "8647-07-01T13:10:19.691Z" };
        AMutableDateTime[] parsedDateTimes =
                new AMutableDateTime[] { new AMutableDateTime(-145452954464325L), new AMutableDateTime(67345192751902L),
                        new AMutableDateTime(-45286270768513L), new AMutableDateTime(151729886421653L),
                        new AMutableDateTime(5047449515758L), new AMutableDateTime(210721439419691L) };

        Thread[] threads = new Thread[16];
        AtomicInteger errorCount = new AtomicInteger(0);
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(new Runnable() {
                ADMDataParser parser = new ADMDataParser(null, true);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutput dos = new DataOutputStream(bos);

                @Override
                public void run() {
                    try {
                        int round = 0;
                        while (round++ < 10000) {
                            // Test parseDate.
                            for (int index = 0; index < dates.length; ++index) {
                                PA.invokeMethod(parser, "parseDate(java.lang.String, java.io.DataOutput)", dates[index],
                                        dos);
                                AMutableDate aDate = (AMutableDate) PA.getValue(parser, "aDate");
                                Assert.assertTrue(aDate.equals(parsedDates[index]));
                            }

                            // Tests parseTime.
                            for (int index = 0; index < times.length; ++index) {
                                PA.invokeMethod(parser, "parseTime(java.lang.String, java.io.DataOutput)", times[index],
                                        dos);
                                AMutableTime aTime = (AMutableTime) PA.getValue(parser, "aTime");
                                Assert.assertTrue(aTime.equals(parsedTimes[index]));
                            }

                            // Tests parseDateTime.
                            for (int index = 0; index < dateTimes.length; ++index) {
                                PA.invokeMethod(parser, "parseDateTime(java.lang.String, java.io.DataOutput)",
                                        dateTimes[index], dos);
                                AMutableDateTime aDateTime = (AMutableDateTime) PA.getValue(parser, "aDateTime");
                                Assert.assertTrue(aDateTime.equals(parsedDateTimes[index]));
                            }
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        e.printStackTrace();
                    }
                }
            });
            // Kicks off test threads.
            threads[i].start();
        }

        // Joins all the threads.
        try {
            for (int i = 0; i < threads.length; ++i) {
                threads[i].join();
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        // Asserts no failure.
        Assert.assertTrue(errorCount.get() == 0);
    }
}
