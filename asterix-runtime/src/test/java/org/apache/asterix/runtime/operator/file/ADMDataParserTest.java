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
package org.apache.asterix.runtime.operator.file;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.om.base.AMutableInterval;
import org.apache.asterix.runtime.operators.file.ADMDataParser;
import org.junit.Assert;
import org.junit.Test;

import junit.extensions.PA;

public class ADMDataParserTest {

    @Test
    public void test() {
        String[] dateIntervals = { "-9537-08-04, 9656-06-03", "-9537-04-04, 9656-06-04", "-9537-10-04, 9626-09-05" };
        AMutableInterval[] parsedDateIntervals = new AMutableInterval[] {
                new AMutableInterval(-4202630, 2807408, (byte) 17), new AMutableInterval(-4202752, 2807409, (byte) 17),
                new AMutableInterval(-4202569, 2796544, (byte) 17), };

        String[] timeIntervals = { "12:04:45.689Z, 12:41:59.002Z", "12:10:45.169Z, 15:37:48.736Z",
                "04:16:42.321Z, 12:22:56.816Z" };
        AMutableInterval[] parsedTimeIntervals = new AMutableInterval[] {
                new AMutableInterval(43485689, 45719002, (byte) 18),
                new AMutableInterval(43845169, 56268736, (byte) 18),
                new AMutableInterval(15402321, 44576816, (byte) 18), };

        String[] dateTimeIntervals = { "-2640-10-11T17:32:15.675Z, 4104-02-01T05:59:11.902Z",
                "0534-12-08T08:20:31.487Z, 6778-02-16T22:40:21.653Z",
                "2129-12-12T13:18:35.758Z, 8647-07-01T13:10:19.691Z" };
        AMutableInterval[] parsedDateTimeIntervals = new AMutableInterval[] {
                new AMutableInterval(-145452954464325L, 67345192751902L, (byte) 16),
                new AMutableInterval(-45286270768513L, 151729886421653L, (byte) 16),
                new AMutableInterval(5047449515758L, 210721439419691L, (byte) 16) };

        Thread[] threads = new Thread[16];
        AtomicInteger errorCount = new AtomicInteger(0);
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(new Runnable() {
                ADMDataParser parser = new ADMDataParser();
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutput dos = new DataOutputStream(bos);

                @Override
                public void run() {
                    try {
                        int round = 0;
                        while (round++ < 10000) {
                            // Test parseDateInterval.
                            for (int index = 0; index < dateIntervals.length; ++index) {
                                PA.invokeMethod(parser, "parseDateInterval(java.lang.String, java.io.DataOutput)",
                                        dateIntervals[index], dos);
                                AMutableInterval aInterval = (AMutableInterval) PA.getValue(parser, "aInterval");
                                Assert.assertTrue(aInterval.equals(parsedDateIntervals[index]));
                            }

                            // Tests parseTimeInterval.
                            for (int index = 0; index < timeIntervals.length; ++index) {
                                PA.invokeMethod(parser, "parseTimeInterval(java.lang.String, java.io.DataOutput)",
                                        timeIntervals[index], dos);
                                AMutableInterval aInterval = (AMutableInterval) PA.getValue(parser, "aInterval");
                                Assert.assertTrue(aInterval.equals(parsedTimeIntervals[index]));
                            }

                            // Tests parseDateTimeInterval.
                            for (int index = 0; index < dateTimeIntervals.length; ++index) {
                                PA.invokeMethod(parser, "parseDateTimeInterval(java.lang.String, java.io.DataOutput)",
                                        dateTimeIntervals[index], dos);
                                AMutableInterval aInterval = (AMutableInterval) PA.getValue(parser, "aInterval");
                                Assert.assertTrue(aInterval.equals(parsedDateTimeIntervals[index]));
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
