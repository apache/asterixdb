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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.external.parser.ADMDataParser;
import org.apache.asterix.external.parser.AbstractDataParser;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AGeometry;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableDate;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.base.AMutableTime;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.junit.Assert;
import org.junit.Test;

import com.esri.core.geometry.ogc.OGCPoint;

public class ADMDataParserTest {

    @Test
    public void test() throws IOException, NoSuchMethodException, SecurityException, NoSuchFieldException {
        char[][] dates = toChars(
                new String[] { "-9537-08-04", "9656-06-03", "-9537-04-04", "9656-06-04", "-9537-10-04", "9626-09-05" });
        AMutableDate[] parsedDates =
                new AMutableDate[] { new AMutableDate(-4202630), new AMutableDate(2807408), new AMutableDate(-4202752),
                        new AMutableDate(2807409), new AMutableDate(-4202569), new AMutableDate(2796544), };

        char[][] times = toChars(new String[] { "12:04:45.689Z", "12:41:59.002Z", "12:10:45.169Z", "15:37:48.736Z",
                "04:16:42.321Z", "12:22:56.816Z" });
        AMutableTime[] parsedTimes =
                new AMutableTime[] { new AMutableTime(43485689), new AMutableTime(45719002), new AMutableTime(43845169),
                        new AMutableTime(56268736), new AMutableTime(15402321), new AMutableTime(44576816), };

        char[][] dateTimes = toChars(
                new String[] { "-2640-10-11T17:32:15.675Z", "4104-02-01T05:59:11.902Z", "0534-12-08T08:20:31.487Z",
                        "6778-02-16T22:40:21.653Z", "2129-12-12T13:18:35.758Z", "8647-07-01T13:10:19.691Z" });
        AMutableDateTime[] parsedDateTimes =
                new AMutableDateTime[] { new AMutableDateTime(-145452954464325L), new AMutableDateTime(67345192751902L),
                        new AMutableDateTime(-45286270768513L), new AMutableDateTime(151729886421653L),
                        new AMutableDateTime(5047449515758L), new AMutableDateTime(210721439419691L) };

        Method parseDateMethod = AbstractDataParser.class.getDeclaredMethod("parseDate", char[].class, int.class,
                int.class, DataOutput.class);
        parseDateMethod.setAccessible(true);

        Field aDateField = AbstractDataParser.class.getDeclaredField("aDate");
        aDateField.setAccessible(true);

        Method parseTimeMethod = AbstractDataParser.class.getDeclaredMethod("parseTime", char[].class, int.class,
                int.class, DataOutput.class);
        parseTimeMethod.setAccessible(true);

        Field aTimeField = AbstractDataParser.class.getDeclaredField("aTime");
        aTimeField.setAccessible(true);

        Method parseDateTimeMethod = AbstractDataParser.class.getDeclaredMethod("parseDateTime", char[].class,
                int.class, int.class, DataOutput.class);
        parseDateTimeMethod.setAccessible(true);

        Field aDateTimeField = AbstractDataParser.class.getDeclaredField("aDateTime");
        aDateTimeField.setAccessible(true);

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
                                parseDateMethod.invoke(parser, dates[index], 0, dates[index].length, dos);
                                AMutableDate aDate = (AMutableDate) aDateField.get(parser);
                                Assert.assertTrue(aDate.equals(parsedDates[index]));
                            }

                            // Tests parseTime.
                            for (int index = 0; index < times.length; ++index) {
                                parseTimeMethod.invoke(parser, times[index], 0, times[index].length, dos);
                                AMutableTime aTime = (AMutableTime) aTimeField.get(parser);
                                Assert.assertTrue(aTime.equals(parsedTimes[index]));
                            }

                            // Tests parseDateTime.
                            for (int index = 0; index < dateTimes.length; ++index) {
                                parseDateTimeMethod.invoke(parser, dateTimes[index], 0, dateTimes[index].length, dos);
                                AMutableDateTime aDateTime = (AMutableDateTime) aDateTimeField.get(parser);
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

    private char[][] toChars(String[] strings) {
        char[][] results = new char[strings.length][];
        for (int i = 0; i < strings.length; i++) {
            results[i] = strings[i].toCharArray();
        }
        return results;
    }

    @Test
    public void testWKTParser() {
        try {
            ARecordType recordType = new ARecordType("POIType", new String[] { "id", "coord" },
                    new IAType[] { BuiltinType.AINT32, BuiltinType.AGEOMETRY }, false);

            String wktObject = "{\"id\": 123, \"coord\": \"POINT(3 4)\"}";
            InputStream in = new ByteArrayInputStream(wktObject.getBytes());
            ADMDataParser parser = new ADMDataParser(recordType, true);
            parser.setInputStream(in);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            parser.parse(out);
            out.close();
            byte[] serialized = baos.toByteArray();

            // Parse to make sure it was correct
            ByteBuffer bb = ByteBuffer.wrap(serialized);
            Assert.assertEquals(ATypeTag.SERIALIZED_RECORD_TYPE_TAG, bb.get());
            Assert.assertEquals(serialized.length, bb.getInt()); // Total record size including header
            Assert.assertEquals(2, bb.getInt()); // # of records
            int offsetOfID = bb.getInt();
            int offsetOfGeometry = bb.getInt();
            ISerializerDeserializer intDeser =
                    SerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(BuiltinType.AINT32);
            Assert.assertEquals(offsetOfID, bb.position());
            // Serialize the two records
            DataInputByteBuffer dataIn = new DataInputByteBuffer();
            dataIn.reset(bb);
            Object o = intDeser.deserialize(dataIn);
            Assert.assertEquals(new AInt32(123), o);
            ISerializerDeserializer geomDeser =
                    SerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(BuiltinType.AGEOMETRY);
            Object point = geomDeser.deserialize(dataIn);
            Assert.assertTrue(point instanceof AGeometry);
            Assert.assertTrue(((AGeometry) point).getGeometry() instanceof OGCPoint);
            OGCPoint p = (OGCPoint) ((AGeometry) point).getGeometry();
            Assert.assertEquals(3.0, p.X(), 1E-5);
            Assert.assertEquals(4.0, p.Y(), 1E-5);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Error in parsing");
        }

    }
}
