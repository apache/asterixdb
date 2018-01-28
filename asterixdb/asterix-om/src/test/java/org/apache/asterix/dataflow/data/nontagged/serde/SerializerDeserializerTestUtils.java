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
package org.apache.asterix.dataflow.data.nontagged.serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.junit.Assert;

public class SerializerDeserializerTestUtils {

    public static ARecordType generateAddressRecordType() {
        String[] addrFieldNames = new String[] { "line", "city", "state", "postcode", "duration" };
        IAType[] addrFieldTypes = new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                BuiltinType.AINT16, BuiltinType.AINTERVAL };
        return new ARecordType("addritem", addrFieldNames, addrFieldTypes, true);
    }

    public static ARecordType generateEmployeeRecordType(ARecordType addrRecordType) {
        AOrderedListType addrListType = new AOrderedListType(addrRecordType, "address_list");
        String[] fieldNames = new String[] { "id", "name", "addresses_history" };
        IAType[] fieldTypes = new IAType[] { BuiltinType.AINT64, BuiltinType.ASTRING, addrListType };
        return new ARecordType("employee", fieldNames, fieldTypes, true);
    }

    public static ARecord[] generateRecords(ARecordType addrRecordType, ARecordType employeeType) {
        AOrderedListType addrListType = new AOrderedListType(addrRecordType, "address_history");
        ARecord addr11 = new ARecord(addrRecordType, new IAObject[] { new AString("120 San Raman Street"),
                new AString("Irvine"), new AString("CA"), new AInt16((short) 95051), new AInterval(0, 100, (byte) 0) });
        ARecord addr12 = new ARecord(addrRecordType,
                new IAObject[] { new AString("210 University Drive"), new AString("Philadelphia"), new AString("PA"),
                        new AInt16((short) 10086), new AInterval(100, 300, (byte) 0) });
        ARecord addr21 =
                new ARecord(addrRecordType, new IAObject[] { new AString("1 College Street"), new AString("Seattle"),
                        new AString("WA"), new AInt16((short) 20012), new AInterval(400, 500, (byte) 0) });
        ARecord addr22 =
                new ARecord(addrRecordType, new IAObject[] { new AString("20 Lindsay Avenue"), new AString("Columbus"),
                        new AString("OH"), new AInt16((short) 30120), new AInterval(600, 900, (byte) 0) });
        ARecord addr31 =
                new ARecord(addrRecordType, new IAObject[] { new AString("200 14th Avenue"), new AString("Long Island"),
                        new AString("NY"), new AInt16((short) 95011), new AInterval(12000, 14000, (byte) 0) });
        // With nested open field addr31.
        ARecord addr32 =
                new ARecord(addrRecordType, new IAObject[] { new AString("51 8th Street"), new AString("Orlando"),
                        new AString("FL"), new AInt16((short) 49045), new AInterval(190000, 200000, (byte) 0) });

        ARecord record1 = new ARecord(employeeType, new IAObject[] { new AInt64(0L), new AString("Tom"),
                new AOrderedList(addrListType, Arrays.asList(new IAObject[] { addr11, addr12 })) });
        ARecord record2 = new ARecord(employeeType, new IAObject[] { new AInt64(1L), new AString("John"),
                new AOrderedList(addrListType, Arrays.asList(new IAObject[] { addr21, addr22 })) });
        ARecord record3 = new ARecord(employeeType, new IAObject[] { new AInt64(2L), new AString("Lindsay"),
                new AOrderedList(addrListType, Arrays.asList(new IAObject[] { addr31, addr32 })) });
        // With nested open field addr41.
        ARecord record4 = new ARecord(employeeType, new IAObject[] { new AInt64(3L), new AString("Joshua"),
                new AOrderedList(addrListType, Arrays.asList(new IAObject[] {})) });
        ARecord[] records = new ARecord[] { record1, record2, record3, record4 };
        return records;
    }

    @SuppressWarnings("rawtypes")
    public static void concurrentSerDeTestRun(ISerializerDeserializer serde, IAObject[] records) {
        Thread[] threads = new Thread[records.length];
        AtomicInteger errorCount = new AtomicInteger(0);
        for (int i = 0; i < threads.length; ++i) {
            final int index = i;
            threads[i] = new Thread(new Runnable() {

                @SuppressWarnings("unchecked")
                @Override
                public void run() {
                    try {
                        int round = 0;
                        while (round++ < 100000) {
                            // serialize
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            DataOutput dos = new DataOutputStream(bos);
                            serde.serialize(records[index], dos);
                            bos.close();

                            // deserialize
                            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
                            DataInput dis = new DataInputStream(bis);
                            IAObject object = (IAObject) serde.deserialize(dis);
                            bis.close();

                            // asserts the equivalence of objects before and after serde.
                            Assert.assertTrue(object.deepEqual(records[index]));
                            Assert.assertTrue(records[index].deepEqual(object));
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
