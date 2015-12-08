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

import java.util.Arrays;

import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.junit.Test;

public class AUnorderedListSerializerDeserializerTest {

    @Test
    public void test() {
        // Generates types.
        ARecordType addrRecordType = SerializerDeserializerTestUtils.generateAddressRecordType();
        ARecordType employeeType = SerializerDeserializerTestUtils.generateEmployeeRecordType(addrRecordType);
        AUnorderedListType employeeListType = new AUnorderedListType(employeeType, "employee_list");

        //Generates records.
        ARecord[] records = SerializerDeserializerTestUtils.generateRecords(addrRecordType, employeeType);

        // Generates lists
        AUnorderedList[] lists = new AUnorderedList[4];
        for (int index = 0; index < lists.length; ++index) {
            lists[index] = new AUnorderedList(employeeListType, Arrays.asList(records));
        }

        AUnorderedListSerializerDeserializer serde = new AUnorderedListSerializerDeserializer(employeeListType);
        // Run four test threads to serialize/deserialize lists concurrently.
        SerializerDeserializerTestUtils.concurrentSerDeTestRun(serde, lists);
    }

}
