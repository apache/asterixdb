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

package org.apache.asterix.test.om.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.JSONDeserializerForTypes;
import org.junit.Assert;
import org.junit.Test;

public class JSONDeserializerForTypesTest {

    @Test
    public void test() throws Exception {
        // Tests a record type with primitive types.
        String[] fieldNames = { "a1", "a2", "a3" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.AINT16, BuiltinType.ABITARRAY };
        ARecordType recordType = new ARecordType("ARecord", fieldNames, fieldTypes, true);
        Assert.assertEquals(recordType, JSONDeserializerForTypes.convertFromJSON(recordType.toJSON()));

        // Tests a record type with a nested record type.
        String[] fieldNames2 = { "a1", "a2" };
        IAType[] fieldTypes2 = { BuiltinType.ABOOLEAN, recordType };
        ARecordType recordType2 = new ARecordType("ARecord2", fieldNames2, fieldTypes2, true);
        Assert.assertEquals(recordType2, JSONDeserializerForTypes.convertFromJSON(recordType2.toJSON()));

        // Tests a record type with a union type.
        String[] fieldNames3 = { "a1", "a2" };
        List<IAType> unionTypes = new ArrayList<IAType>();
        unionTypes.add(BuiltinType.ADURATION);
        unionTypes.add(recordType2);
        AUnionType unionType = new AUnionType(unionTypes, "union");
        IAType[] fieldTypes3 = { BuiltinType.ABOOLEAN, unionType };
        ARecordType recordType3 = new ARecordType("ARecord3", fieldNames3, fieldTypes3, true);
        Assert.assertEquals(recordType3, JSONDeserializerForTypes.convertFromJSON(recordType3.toJSON()));

        // Tests a record type with an ordered list.
        String[] fieldNames4 = { "a1", "a2" };
        IAType[] fieldTypes4 = { BuiltinType.ABOOLEAN, new AOrderedListType(BuiltinType.ADATETIME, "list") };
        ARecordType recordType4 = new ARecordType("ARecord4", fieldNames4, fieldTypes4, false);
        Assert.assertEquals(recordType4, JSONDeserializerForTypes.convertFromJSON(recordType4.toJSON()));

        // Tests a record type with an unordered list.
        String[] fieldNames5 = { "a1", "a2" };
        IAType[] fieldTypes5 = { BuiltinType.ABOOLEAN, new AUnorderedListType(recordType2, "list") };
        ARecordType recordType5 = new ARecordType("ARecord5", fieldNames5, fieldTypes5, false);
        Assert.assertEquals(recordType5, JSONDeserializerForTypes.convertFromJSON(recordType5.toJSON()));
    }
}
