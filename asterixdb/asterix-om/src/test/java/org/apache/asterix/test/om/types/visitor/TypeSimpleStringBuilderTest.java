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
package org.apache.asterix.test.om.types.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.visitor.SimpleStringBuilderForIATypeVisitor;
import org.junit.Assert;
import org.junit.Test;

public class TypeSimpleStringBuilderTest {
    private static final ARecordType ROOT_TYPE;
    private static final String EXPECTED_STRING;

    static {
        StringBuilder expectedStringBuilder = new StringBuilder();
        //Record with two fields
        ARecordType recordType = createNestedRecord(expectedStringBuilder);
        String recordTypeString = getStringAndReset(expectedStringBuilder);

        //Array of records
        AOrderedListType arrayOfRecords = new AOrderedListType(recordType, "arrayOfRecords");
        surroundString(expectedStringBuilder, "[", "]", recordTypeString);
        String arrayOfRecordsString = getStringAndReset(expectedStringBuilder);

        //Multiset of records
        AUnorderedListType multiSetOfRecords = new AUnorderedListType(recordType, "multiSetOfRecords");
        surroundString(expectedStringBuilder, "{{", "}}", recordTypeString);
        String multiSetOfRecordsString = getStringAndReset(expectedStringBuilder);

        //Union
        List<IAType> unionList = new ArrayList<>();
        unionList.add(recordType);
        unionList.add(arrayOfRecords);
        unionList.add(multiSetOfRecords);
        unionList.add(BuiltinType.AINT64);
        AUnionType unionType = new AUnionType(unionList, "unionType");
        surroundString(expectedStringBuilder, "<", ">", recordTypeString, arrayOfRecordsString, multiSetOfRecordsString,
                BuiltinType.AINT64.getTypeTag().toString());
        String unionTypeString = getStringAndReset(expectedStringBuilder);

        //Root type
        String[] rootFieldNames = { BuiltinType.ANY.getTypeName(), arrayOfRecords.getTypeName(),
                multiSetOfRecords.getTypeName(), unionType.getTypeName() };
        IAType[] rootFieldTypes = { BuiltinType.ANY, arrayOfRecords, multiSetOfRecords, unionType };
        ROOT_TYPE = new ARecordType("rootType", rootFieldNames, rootFieldTypes, false);
        buildRecordString(expectedStringBuilder, rootFieldNames, rootFieldTypes[0].getTypeTag().toString(),
                arrayOfRecordsString, multiSetOfRecordsString, unionTypeString);
        EXPECTED_STRING = getStringAndReset(expectedStringBuilder);
    }

    private static ARecordType createNestedRecord(StringBuilder builder) {
        String[] fieldNames = { "field1", "field2" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.AINT64 };
        buildRecordString(builder, fieldNames, fieldTypes[0].getTypeTag().toString(),
                fieldTypes[1].getTypeTag().toString());
        return new ARecordType("nestedRecord", fieldNames, fieldTypes, true);
    }

    private static void surroundString(StringBuilder builder, String open, String close, String... strings) {
        builder.append(open);
        for (int i = 0; i < strings.length; i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(strings[i]);
        }
        builder.append(close);
    }

    private static void buildRecordString(StringBuilder builder, String[] fieldNames, String... fieldTypesStrings) {
        builder.append('{');
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(fieldNames[i]);
            builder.append(':');
            builder.append(fieldTypesStrings[i]);
        }
        builder.append('}');
    }

    private static String getStringAndReset(StringBuilder expectedStringBuilder) {
        String value = expectedStringBuilder.toString();
        expectedStringBuilder.setLength(0);
        return value;
    }

    @Test
    public void testSimpleStringBuilderForIAType() {
        StringBuilder builder = new StringBuilder();
        SimpleStringBuilderForIATypeVisitor visitor = new SimpleStringBuilderForIATypeVisitor();
        ROOT_TYPE.accept(visitor, builder);
        Assert.assertEquals(EXPECTED_STRING, builder.toString());
    }

}
