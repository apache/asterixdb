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
package org.apache.asterix.spidersilk.schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the PR-2 schema extraction components.
 *
 * These tests exercise {@link DatasetSchemaFormatter} and {@link DatasetSchema}
 * using ADM type objects constructed directly in-memory, with no dependency on
 * a running AsterixDB instance or MetadataManager.
 *
 * Integration tests that verify the full {@link SchemaContextBuilder#build(String)}
 * path against a live AsterixDB + TinySocial dataset are left for the integration
 * test suite (require a running cluster).
 */
public class SchemaContextBuilderTest {

    private final DatasetSchemaFormatter formatter = new DatasetSchemaFormatter();

    // -------------------------------------------------------------------------
    // DatasetSchemaFormatter tests
    // -------------------------------------------------------------------------

    @Test
    public void testFormatPrimitiveTypes() {
        Assert.assertEquals("int64", formatter.formatType(BuiltinType.AINT64));
        Assert.assertEquals("string", formatter.formatType(BuiltinType.ASTRING));
        Assert.assertEquals("boolean", formatter.formatType(BuiltinType.ABOOLEAN));
        Assert.assertEquals("double", formatter.formatType(BuiltinType.ADOUBLE));
    }

    @Test
    public void testFormatNullType() {
        Assert.assertEquals("any", formatter.formatType(null));
    }

    @Test
    public void testFormatOrderedList() {
        // [string] — ordered list of strings (SQL++ array)
        AOrderedListType listType = new AOrderedListType(BuiltinType.ASTRING, "string-list");
        String result = formatter.formatType(listType);
        Assert.assertEquals("[string]", result);
    }

    @Test
    public void testFormatNullableField() {
        // string? — union of string + missing (nullable field)
        AUnionType unionType =
                new AUnionType(Arrays.asList(BuiltinType.ASTRING, BuiltinType.AMISSING), "nullable-string");
        String result = formatter.formatType(unionType);
        Assert.assertEquals("string?", result);
    }

    @Test
    public void testFormatNestedRecord() {
        // Nested record: { street: string, city: string }
        ARecordType addressType = new ARecordType("AddressType", new String[] { "street", "city" },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING }, false);

        // Top-level record with a nested field
        ARecordType personType = new ARecordType("PersonType", new String[] { "name", "address" },
                new IAType[] { BuiltinType.ASTRING, addressType }, false);

        // formatType on the top-level record (depth=0) should not wrap in braces
        String result = formatter.formatType(personType);
        Assert.assertTrue("Should contain nested field 'address'", result.contains("address"));
        Assert.assertTrue("Should contain nested field 'street'", result.contains("street"));
        Assert.assertTrue("Should contain nested field 'city'", result.contains("city"));
    }

    @Test
    public void testFormatTweetMessagesSchema() {
        // Mimics the TinySocial TweetMessages item type
        AOrderedListType topicsType = new AOrderedListType(BuiltinType.ASTRING, "topics-list");
        ARecordType tweetType = new ARecordType("TweetMessageType",
                new String[] { "tweetid", "sender-location", "send-time", "referred-topics", "message-text",
                        "author-id" },
                new IAType[] { BuiltinType.AINT64, BuiltinType.ANY, BuiltinType.ADATETIME, topicsType,
                        BuiltinType.ASTRING, BuiltinType.AINT64 },
                false);

        String result = formatter.formatType(tweetType);
        Assert.assertTrue(result.contains("tweetid"));
        Assert.assertTrue(result.contains("int64"));
        Assert.assertTrue(result.contains("message-text"));
        Assert.assertTrue(result.contains("referred-topics"));
        Assert.assertTrue(result.contains("[string]"));
    }

    // -------------------------------------------------------------------------
    // ColumnInfo tests
    // -------------------------------------------------------------------------

    @Test
    public void testColumnInfoPrimaryKeyDescription() {
        ColumnInfo pk = new ColumnInfo("tweetid", "bigint", true);
        Assert.assertEquals("tweetid: bigint [PK]", pk.toDescriptionString());
    }

    @Test
    public void testColumnInfoNonPrimaryKeyDescription() {
        ColumnInfo col = new ColumnInfo("message-text", "string", false);
        Assert.assertEquals("message-text: string", col.toDescriptionString());
    }

    // -------------------------------------------------------------------------
    // DatasetSchema tests
    // -------------------------------------------------------------------------

    @Test
    public void testDatasetSchemaDescriptionString() {
        List<ColumnInfo> columns = Arrays.asList(new ColumnInfo("tweetid", "int64", true),
                new ColumnInfo("message-text", "string", false), new ColumnInfo("author-id", "int64", false));

        DatasetSchema schema = new DatasetSchema("TweetMessages", columns);
        String desc = schema.toDescriptionString();

        Assert.assertTrue(desc.startsWith("Dataset TweetMessages ("));
        Assert.assertTrue(desc.contains("tweetid: int64 [PK]"));
        Assert.assertTrue(desc.contains("message-text: string"));
        Assert.assertTrue(desc.endsWith(")"));
    }

    @Test
    public void testDatasetSchemaFallsBackToAllColumnsBeforePruning() {
        List<ColumnInfo> columns =
                Arrays.asList(new ColumnInfo("id", "bigint", true), new ColumnInfo("name", "string", false));

        DatasetSchema schema = new DatasetSchema("Users", columns);

        // Before pruning, getEffectiveColumns() returns the full list
        Assert.assertEquals(2, schema.getEffectiveColumns().size());
    }

    @Test
    public void testDatasetSchemaUsesPrunedColumnsAfterPruning() {
        List<ColumnInfo> allColumns = Arrays.asList(new ColumnInfo("id", "bigint", true),
                new ColumnInfo("name", "string", false), new ColumnInfo("created-at", "datetime", false));

        DatasetSchema schema = new DatasetSchema("Users", allColumns);

        // Simulate ColumnPruner keeping only id and name
        List<ColumnInfo> pruned =
                Arrays.asList(new ColumnInfo("id", "bigint", true), new ColumnInfo("name", "string", false));
        schema.setPrunedColumns(pruned);

        Assert.assertEquals(2, schema.getEffectiveColumns().size());
        String desc = schema.toDescriptionString();
        Assert.assertFalse("Pruned field should not appear", desc.contains("created-at"));
    }

    @Test
    public void testDatasetSchemaImmutableAllColumns() {
        List<ColumnInfo> mutable = new java.util.ArrayList<>();
        mutable.add(new ColumnInfo("id", "bigint", true));
        DatasetSchema schema = new DatasetSchema("Foo", mutable);

        // Modifying original list must not affect the schema
        mutable.add(new ColumnInfo("extra", "string", false));
        Assert.assertEquals(1, schema.getAllColumns().size());
    }

    @Test
    public void testEmptyDatasetDescription() {
        DatasetSchema schema = new DatasetSchema("EmptyDataset", Collections.emptyList());
        String desc = schema.toDescriptionString();
        Assert.assertEquals("Dataset EmptyDataset ()", desc);
    }
}
