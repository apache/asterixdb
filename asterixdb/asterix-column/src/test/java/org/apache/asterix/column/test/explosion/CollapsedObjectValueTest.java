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
package org.apache.asterix.column.test.explosion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.apache.asterix.column.bytes.stream.in.MultiByteBufferInputStream;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.test.bytes.components.TestColumnBufferProvider;
import org.apache.asterix.column.util.SchemaJSONBuilderVisitor;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IValueReference;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An object value arriving under a collapsed node, after that node's map value column has already been typed as a
 * scalar, turns the map value into a union (scalar | object). The object arm of that union must round-trip.
 */
public class CollapsedObjectValueTest {

    private static String doc(int d) {
        return "{\"id\": " + d + ", \"name\": \"n" + d + "\", \"k" + d + "\": " + d + "}";
    }

    private static String docWithObject(int d) {
        return "{\"id\": " + d + ", \"name\": \"n" + d + "\", \"k" + d + "\": " + d + ", \"extra\": {\"deep\": \"x" + d
                + "\"}}";
    }

    private static List<String> docs(IntFunction<String> shape, int from, int count) {
        List<String> out = new ArrayList<>();
        for (int d = from; d < from + count; d++) {
            out.add(shape.apply(d));
        }
        return out;
    }

    private static int countDeep(List<IValueReference> records, boolean print) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        int deep = 0;
        for (IValueReference record : records) {
            String json = ColumnFlushHarness.json(record);
            JsonNode node = mapper.readTree(json);
            int id = node.get("id").asInt();
            if (print && id >= 200 && id < 203) {
                System.out.println("ASSEMBLED " + json);
            }
            if (node.path("extra").path("deep").isTextual()) {
                assertEquals("x" + id, node.path("extra").path("deep").asText());
                deep++;
            }
        }
        return deep;
    }

    private static void dump(String label, FlushColumnMetadata md) throws Exception {
        System.out.println(
                label + " SCHEMA " + new SchemaJSONBuilderVisitor(md.getFieldNamesDictionary()).build(md.getRoot()));
        System.out.println(label + " COLUMNS " + ColumnFlushHarness.columnsByIndex(md));
    }

    /**
     * Plain ingest, one flush: the root collapses on the unique k&lt;d&gt; keys (bigint map values), then documents
     * carrying an object-valued new key arrive. No merge.
     */
    @Test
    public void aSingleFlushKeepsAnObjectValueArrivingAfterCollapse() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        List<String> input = new ArrayList<>(docs(CollapsedObjectValueTest::doc, 0, 120));
        input.addAll(docs(CollapsedObjectValueTest::docWithObject, 200, 10));
        ColumnFlushHarness.Component[] out = new ColumnFlushHarness.Component[1];
        FlushColumnMetadata[] md = new FlushColumnMetadata[1];
        ColumnFlushHarness.withDetector(16, 1, () -> {
            int file = harness.createFile();
            md[0] = harness.freshMetadata(file);
            out[0] = harness.writeComponentWith(md[0], file, input);
        });
        assertTrue("the root must collapse", md[0].getRoot().isMapified());
        dump("FLUSH1", md[0]);
        List<IValueReference> records = harness.assembleRecords(out[0]);
        assertEquals(130, records.size());
        ColumnFlushHarness.assertKeysIntact(records);
        assertEquals("extra.deep must survive a flush through a collapsed root", 10, countDeep(records, true));
    }

    /** Same, but the object-valued documents arrive in a later flush that continues the collapsed schema. */
    @Test
    public void aLaterFlushKeepsAnObjectValueArrivingAfterCollapse() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        ColumnFlushHarness.Component[] out = new ColumnFlushHarness.Component[2];
        FlushColumnMetadata[] md = new FlushColumnMetadata[2];
        ColumnFlushHarness.withDetector(16, 1, () -> {
            int file = harness.createFile();
            md[0] = harness.freshMetadata(file);
            out[0] = harness.writeComponentWith(md[0], file, docs(CollapsedObjectValueTest::doc, 0, 120));
            int file2 = harness.createFile();
            md[1] = harness.continueMetadata(file2, out[0].serializedMetadata());
            out[1] = harness.writeComponentWith(md[1], file2, docs(CollapsedObjectValueTest::docWithObject, 200, 10));
        });
        assertTrue("the root must collapse", md[0].getRoot().isMapified());
        dump("FLUSH2", md[1]);
        List<IValueReference> records = harness.assembleRecords(out[1]);
        assertEquals(10, records.size());
        ColumnFlushHarness.assertKeysIntact(records);
        assertEquals("extra.deep must survive a flush through a collapsed root", 10, countDeep(records, true));
    }

    /**
     * Control: "extra" is seen before the root collapses, so it becomes an ordinary column and the map value stays a
     * plain bigint -- no union, no loss.
     */
    @Test
    public void aKeyDeclaredBeforeCollapseControl() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        List<String> input = new ArrayList<>(docs(CollapsedObjectValueTest::docWithObject, 200, 10));
        input.addAll(docs(CollapsedObjectValueTest::doc, 0, 120));
        input.addAll(docs(CollapsedObjectValueTest::docWithObject, 300, 10));
        ColumnFlushHarness.Component[] out = new ColumnFlushHarness.Component[1];
        FlushColumnMetadata[] md = new FlushColumnMetadata[1];
        ColumnFlushHarness.withDetector(16, 1, () -> {
            int file = harness.createFile();
            md[0] = harness.freshMetadata(file);
            out[0] = harness.writeComponentWith(md[0], file, input);
        });
        assertTrue("the root must collapse", md[0].getRoot().isMapified());
        dump("CONTROL", md[0]);
        List<IValueReference> records = harness.assembleRecords(out[0]);
        assertEquals(20, countDeep(records, false));
    }

    /**
     * Reads the map's key and value-arm columns of a component directly, bypassing the assembler: each line is
     * {@code (definition level, value)} per entry, in write order.
     */
    private static String rawColumn(ColumnFlushHarness harness, ColumnFlushHarness.Component component, ATypeTag type,
            int column, int maxLevel) throws Exception {
        IColumnValuesReader reader =
                new ColumnValueReaderFactory().createValueReader(type, column, maxLevel, new int[] { 0 });
        TestColumnBufferProvider provider =
                new TestColumnBufferProvider(component.fileId, column, harness.getBufferCache());
        MultiByteBufferInputStream stream = new MultiByteBufferInputStream();
        StringBuilder sb = new StringBuilder();
        for (org.apache.asterix.column.common.buffer.DummyPage page : component.pageZeros) {
            provider.reset(page);
            stream.reset(provider);
            int tuples = page.getBuffer().getInt(
                    org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.TUPLE_COUNT_OFFSET);
            reader.reset(stream, tuples);
            while (reader.next()) {
                sb.append('(').append(reader.getLevel());
                if (reader.isValue()) {
                    sb.append(',').append(type == ATypeTag.BIGINT ? String.valueOf(reader.getLong())
                            : ColumnFlushHarness.render(reader.getBytes()).replaceAll("[^ -~]", ""));
                } else if (reader.isDelimiter()) {
                    sb.append(",<end>");
                } else if (reader.isMissing()) {
                    sb.append(",MISSING");
                }
                sb.append(") ");
            }
        }
        return sb.toString();
    }

    /**
     * Write-side evidence for {@link #aLaterFlushKeepsAnObjectValueArrivingAfterCollapse}: the second flush's columns
     * hold the "extra" key, a MISSING in the bigint arm and "x&lt;d&gt;" in the object arm's deep column.
     */
    @Test
    public void theObjectArmIsWrittenToItsColumns() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        ColumnFlushHarness.Component[] out = new ColumnFlushHarness.Component[2];
        FlushColumnMetadata[] md = new FlushColumnMetadata[2];
        ColumnFlushHarness.withDetector(16, 1, () -> {
            int file = harness.createFile();
            md[0] = harness.freshMetadata(file);
            out[0] = harness.writeComponentWith(md[0], file, docs(CollapsedObjectValueTest::doc, 0, 120));
            int file2 = harness.createFile();
            md[1] = harness.continueMetadata(file2, out[0].serializedMetadata());
            out[1] = harness.writeComponentWith(md[1], file2, docs(CollapsedObjectValueTest::docWithObject, 200, 10));
        });
        String keys = rawColumn(harness, out[1], ATypeTag.STRING, 17, 2);
        String bigints = rawColumn(harness, out[1], ATypeTag.BIGINT, 18, 2);
        String deeps = rawColumn(harness, out[1], ATypeTag.STRING, 19, 3);
        System.out.println("RAW key   col17: " + keys);
        System.out.println("RAW bigint col18: " + bigints);
        System.out.println("RAW deep  col19: " + deeps);
        for (int d = 200; d < 210; d++) {
            assertTrue("key column must hold extra for doc " + d, keys.contains("k" + d));
            assertTrue("deep column must hold x" + d, deeps.contains("x" + d));
        }
        assertEquals("one 'extra' key per document", 10, keys.split("extra", -1).length - 1);
    }

    /**
     * The loss is not about objects: any map entry whose value lives in a union arm other than the first is dropped.
     * Here the map value is typed string first (collapse on string values), then bigint values arrive.
     */
    @Test
    public void aSecondScalarArmIsLostToo() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        List<String> input = new ArrayList<>();
        for (int d = 0; d < 120; d++) {
            input.add("{\"id\": " + d + ", \"k" + d + "\": \"s" + d + "\"}");
        }
        for (int d = 200; d < 210; d++) {
            input.add("{\"id\": " + d + ", \"k" + d + "\": " + d + "}");
        }
        ColumnFlushHarness.Component[] out = new ColumnFlushHarness.Component[1];
        FlushColumnMetadata[] md = new FlushColumnMetadata[1];
        ColumnFlushHarness.withDetector(16, 1, () -> {
            int file = harness.createFile();
            md[0] = harness.freshMetadata(file);
            out[0] = harness.writeComponentWith(md[0], file, input);
        });
        assertTrue("the root must collapse", md[0].getRoot().isMapified());
        dump("SCALARS", md[0]);
        ObjectMapper mapper = new ObjectMapper();
        int stringKept = 0;
        int bigintKept = 0;
        for (IValueReference record : harness.assembleRecords(out[0])) {
            JsonNode node = mapper.readTree(ColumnFlushHarness.json(record));
            int id = node.get("id").asInt();
            JsonNode v = node.path("k" + id);
            if (id >= 200 && id < 203) {
                System.out.println("SCALARS ASSEMBLED " + node);
            }
            if (id >= 200 && v.isInt() && v.asInt() == id) {
                bigintKept++;
            } else if (id >= 15 && id < 120 && v.isTextual()) {
                stringKept++;
            }
        }
        System.out.println("SCALARS collapsed string entries kept=" + stringKept + "/105, bigint entries kept="
                + bigintKept + "/10");
        assertEquals("every bigint-armed map entry must survive", 10, bigintKept);
        assertEquals("every string-armed map entry must survive", 105, stringKept);
    }

    /**
     * Writes {@code collapsing} (which must collapse a node on string values) followed by {@code mixed} in one flush,
     * and requires every document to read back equal to what was written.
     */
    private static void assertRoundTrip(String label, List<String> collapsing, List<String> mixed) throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        List<String> input = new ArrayList<>(collapsing);
        input.addAll(mixed);
        ColumnFlushHarness.Component[] out = new ColumnFlushHarness.Component[1];
        FlushColumnMetadata[] md = new FlushColumnMetadata[1];
        ColumnFlushHarness.withDetector(16, 1, () -> {
            int file = harness.createFile();
            md[0] = harness.freshMetadata(file);
            out[0] = harness.writeComponentWith(md[0], file, input);
        });
        String schema = new SchemaJSONBuilderVisitor(md[0].getFieldNamesDictionary()).build(md[0].getRoot());
        System.out.println(label + " SCHEMA " + schema);
        assertTrue(label + ": a node must collapse", schema.contains("<map>"));
        assertTrue(label + ": the map value must become a union", schema.contains("\"union\""));
        ObjectMapper mapper = new ObjectMapper();
        Map<Integer, JsonNode> expected = new HashMap<>();
        for (String doc : input) {
            JsonNode node = mapper.readTree(doc);
            expected.put(node.get("id").asInt(), node);
        }
        List<IValueReference> records = harness.assembleRecords(out[0]);
        assertEquals(label + ": record count", input.size(), records.size());
        for (IValueReference record : records) {
            JsonNode actual = mapper.readTree(ColumnFlushHarness.json(record));
            int id = actual.get("id").asInt();
            assertEquals(label + ": document " + id, expected.get(id), actual);
        }
    }

    private static List<String> stringValued(int count) {
        List<String> out = new ArrayList<>();
        for (int d = 0; d < count; d++) {
            out.add("{\"id\": " + d + ", \"m\": {\"k" + d + "\": \"s" + d + "\", \"j" + d + "\": \"t" + d + "\"}}");
        }
        return out;
    }

    /**
     * Several entries per document, each in a different arm and in an order unrelated to the arm order, so that
     * every arm both holds a value and sits between a key and the arm that holds it. NULL values must survive too.
     */
    @Test
    public void consecutiveEntriesInDifferentArmsAllSurvive() throws Exception {
        List<String> mixed = new ArrayList<>();
        for (int d = 200; d < 220; d++) {
            mixed.add("{\"id\": " + d + ", \"m\": {\"o" + d + "\": {\"deep\": \"x" + d + "\"}, \"s" + d + "\": \"s" + d
                    + "\", \"b" + d + "\": " + d + ", \"n" + d + "\": null, \"t" + d + "\": \"t" + d + "\", \"c" + d
                    + "\": " + (d + 1) + ", \"p" + d + "\": {\"deep\": \"y" + d + "\"}}}");
        }
        assertRoundTrip("CONSECUTIVE", stringValued(120), mixed);
    }

    /** An entry whose value is NULL, in documents where it is the only entry or the last one after other arms. */
    @Test
    public void nullValuesUnderAUnionSurvive() throws Exception {
        List<String> mixed = new ArrayList<>();
        for (int d = 200; d < 210; d++) {
            mixed.add("{\"id\": " + d + ", \"m\": {\"n" + d + "\": null}}");
            mixed.add("{\"id\": " + (d + 100) + ", \"m\": {\"b" + d + "\": " + d + ", \"o" + d + "\": {\"deep\": \"x"
                    + d + "\"}, \"n" + d + "\": null}}");
        }
        assertRoundTrip("NULLS", stringValued(120), mixed);
    }

    /** Array values under the collapsed node add an array arm to the union. */
    @Test
    public void arrayArmSurvives() throws Exception {
        List<String> mixed = new ArrayList<>();
        for (int d = 200; d < 210; d++) {
            mixed.add("{\"id\": " + d + ", \"m\": {\"a" + d + "\": [" + d + ", " + (d + 1) + "], \"s" + d + "\": \"s"
                    + d + "\", \"o" + d + "\": {\"deep\": [\"x" + d + "\"]}}}");
        }
        assertRoundTrip("ARRAY-ARM", stringValued(120), mixed);
    }

    /** The object arm itself holds a collapsed node whose value later becomes a union: a map in a map's union. */
    @Test
    public void nestedUnionValuedMapsSurvive() throws Exception {
        List<String> collapsing = new ArrayList<>();
        for (int d = 0; d < 120; d++) {
            collapsing.add("{\"id\": " + d + ", \"m\": {\"k" + d + "\": \"s" + d + "\", \"o" + d + "\": {\"i" + d
                    + "\": \"u" + d + "\", \"h" + d + "\": \"v" + d + "\"}}}");
        }
        List<String> mixed = new ArrayList<>();
        for (int d = 200; d < 210; d++) {
            mixed.add("{\"id\": " + d + ", \"m\": {\"o" + d + "\": {\"i" + d + "\": " + d + ", \"h" + d + "\": \"v" + d
                    + "\", \"g" + d + "\": {\"deep\": \"x" + d + "\"}}, \"k" + d + "\": " + d + "}}");
        }
        assertRoundTrip("NESTED", collapsing, mixed);
    }

    /** A collapsed node inside array items, whose map value becomes a union. */
    @Test
    public void aUnionValuedMapInsideAnArraySurvives() throws Exception {
        List<String> collapsing = new ArrayList<>();
        for (int d = 0; d < 120; d++) {
            collapsing.add("{\"id\": " + d + ", \"arr\": [{\"k" + d + "\": \"s" + d + "\"}, {\"j" + d + "\": \"t" + d
                    + "\", \"q" + d + "\": \"r" + d + "\"}]}");
        }
        List<String> mixed = new ArrayList<>();
        for (int d = 200; d < 210; d++) {
            mixed.add("{\"id\": " + d + ", \"arr\": [{\"b" + d + "\": " + d + ", \"s" + d + "\": \"s" + d + "\"}, {\"o"
                    + d + "\": {\"deep\": \"x" + d + "\"}}, {}, {\"n" + d + "\": null, \"t" + d + "\": \"t" + d
                    + "\"}]}");
        }
        assertRoundTrip("IN-ARRAY", collapsing, mixed);
    }
}
