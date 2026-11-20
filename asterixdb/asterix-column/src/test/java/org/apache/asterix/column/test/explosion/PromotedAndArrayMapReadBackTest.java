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
import java.util.Collections;
import java.util.List;
import java.util.function.IntFunction;

import org.apache.asterix.column.assembler.value.ValueGetterFactory;
import org.apache.asterix.column.bytes.stream.in.MultiByteBufferInputStream;
import org.apache.asterix.column.common.buffer.DummyPage;
import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.metadata.PromotionBoundaries;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.MapSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.query.ColumnAssembler;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.test.bytes.components.TestColumnBufferProvider;
import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.api.exceptions.NoOpWarningCollector;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Read-back regressions for collapsed (map) nodes.
 * <p>
 * Promoted keys: a key promoted out of a collapsed node is appended to the node's children after the map. The
 * whole-record (ALL_FIELDS) assembler visited children in insertion order, so the map -- the child that ends the
 * record -- ended it before the promoted column was read, and that column's start() then wiped the record: every
 * whole-record read of a node with a promoted key returned {@code {}} or only the promoted field.
 * <p>
 * Maps in array items: the map's key reader and each value-arm reader reported an ancestor-level null or missing
 * separately, while the enclosing item object counts the map as one slot, so each surplus report became a phantom
 * trailing null item. Most visible on projected reads, which clip the item down to one or two slots.
 * <p>
 * Every comparison is against the input as the ADM parser reads it, so values (and null versus missing) must round
 * trip exactly.
 */
public class PromotedAndArrayMapReadBackTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ------------------------------------------------------------------------------------------------------------
    // Fixture plumbing
    // ------------------------------------------------------------------------------------------------------------

    interface Body {
        void run() throws Exception;
    }

    /** Collapses after a few dozen small documents; promotion at its defaults. */
    static void withSettings(Body body) throws Exception {
        CollapseSettings.install(CollapseSettings.lowered(16, 1));
        try {
            body.run();
        } finally {
            CollapseSettings.reset();
        }
    }

    interface SchemaSetup {
        void apply(FlushColumnMetadata metadata) throws Exception;
    }

    /** One component written from {@code input}, plus the schema it was written with. */
    static final class Written {
        final ColumnFlushHarness harness = new ColumnFlushHarness();
        ColumnFlushHarness.Component component;
        FlushColumnMetadata metadata;
        final List<String> input;

        Written(List<String> input) {
            this.input = input;
        }

        ObjectSchemaNode root() {
            return metadata.getRoot();
        }

        int fieldIndex(String name) throws Exception {
            return metadata.getFieldNamesDictionary().getFieldNameIndex(PromotionTest.name(name));
        }

        ObjectSchemaNode object(ObjectSchemaNode parent, String name) throws Exception {
            return (ObjectSchemaNode) parent.getChild(fieldIndex(name));
        }

        ObjectSchemaNode arrayItem(ObjectSchemaNode parent, String name) throws Exception {
            return (ObjectSchemaNode) ((AbstractCollectionSchemaNode) parent.getChild(fieldIndex(name))).getItemNode();
        }

        boolean promoted(ObjectSchemaNode node, String name) throws Exception {
            int index = fieldIndex(name);
            return index >= 0 && node.isPromoted(index) && node.getChild(index).getTypeTag() != ATypeTag.MISSING;
        }

        List<IValueReference> allFields() throws Exception {
            return harness.assembleRecords(component);
        }
    }

    static Written write(List<String> input, SchemaSetup setup) throws Exception {
        Written w = new Written(input);
        withSettings(() -> {
            int file = w.harness.createFile();
            w.metadata = w.harness.freshMetadata(file);
            if (setup != null) {
                setup.apply(w.metadata);
            }
            w.component = w.harness.writeComponentWith(w.metadata, file, input);
        });
        return w;
    }

    static Written write(List<String> input) throws Exception {
        return write(input, null);
    }

    /** The input as the ADM parser reads it, printed by the same printer as the read-back, as JSON trees. */
    static List<JsonNode> expected(List<String> input) throws Exception {
        List<JsonNode> out = new ArrayList<>();
        for (IValueReference record : ColumnFlushHarness.parse(input)) {
            out.add(MAPPER.readTree(ColumnFlushHarness.json(record)));
        }
        return out;
    }

    static List<JsonNode> actual(List<IValueReference> records) throws Exception {
        List<JsonNode> out = new ArrayList<>();
        for (IValueReference record : records) {
            out.add(MAPPER.readTree(ColumnFlushHarness.json(record)));
        }
        return out;
    }

    /** Asserts record-for-record equality, naming the first few differences so a failure explains itself. */
    static void assertSameRecords(String label, List<JsonNode> expected, List<JsonNode> actual) {
        int bad = 0;
        StringBuilder diffs = new StringBuilder();
        for (int i = 0; i < Math.min(expected.size(), actual.size()); i++) {
            if (!expected.get(i).equals(actual.get(i))) {
                if (bad++ < 3) {
                    diffs.append("\n#").append(i).append("\n   expected ").append(expected.get(i))
                            .append("\n   actual   ").append(actual.get(i));
                }
            }
        }
        assertEquals(label + ": record count", expected.size(), actual.size());
        assertEquals(label + ": records that did not round-trip" + diffs, 0, bad);
    }

    static void assertAllFieldsRoundTrip(String label, Written w) throws Exception {
        assertSameRecords(label + " ALL_FIELDS", expected(w.input), actual(w.allFields()));
    }

    /** Assembles {@code component} through the clipper, the projected-query path. */
    static List<IValueReference> assemble(Written w, ARecordType requested) throws Exception {
        QueryColumnMetadata readMetadata = QueryColumnMetadata.create(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, 0,
                w.component.serializedMetadata(), new ColumnValueReaderFactory(), ValueGetterFactory.INSTANCE,
                requested, Collections.emptyMap(), NoOpColumnFilterEvaluatorFactory.INSTANCE,
                NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpWarningCollector.INSTANCE, null,
                ColumnProjectorType.QUERY);
        ColumnAssembler assembler = readMetadata.getAssembler();
        int columns = readMetadata.getNumberOfColumns();
        TestColumnBufferProvider[] providers = new TestColumnBufferProvider[columns];
        MultiByteBufferInputStream[] streams = new MultiByteBufferInputStream[columns];
        for (int i = 0; i < columns; i++) {
            providers[i] = new TestColumnBufferProvider(w.component.fileId, assembler.getColumnIndex(i),
                    w.harness.getBufferCache());
            streams[i] = new MultiByteBufferInputStream();
        }
        List<IValueReference> out = new ArrayList<>();
        for (DummyPage pageZero : w.component.pageZeros) {
            for (int i = 0; i < columns; i++) {
                providers[i].reset(pageZero);
                streams[i].reset(providers[i]);
            }
            assembler.reset(pageZero.getBuffer().getInt(AbstractColumnBTreeLeafFrame.TUPLE_COUNT_OFFSET));
            for (int i = 0; i < columns; i++) {
                assembler.resetColumn(streams[i], i);
            }
            while (assembler.hasNext()) {
                out.add(ColumnFlushHarness.copyOf(assembler.nextValue()));
            }
        }
        return out;
    }

    /** {@code {arr: [{field}]}} -- a projection of one field of the items of {@code arr}. */
    static ARecordType arrItemField(String field) {
        ARecordType item = new ARecordType("item", new String[] { field }, new IAType[] { BuiltinType.ANY }, true);
        return new ARecordType("ALL", new String[] { "arr" }, new IAType[] { new AOrderedListType(item, "items") },
                true);
    }

    /**
     * What projecting {@code arr[*].field} must return for one input record: the array with every object item cut
     * down to {@code field}, and every non-object item (null, missing) left exactly as it is.
     */
    static JsonNode projectArrItemField(JsonNode record, String field) {
        ObjectNode out = JsonNodeFactory.instance.objectNode();
        JsonNode arr = record.get("arr");
        if (arr == null) {
            return out;
        }
        if (!arr.isArray()) {
            out.set("arr", arr);
            return out;
        }
        ArrayNode items = out.putArray("arr");
        for (JsonNode item : arr) {
            if (item.isObject()) {
                ObjectNode projected = items.addObject();
                if (item.has(field)) {
                    projected.set(field, item.get(field));
                }
            } else {
                items.add(item);
            }
        }
        return out;
    }

    static void assertArrItemProjectionRoundTrips(String label, Written w, String... fields) throws Exception {
        List<JsonNode> expected = expected(w.input);
        for (String field : fields) {
            List<JsonNode> projected = new ArrayList<>();
            for (JsonNode record : expected) {
                projected.add(projectArrItemField(record, field));
            }
            assertSameRecords(label + " arr[*]." + field, projected, actual(assemble(w, arrItemField(field))));
        }
    }

    static List<String> range(IntFunction<String> f, int from, int count) {
        List<String> out = new ArrayList<>();
        for (int d = from; d < from + count; d++) {
            out.add(f.apply(d));
        }
        return out;
    }

    // ------------------------------------------------------------------------------------------------------------
    // Promoted keys and whole-record reads
    // ------------------------------------------------------------------------------------------------------------

    /**
     * One fresh key per document collapses the root; {@code hot} joins at 301, after the collapse, so it can only be
     * promoted.
     */
    static List<String> streamingPromotionInput(boolean objectValued) {
        List<String> lines = new ArrayList<>();
        for (int x = 1; x <= 900; x++) {
            String hot = objectValued ? "{\"deep\": " + x + "}" : String.valueOf(x);
            lines.add("{\"id\": " + x + (x > 300 ? ", \"hot\": " + hot : "") + ", \"k" + x + "\": " + x + "}");
        }
        return lines;
    }

    private void promotedKeyReadsBackWholeRecord(boolean objectValued) throws Exception {
        Written w = write(streamingPromotionInput(objectValued));
        assertTrue("the fixture must collapse the root", w.root().isCollapsed());
        assertTrue("the fixture must promote hot", w.promoted(w.root(), "hot"));
        assertAllFieldsRoundTrip("promoted " + (objectValued ? "object" : "flat"), w);
        // The projected path never had the defect (the clipper adds the map last); it must stay intact.
        List<JsonNode> expected = expected(w.input);
        List<JsonNode> clipped = actual(assemble(w,
                ProjectionFiltrationTypeUtil.getRecordType(List.of(List.of("id"), List.of("hot"), List.of("k5")))));
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("projected id #" + i, expected.get(i).get("id"), clipped.get(i).get("id"));
            assertEquals("projected hot #" + i, expected.get(i).path("hot"), clipped.get(i).path("hot"));
        }
    }

    @Test
    public void flatPromotedKeyReadsBackWholeRecord() throws Exception {
        promotedKeyReadsBackWholeRecord(false);
    }

    @Test
    public void objectPromotedKeyReadsBackWholeRecord() throws Exception {
        promotedKeyReadsBackWholeRecord(true);
    }

    /** Reading the boundaries touches every component a search reads; a query on no promoted key must not. */
    @Test
    public void promotionBoundariesAreReadOnlyForAPromotedKey() throws Exception {
        Written w = write(streamingPromotionInput(false));
        assertTrue("the fixture must promote hot", w.promoted(w.root(), "hot"));
        assertEquals(0, boundaryReads(w, "id"));
        assertEquals(1, boundaryReads(w, "hot"));
    }

    private static int boundaryReads(Written w, String field) throws Exception {
        int[] reads = { 0 };
        PromotionBoundaries.Source source = () -> {
            reads[0]++;
            return null;
        };
        QueryColumnMetadata.create(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, 0, w.component.serializedMetadata(),
                new ColumnValueReaderFactory(), ValueGetterFactory.INSTANCE,
                ProjectionFiltrationTypeUtil.getRecordType(List.of(List.of(field))), Collections.emptyMap(),
                NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                NoOpWarningCollector.INSTANCE, null, ColumnProjectorType.QUERY, source);
        return reads[0];
    }

    private static String docWithObject(int d) {
        return "{\"id\": " + d + ", \"name\": \"n" + d + "\", \"k" + d + "\": " + d + ", \"extra\": {\"deep\": \"x" + d
                + "\"}}";
    }

    private static String doc(int d) {
        return "{\"id\": " + d + ", \"name\": \"n" + d + "\", \"k" + d + "\": " + d + "}";
    }

    /**
     * {@code extra} appears in the first ten documents, then vanishes for 120, then comes back: it is an ordinary
     * column ahead of the map. It must read back whole.
     */
    @Test
    public void keyDeclaredBeforeCollapseReadsBack() throws Exception {
        List<String> input = new ArrayList<>(range(PromotedAndArrayMapReadBackTest::docWithObject, 200, 10));
        input.addAll(range(PromotedAndArrayMapReadBackTest::doc, 0, 120));
        input.addAll(range(PromotedAndArrayMapReadBackTest::docWithObject, 300, 10));
        Written w = write(input);
        assertTrue("the root must collapse", w.root().isMapified());
        assertAllFieldsRoundTrip("key declared before collapse", w);
    }

    /**
     * Several promoted children after the map, of different types, with documents that carry all, some, or none of
     * them, documents that carry nothing but promoted keys, and documents whose collapsed part is empty.
     */
    @Test
    public void multiplePromotedKeysAndSparseDocumentsReadBackWholeRecord() throws Exception {
        List<String> input = new ArrayList<>();
        for (int x = 1; x <= 900; x++) {
            StringBuilder sb = new StringBuilder("{\"id\": " + x + ", \"k" + x + "\": " + x);
            if (x > 300) {
                sb.append(", \"hot1\": ").append(x);
                sb.append(", \"hot2\": \"s").append(x).append('"');
                sb.append(", \"hot3\": {\"deep\": ").append(x).append('}');
            }
            input.add(sb.append('}').toString());
        }
        // Only promoted keys; only some of them; a promoted key that is null; nothing collapsed at all.
        input.add("{\"hot1\": 1001, \"hot2\": \"only\", \"hot3\": {\"deep\": 1001}}");
        input.add("{\"id\": 1002, \"hot2\": \"some\"}");
        input.add("{\"id\": 1003, \"hot1\": null, \"hot3\": {}}");
        input.add("{\"id\": 1004}");
        input.add("{}");
        Written w = write(input);
        int promoted = 0;
        for (String hot : new String[] { "hot1", "hot2", "hot3" }) {
            promoted += w.promoted(w.root(), hot) ? 1 : 0;
        }
        assertTrue("the fixture must promote more than one key, promoted " + promoted, promoted >= 2);
        assertAllFieldsRoundTrip("multi-promoted", w);
    }

    /**
     * A union-valued map (bigint, string, object and null values among the collapsed keys) with a promoted key after
     * it: the map is the delegate whatever its value type.
     */
    @Test
    public void unionValuedMapWithPromotedKeyReadsBackWholeRecord() throws Exception {
        List<String> input = new ArrayList<>();
        for (int x = 1; x <= 900; x++) {
            String value = x % 4 == 0 ? String.valueOf(x)
                    : x % 4 == 1 ? "\"s" + x + "\"" : x % 4 == 2 ? "{\"deep\": " + x + "}" : "null";
            input.add("{\"id\": " + x + (x > 300 ? ", \"hot\": " + x : "") + ", \"k" + x + "\": " + value + "}");
        }
        Written w = write(input);
        assertTrue("the fixture must promote hot", w.promoted(w.root(), "hot"));
        assertEquals("the collapsed values must be a union", ATypeTag.UNION,
                w.root().getMapNode().getItemNode().getTypeTag());
        assertAllFieldsRoundTrip("union-map+promoted", w);
    }

    /** The same defect one level down: a collapsed nested object with a promoted key, plus empty and absent forms. */
    @Test
    public void promotedKeyInANestedCollapsedObjectReadsBackWholeRecord() throws Exception {
        List<String> input = new ArrayList<>();
        for (int x = 1; x <= 900; x++) {
            input.add("{\"id\": " + x + ", \"ev\": {\"channel\": \"web\", \"users\": {\"k" + x + "\": " + x
                    + (x > 300 ? ", \"hot\": " + x : "") + "}}}");
        }
        input.add("{\"id\": 1001, \"ev\": {\"channel\": \"web\", \"users\": {\"hot\": 1001}}}");
        input.add("{\"id\": 1002, \"ev\": {\"channel\": \"web\", \"users\": {}}}");
        input.add("{\"id\": 1003, \"ev\": {\"users\": null}}");
        input.add("{\"id\": 1004, \"ev\": {}}");
        input.add("{\"id\": 1005, \"ev\": null}");
        input.add("{\"id\": 1006}");
        Written w = write(input);
        ObjectSchemaNode users = w.object(w.object(w.root(), "ev"), "users");
        assertTrue("ev.users must collapse", users.isCollapsed());
        assertTrue("the fixture must promote ev.users.hot", w.promoted(users, "hot"));
        assertAllFieldsRoundTrip("nested promoted", w);
    }

    /**
     * Both defects at once: a collapsed array item with a promoted key. The promoted column follows the map inside
     * the item, and the item's map sits in an array.
     */
    @Test
    public void promotedKeyInsideArrayItemsReadsBack() throws Exception {
        List<String> input = new ArrayList<>();
        for (int x = 1; x <= 900; x++) {
            input.add("{\"id\": " + x + ", \"arr\": [{\"k" + x + "\": " + x + (x > 300 ? ", \"hot\": " + x : "")
                    + "}, {\"j" + x + "\": \"t" + x + "\"" + (x > 300 ? ", \"hot\": \"h" + x + "\"" : "") + "}]}");
        }
        input.add("{\"id\": 1001, \"arr\": [{\"hot\": 1001}, {}, null]}");
        input.add("{\"id\": 1002, \"arr\": [null, {\"hot\": 1002}]}");
        input.add("{\"id\": 1003, \"arr\": []}");
        input.add("{\"id\": 1004, \"arr\": null}");
        input.add("{\"id\": 1005}");
        Written w = write(input);
        ObjectSchemaNode item = w.arrayItem(w.root(), "arr");
        assertTrue("arr[*] must collapse", item.isCollapsed());
        assertTrue("the fixture must promote arr[*].hot", w.promoted(item, "hot"));
        assertAllFieldsRoundTrip("array-item promoted", w);
        assertArrItemProjectionRoundTrips("array-item promoted", w, "hot", "k5", "k700", "j800");
    }

    /**
     * Write side: a column field that follows a map entry in the same record (field order is the document's, so this
     * is routine) used to be written inside the map's still-open repeated group, at the map's level. The column's
     * level stream then disagreed with its schema position: reads under a null ancestor over-read the column, and
     * inside array items the reader ran past its delimiters. {@code a} is an ordinary column from before the
     * collapse, {@code hot} a promoted one; both come after the collapsed key.
     */
    @Test
    public void columnFieldAfterAMapEntryReadsBackUnderNullAncestorsAndInArrays() throws Exception {
        List<String> input = new ArrayList<>();
        for (int x = 1; x <= 900; x++) {
            String fields = "\"k" + x + "\": " + x + ", \"a\": \"a" + x + "\"" + (x > 300 ? ", \"hot\": " + x : "");
            input.add("{\"id\": " + x + ", \"ev\": {\"users\": {" + fields + "}}, \"arr\": [{" + fields + "}, {\"j" + x
                    + "\": " + x + ", \"a\": null" + (x > 300 ? ", \"hot\": " + -x : "") + "}]}");
        }
        input.add("{\"id\": 1001, \"ev\": null, \"arr\": [null, {\"k1001\": 1, \"a\": \"x\", \"hot\": 2}, null]}");
        input.add("{\"id\": 1002, \"ev\": {\"users\": null}, \"arr\": null}");
        input.add("{\"id\": 1003, \"ev\": {\"users\": {\"k1003\": 3, \"hot\": null}}, \"arr\": [{}]}");
        input.add("{\"id\": 1004, \"ev\": {}, \"arr\": [{\"k1004\": 4, \"a\": \"y\"}, {\"a\": \"z\"}]}");
        Written w = write(input);
        ObjectSchemaNode users = w.object(w.object(w.root(), "ev"), "users");
        ObjectSchemaNode item = w.arrayItem(w.root(), "arr");
        assertTrue("ev.users and arr[*] must collapse", users.isCollapsed() && item.isCollapsed());
        int a = w.fieldIndex("a");
        assertTrue("a must be an ordinary column of both", users.getChild(a).getTypeTag() != ATypeTag.MISSING
                && item.getChild(a).getTypeTag() != ATypeTag.MISSING && !users.isPromoted(a) && !item.isPromoted(a));
        assertTrue("the fixture must promote hot in both", w.promoted(users, "hot") && w.promoted(item, "hot"));
        assertAllFieldsRoundTrip("column after map entry", w);
        assertArrItemProjectionRoundTrips("column after map entry", w, "a", "hot", "k5");
    }

    /**
     * Projected-read side: the field-name dictionary is shared by the whole schema, so a name another node interned
     * resolves under a collapsed node that has no child for it -- and that node routes it into its map. {@code id}
     * is interned by the root before {@code users} collapses (it used to be clipped to a lone missing child, which
     * crashed the assembler or read as missing); {@code hot} is promoted at the root after {@code users} collapsed
     * (it used to be read as both a column, which does not exist here, and a map key, which crashed).
     */
    @Test
    public void nameInternedByAnotherNodeIsReadFromTheCollapsedMap() throws Exception {
        List<String> input = new ArrayList<>();
        for (int x = 1; x <= 900; x++) {
            // Kept below the promotion density in ev.users, so neither key earns a column there.
            String users = "\"k" + x + "\": " + x + (x > 300 && x % 5 == 0 ? ", \"id\": " + -x : "")
                    + (x > 300 && x % 5 == 1 ? ", \"hot\": \"u" + x + "\"" : "");
            input.add("{\"id\": " + x + (x > 300 ? ", \"hot\": " + x : "") + ", \"r" + x
                    + "\": 0, \"ev\": {\"users\": {" + users + "}}}");
        }
        input.add("{\"id\": 1001, \"ev\": {\"users\": {\"id\": 7}}}");
        input.add("{\"id\": 1002, \"ev\": {\"users\": null}}");
        input.add("{\"id\": 1003, \"ev\": null}");
        Written w = write(input);
        ObjectSchemaNode users = w.object(w.object(w.root(), "ev"), "users");
        assertTrue("root and ev.users must collapse", w.root().isCollapsed() && users.isCollapsed());
        assertTrue("the fixture must promote hot at the root", w.promoted(w.root(), "hot"));
        for (String name : new String[] { "id", "hot" }) {
            assertEquals(name + " must not be a column of ev.users", ATypeTag.MISSING,
                    users.getChild(w.fieldIndex(name)).getTypeTag());
        }
        assertAllFieldsRoundTrip("interned elsewhere", w);
        List<JsonNode> expected = expected(w.input);
        for (String name : new String[] { "id", "hot", "k5", "k700" }) {
            List<IValueReference> records =
                    assemble(w, ProjectionFiltrationTypeUtil.getRecordType(List.of(List.of("ev", "users", name))));
            for (int i = 0; i < expected.size(); i++) {
                JsonNode actual = MAPPER.readTree(ColumnFlushHarness.json(records.get(i)));
                assertEquals("ev.users." + name + " #" + i, expected.get(i).path("ev").path("users").path(name),
                        actual.path("ev").path("users").path(name));
            }
        }
    }

    /**
     * A name another node interned before this node collapsed has a dictionary index below this node's collapse
     * boundary. Routed into the map and later promoted here, it used to read as not promoted, so a projection read
     * only its new column and silently lost every value written into the map before the promotion.
     */
    @Test
    public void keyPromotedUnderANameInternedBeforeTheCollapseReadsBothRepresentations() throws Exception {
        List<String> input = new ArrayList<>();
        for (int d = 1; d <= 900; d++) {
            // Sparse in ev.users before 600 (stays in the map), dense after (promoted).
            boolean early = d >= 300 && d < 600 && d % 5 == 0;
            String x = d > 600 || early ? ", \"x\": " + -d : "";
            input.add("{\"id\": " + d + ", \"x\": " + d + ", \"ev\": {\"users\": {\"k" + d + "\": " + d + x + "}}}");
        }
        Written w = write(input);
        ObjectSchemaNode users = w.object(w.object(w.root(), "ev"), "users");
        int x = w.fieldIndex("x");
        assertTrue("ev.users must collapse", users.isCollapsed());
        assertTrue("x must have been promoted in ev.users", users.getChild(x).getTypeTag() != ATypeTag.MISSING);
        assertAllFieldsRoundTrip("low-index promotion", w);
        List<JsonNode> expected = expected(w.input);
        for (List<String> path : List.of(List.of("ev", "users", "x"), List.of("x"))) {
            List<IValueReference> records = assemble(w, ProjectionFiltrationTypeUtil.getRecordType(List.of(path)));
            int bad = 0;
            for (int i = 0; i < expected.size(); i++) {
                JsonNode e = expected.get(i);
                JsonNode a = MAPPER.readTree(ColumnFlushHarness.json(records.get(i)));
                for (String step : path) {
                    e = e.path(step);
                    a = a.path(step);
                }
                if (!e.equals(a)) {
                    bad++;
                }
            }
            assertEquals("projected " + path + ": records that did not round-trip", 0, bad);
        }
        assertTrue("x must read as promoted in ev.users", users.isPromoted(x));
    }

    // ------------------------------------------------------------------------------------------------------------
    // Maps inside array items
    // ------------------------------------------------------------------------------------------------------------

    static String collapsingArr(int d) {
        return "{\"id\": " + d + ", \"arr\": [{\"k" + d + "\": \"s" + d + "\"}, {\"j" + d + "\": \"t" + d + "\", \"q"
                + d + "\": \"r" + d + "\"}]}";
    }

    /** Turns the collapsed value into a bigint | string | object | null union, and adds an empty item. */
    static String mixedArr(int d) {
        return "{\"id\": " + d + ", \"arr\": [{\"b" + d + "\": " + d + ", \"s" + d + "\": \"s" + d + "\"}, {\"o" + d
                + "\": {\"deep\": \"x" + d + "\"}}, {}, {\"n" + d + "\": null, \"t" + d + "\": \"t" + d + "\"}]}";
    }

    static List<String> collapsingThenMixed() {
        List<String> input = range(PromotedAndArrayMapReadBackTest::collapsingArr, 0, 120);
        input.addAll(range(PromotedAndArrayMapReadBackTest::mixedArr, 200, 10));
        return input;
    }

    /**
     * Null and empty items, empty, null and absent arrays, including trailing null items: folding the map's reports
     * into one must not drop a legitimate trailing null item. (A missing array item cannot be expressed in ADM input;
     * an absent array is the missing case these readers see.)
     */
    static List<String> nullAndMissingItems() {
        List<String> input = new ArrayList<>();
        input.add("{\"id\": 500, \"arr\": [{\"k500\": \"a\"}, null]}");
        input.add("{\"id\": 501, \"arr\": [{\"k501\": \"a\"}, null, null]}");
        input.add("{\"id\": 502, \"arr\": [null]}");
        input.add("{\"id\": 503, \"arr\": [null, {\"k503\": \"a\"}]}");
        input.add("{\"id\": 504, \"arr\": [{}, null]}");
        input.add("{\"id\": 505, \"arr\": [null, {\"k505\": \"a\"}, null, null]}");
        input.add("{\"id\": 506, \"arr\": [null, {}]}");
        input.add("{\"id\": 507, \"arr\": [{}, {}]}");
        input.add("{\"id\": 508, \"arr\": [{\"k508\": \"a\"}, {}]}");
        input.add("{\"id\": 509, \"arr\": []}");
        input.add("{\"id\": 510, \"arr\": null}");
        input.add("{\"id\": 511}");
        input.add("{\"id\": 512, \"arr\": [{\"k512\": null}, null]}");
        return input;
    }

    /**
     * An array item that collapses before its first write, leaving the map its only child (or one of very few,
     * {@code placeholders}). That is the shape with the fewest slots per item, so the one where surplus reports showed
     * most.
     */
    static SchemaSetup preCollapsedArrayItem(ATypeTag valueType, String... placeholders) {
        return md -> {
            ObjectSchemaNode root = md.getRoot();
            md.enterLevel(root);
            try {
                AbstractCollectionSchemaNode arr = (AbstractCollectionSchemaNode) root
                        .getOrCreateChild(PromotionTest.name("arr"), ATypeTag.ARRAY, md);
                md.enterLevel(arr);
                try {
                    ObjectSchemaNode item = (ObjectSchemaNode) arr.getOrCreateItem(ATypeTag.OBJECT, md);
                    md.enterLevel(item);
                    try {
                        for (String name : placeholders) {
                            item.getOrCreateChild(PromotionTest.name(name), ATypeTag.STRING, md);
                        }
                        MapSchemaNode map = item.getOrCreateMapNode(md);
                        md.enterLevel(map);
                        try {
                            AbstractSchemaNode value = map.getOrCreateItem(valueType, md);
                            item.incrementColumns(value.getDeltaColumnsChanged());
                        } finally {
                            md.exitLevel(map);
                        }
                    } finally {
                        md.exitLevel(item);
                    }
                } finally {
                    md.exitLevel(arr);
                }
            } finally {
                md.exitLevel(root);
            }
        };
    }

    @Test
    public void mapOnlyArrayItemReadsBackExactItemCount() throws Exception {
        Written w = write(range(PromotedAndArrayMapReadBackTest::collapsingArr, 0, 120),
                preCollapsedArrayItem(ATypeTag.STRING));
        assertAllFieldsRoundTrip("map-only item", w);
    }

    @Test
    public void mapOnlyArrayItemWithUnionValueReadsBackExactItemCount() throws Exception {
        Written w = write(collapsingThenMixed(), preCollapsedArrayItem(ATypeTag.STRING));
        assertAllFieldsRoundTrip("map-only item, union value", w);
    }

    /** Previously the item count drifted for 0-2 sibling columns (by 1 to 3 items); it must now be exact for any. */
    @Test
    public void arrayItemReadsBackExactlyForAnyNumberOfSiblingColumns() throws Exception {
        String[][] placeholders = { {}, { "zz" }, { "zz", "zy" }, { "zz", "zy", "zx" }, { "zz", "zy", "zx", "zw" } };
        for (String[] ph : placeholders) {
            for (boolean union : new boolean[] { false, true }) {
                List<String> input =
                        union ? collapsingThenMixed() : range(PromotedAndArrayMapReadBackTest::collapsingArr, 0, 120);
                Written w = write(input, preCollapsedArrayItem(ATypeTag.STRING, ph));
                assertAllFieldsRoundTrip("siblings=" + ph.length + " union=" + union, w);
            }
        }
    }

    /**
     * A value arm whose column is absent from a leaf (it first appears in a later one) reports an ancestor-level
     * missing on every round of that leaf, entry rounds included. Counting such reports, rather than rounds, filled the
     * map's count partway through a record and forwarded a spurious report. A leaf holds at most 1000 tuples, so the
     * first leaf here is all strings and the bigint, object and null arms only exist in the second.
     */
    @Test
    public void unionArmAbsentFromAnEarlierLeafReadsBackExactly() throws Exception {
        List<String> input = range(PromotedAndArrayMapReadBackTest::collapsingArr, 0, 1500);
        input.addAll(range(PromotedAndArrayMapReadBackTest::mixedArr, 2000, 200));
        input.addAll(nullAndMissingItems());
        String[][] shapes = { null, {}, { "zz" }, { "zz", "zy" } };
        for (String[] shape : shapes) {
            boolean atBirth = shape != null;
            Written w = write(input, atBirth ? preCollapsedArrayItem(ATypeTag.STRING, shape) : null);
            assertTrue("the fixture must span several leaves", w.component.pageZeros.size() >= 2);
            assertTrue("the first leaf must end before the first non-string value", w.component.pageZeros.get(0)
                    .getBuffer().getInt(AbstractColumnBTreeLeafFrame.TUPLE_COUNT_OFFSET) <= 1500);
            assertEquals("the collapsed values must be a union", ATypeTag.UNION,
                    w.arrayItem(w.root(), "arr").getMapNode().getItemNode().getTypeTag());
            String label = "arm absent from first leaf, shape=" + (atBirth ? shape.length + " siblings" : "streaming");
            assertAllFieldsRoundTrip(label, w);
            assertArrItemProjectionRoundTrips(label, w, "k5", "k100", "b2005", "o2001", "k500");
        }
    }

    @Test
    public void mapOnlyArrayItemKeepsNullAndMissingItems() throws Exception {
        for (boolean union : new boolean[] { false, true }) {
            List<String> input =
                    union ? collapsingThenMixed() : range(PromotedAndArrayMapReadBackTest::collapsingArr, 0, 120);
            input.addAll(nullAndMissingItems());
            Written w = write(input, preCollapsedArrayItem(ATypeTag.STRING));
            assertAllFieldsRoundTrip("null/missing items union=" + union, w);
            assertArrItemProjectionRoundTrips("null/missing items union=" + union, w, "k5", "k500", "b200");
        }
    }

    /**
     * The production shape: a streaming collapse leaves many column slots per item, so a whole-record
     * read was fine, but a projected read clips the item down to the requested child plus the map.
     */
    @Test
    public void projectedReadOfCollapsedArrayItemKeyReadsBackExactItemCount() throws Exception {
        Written w = write(collapsingThenMixed());
        assertTrue("arr[*] must collapse", w.arrayItem(w.root(), "arr").isCollapsed());
        assertAllFieldsRoundTrip("streaming items", w);
        // k5 is an ordinary pre-collapse column; k100, b200 and o205 live in the map.
        assertArrItemProjectionRoundTrips("streaming items", w, "k5", "k100", "b200", "o205");
    }

    @Test
    public void projectedReadOfCollapsedArrayItemKeyWithoutUnionReadsBackExactItemCount() throws Exception {
        List<String> input = range(PromotedAndArrayMapReadBackTest::collapsingArr, 0, 120);
        input.addAll(nullAndMissingItems());
        Written w = write(input);
        assertTrue("arr[*] must collapse", w.arrayItem(w.root(), "arr").isCollapsed());
        assertAllFieldsRoundTrip("streaming items, no union", w);
        assertArrItemProjectionRoundTrips("streaming items, no union", w, "k5", "k100", "k500");
    }

    /**
     * A non-repeated collapsed object whose map is its only child, under null and missing ancestors: the map now
     * reports to its parent once, which a non-repeated parent must still assemble without duplicates.
     */
    @Test
    public void mapOnlyNestedObjectWithNullAndMissingAncestorsReadsBack() throws Exception {
        List<String> input = new ArrayList<>();
        for (int d = 0; d < 6; d++) {
            input.add("{\"id\": " + d + ", \"outer\": {\"attrs\": {\"k" + d + "\": \"v" + d + "\", \"b" + d + "\": " + d
                    + ", \"o" + d + "\": {\"deep\": " + d + "}}}}");
        }
        input.add("{\"id\": 10, \"outer\": {\"attrs\": null}}");
        input.add("{\"id\": 11, \"outer\": null}");
        input.add("{\"id\": 12}");
        input.add("{\"id\": 13, \"outer\": {}}");
        input.add("{\"id\": 14, \"outer\": {\"attrs\": {}}}");
        Written w = write(input, md -> {
            ObjectSchemaNode root = md.getRoot();
            md.enterLevel(root);
            try {
                ObjectSchemaNode outer =
                        (ObjectSchemaNode) root.getOrCreateChild(PromotionTest.name("outer"), ATypeTag.OBJECT, md);
                md.enterLevel(outer);
                try {
                    ObjectSchemaNode attrs =
                            (ObjectSchemaNode) outer.getOrCreateChild(PromotionTest.name("attrs"), ATypeTag.OBJECT, md);
                    md.enterLevel(attrs);
                    try {
                        MapSchemaNode map = attrs.getOrCreateMapNode(md);
                        md.enterLevel(map);
                        try {
                            attrs.incrementColumns(map.getOrCreateItem(ATypeTag.STRING, md).getDeltaColumnsChanged());
                        } finally {
                            md.exitLevel(map);
                        }
                    } finally {
                        md.exitLevel(attrs);
                    }
                } finally {
                    md.exitLevel(outer);
                }
            } finally {
                md.exitLevel(root);
            }
        });
        assertAllFieldsRoundTrip("nested map-only", w);
        // Duplicate field names would be invisible to a JSON tree comparison.
        for (IValueReference record : w.allFields()) {
            String json = ColumnFlushHarness.json(record);
            assertEquals("duplicate attrs field in " + json, json.indexOf("\"attrs\""), json.lastIndexOf("\"attrs\""));
        }
    }

    /** Control: an uncollapsed item whose only field is an array of a union never had the defect. */
    @Test
    public void plainItemWithUnionArrayFieldReadsBack() throws Exception {
        List<String> input = new ArrayList<>();
        for (int d = 0; d < 20; d++) {
            input.add("{\"id\": " + d + ", \"arr\": [{\"t\": [\"a\" , " + d + "]}, {\"t\": [\"b" + d + "\"]}, null]}");
        }
        assertAllFieldsRoundTrip("plain item", write(input));
    }

    /** Cross-flush: the schema continues from a component written earlier, and the value becomes a union later. */
    @Test
    public void crossFlushArrayItemUnionReadsBack() throws Exception {
        List<String> flush1 = range(PromotedAndArrayMapReadBackTest::collapsingArr, 0, 120);
        List<String> flush2 = range(PromotedAndArrayMapReadBackTest::collapsingArr, 300, 20);
        flush2.addAll(range(PromotedAndArrayMapReadBackTest::mixedArr, 200, 10));
        flush2.addAll(nullAndMissingItems());
        ColumnFlushHarness harness = new ColumnFlushHarness();
        ColumnFlushHarness.Component[] out = new ColumnFlushHarness.Component[2];
        withSettings(() -> {
            int file = harness.createFile();
            FlushColumnMetadata md = harness.freshMetadata(file);
            out[0] = harness.writeComponentWith(md, file, flush1);
            int file2 = harness.createFile();
            out[1] = harness.writeComponentWith(harness.continueMetadata(file2, out[0].serializedMetadata()), file2,
                    flush2);
        });
        assertSameRecords("cross-flush F1", expected(flush1), actual(harness.assembleRecords(out[0])));
        assertSameRecords("cross-flush F2", expected(flush2), actual(harness.assembleRecords(out[1])));
    }
}
