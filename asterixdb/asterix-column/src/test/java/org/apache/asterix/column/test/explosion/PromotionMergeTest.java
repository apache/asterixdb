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

import static org.apache.asterix.column.test.explosion.ColumnLsmHarnessTest.assertSameProjection;
import static org.apache.asterix.column.test.explosion.ColumnLsmHarnessTest.assertSameRecords;
import static org.apache.asterix.column.test.explosion.ColumnLsmHarnessTest.records;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.apache.asterix.column.metadata.PromotionBoundaries;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.lsm.merge.MergeColumnReadMetadata;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A merge moves a promoted scalar key's values out of the map entries of inputs written before its promotion and into
 * its column, so the merged component holds all of them there, and reads of it skip the map.
 */
public class PromotionMergeTest {
    private static final ARecordType HOT = new ARecordType("ALL", new String[] { "id", "hot" },
            new IAType[] { BuiltinType.ANY, BuiltinType.ANY }, true);

    @Before
    public void configure() {
        CollapseSettings.install(CollapseSettings.lowered(16, 1));
    }

    @After
    public void restore() {
        CollapseSettings.reset();
    }

    @Test
    public void aMergeMovesAPromotedScalarOutOfTheMap() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all = write(lsm, d -> d < 200 ? "" : ", \"hot\": " + d, 200, 300, 300);
            lsm.mergeAll();
            assertTrue("the merge must leave hot in its column alone", clean(lsm, "hot"));
            assertSameRecords(all, lsm.scan());
            assertOneOccurrence(lsm.scan(), "\"hot\"");
            assertSameProjection(all, lsm.scan(HOT), "hot");
        }
    }

    /** Map values of another type than the promoted column's stay in the map, which the merge records. */
    @Test
    public void aValueOfAnotherTypeStaysInTheMap() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all = write(lsm, d -> d < 200 ? "" : d < 230 ? ", \"hot\": \"s" + d + "\"" : ", \"hot\": " + d,
                    200, 300, 300);
            lsm.mergeAll();
            assertFalse("strings in a numeric key's map entries keep it in the map", clean(lsm, "hot"));
            assertSameRecords(all, lsm.scan());
            assertSameProjection(all, lsm.scan(HOT), "hot");
        }
    }

    /**
     * Many keys of one node promoted at once, of different types, among a wide map: a merge walks each record's
     * entries once for all of them, and moves every one into its column.
     */
    @Test
    public void aMergeMovesManyKeysOfOneNodeTogether() throws Exception {
        CollapseSettings.install(CollapseSettings.lowered(16, 1));
        IntFunction<String> many = d -> {
            if (d < 200) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            for (int h = 0; h < 6; h++) {
                sb.append(", \"hot").append(h).append("\": ").append(d * 10 + h);
            }
            sb.append(", \"name\": \"n").append(d).append("\", \"flag\": ").append(d % 2 == 0);
            for (int w = 0; w < 20; w++) {
                sb.append(", \"wide").append(d).append('_').append(w).append("\": ").append(w);
            }
            return sb.toString();
        };
        try (ColumnLsmHarness lsm = new ColumnLsmHarness(128 * 1024)) {
            List<String> all = write(lsm, many, 200, 300, 300);
            lsm.mergeAll();
            for (String key : List.of("hot0", "hot1", "hot2", "hot3", "hot4", "hot5", "name", "flag")) {
                assertTrue("the merge must leave " + key + " in its column alone", clean(lsm, key));
            }
            assertSameRecords(all, lsm.scan());
        }
    }

    /** A merge's second reader of a column, made for each movable key, is configured as the first one is. */
    @Test
    public void aSecondReaderOfAColumnMatchesTheFirst() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            write(lsm, d -> d < 200 ? "" : ", \"hot\": " + d + ", \"s\": \"v" + d + "\"", 200, 300);
            MergeColumnReadMetadata metadata = MergeColumnReadMetadata.create(ColumnFlushHarness.KEYED_TYPE, null, 1,
                    new ColumnValueReaderFactory(),
                    ColumnUtil.getColumnMetadataCopy(lsm.diskComponents().get(0).getMetadata()));
            IColumnValuesReader[] readers = metadata.getColumnReaders();
            assertTrue("the fixture must have columns to read", readers.length > 3);
            for (int i = 0; i < readers.length; i++) {
                IColumnValuesReader second = metadata.createColumnReader(i);
                assertEquals(i, second.getColumnIndex());
                assertEquals("column " + i, readers[i].getTypeTag(), second.getTypeTag());
                assertEquals("column " + i, readers[i].getMaxLevel(), second.getMaxLevel());
                assertEquals("column " + i, readers[i].isRepeated(), second.isRepeated());
            }
        }
    }

    /**
     * A key the merge could not move holds back the keys promoted after it: the boundary is one limit per node, and
     * promotions are rare enough that reading a later key's map too costs little. Both still read back as written.
     */
    @Test
    public void aKeyLeftInTheMapHoldsBackKeysPromotedAfterIt() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all =
                    write(lsm, d -> (d < 200 ? "" : d < 230 ? ", \"hot\": \"s" + d + "\"" : ", \"hot\": " + d)
                            + (d < 260 ? "" : ", \"late\": " + d), 200, 300, 300);
            lsm.mergeAll();
            assertFalse("strings in hot's map entries keep it in the map", clean(lsm, "hot"));
            assertFalse("late, promoted after hot, is read from the map too", clean(lsm, "late"));
            assertSameRecords(all, lsm.scan());
            assertSameProjection(all, lsm.scan(HOT), "hot");
        }
    }

    /** Overwritten and deleted records are skipped by the merge, the map lookups along with every column. */
    @Test
    public void overwrittenAndDeletedRecordsAreSkippedInStep() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            IntFunction<String> hot = d -> d < 200 ? "" : ", \"hot\": " + d;
            List<String> all = write(lsm, hot, 200, 300);
            List<String> newer = records(210, 10, d -> ", \"hot\": " + (d * 10));
            lsm.insert(newer);
            lsm.delete(220, 221, 222, 223, 224);
            lsm.insert(records(500, 300, hot));
            lsm.flush();
            List<String> expected = new ArrayList<>();
            for (String record : all) {
                int id = id(record);
                if (id >= 210 && id < 220) {
                    expected.add(newer.get(id - 210));
                } else if (id < 220 || id >= 225) {
                    expected.add(record);
                }
            }
            expected.addAll(records(500, 300, hot));
            lsm.mergeAll();
            assertTrue(clean(lsm, "hot"));
            assertSameRecords(expected, lsm.scan());
            assertSameProjection(expected, lsm.scan(HOT), "hot");
        }
    }

    /** A null written to the map before the promotion moves as a null, not as a missing value. */
    @Test
    public void aNullInTheMapMovesAsANull() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all =
                    write(lsm, d -> d < 200 ? "" : d < 210 ? ", \"hot\": null" : ", \"hot\": " + d, 200, 300, 300);
            lsm.mergeAll();
            assertTrue(clean(lsm, "hot"));
            assertSameRecords(all, lsm.scan());
            assertSameProjection(all, lsm.scan(HOT), "hot");
        }
    }

    /** A collapsed node below the root: its promoted key moves at the key's own level. */
    @Test
    public void aKeyPromotedBelowTheRootMoves() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all = new ArrayList<>();
            int from = 0;
            for (int size : new int[] { 200, 300, 300 }) {
                List<String> batch = new ArrayList<>();
                for (int d = from; d < from + size; d++) {
                    // Every fourth record has no attrs at all, so missing must stay missing at the right level.
                    batch.add(d % 4 == 3 ? "{\"id\": " + d + "}"
                            : "{\"id\": " + d + ", \"attrs\": {\"k" + d + "\": " + d
                                    + (d >= 200 ? ", \"hot\": " + d : "") + "}}");
                }
                lsm.insert(batch);
                lsm.flush();
                all.addAll(batch);
                from += size;
            }
            lsm.mergeAll();
            FlushColumnMetadata schema = lsm.newestSchema();
            ObjectSchemaNode attrs = (ObjectSchemaNode) schema.getRoot().getChild(index(schema, "attrs"));
            assertTrue("attrs must be collapsed", attrs.isCollapsed());
            assertTrue("hot must move out of attrs' map", attrs.holdsEveryValueOfItsKey(index(schema, "hot"),
                    PromotionBoundaries.read(lsm.annotations().get(0))));
            assertSameRecords(all, lsm.scan());
        }
    }

    /** Under an array a key is repeated: the merge leaves it in the map and says so. */
    @Test
    public void aKeyPromotedUnderAnArrayStaysInTheMap() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all = new ArrayList<>();
            int from = 0;
            for (int size : new int[] { 200, 300, 300 }) {
                List<String> batch = new ArrayList<>();
                for (int d = from; d < from + size; d++) {
                    batch.add("{\"id\": " + d + ", \"arr\": [{\"k" + d + "\": " + d
                            + (d >= 200 ? ", \"hot\": " + d : "") + "}]}");
                }
                lsm.insert(batch);
                lsm.flush();
                all.addAll(batch);
                from += size;
            }
            lsm.mergeAll();
            assertSameRecords(all, lsm.scan());
        }
    }

    /**
     * The key's column is born in a later flush than some records whose ancestor is null or missing. Read from the
     * merged column alone, those records must keep their ancestor's null: the column of an input without it reads as
     * missing at level 0, so the merge takes their levels from the node's map key column.
     */
    @Test
    public void aMovedColumnKeepsItsAncestorsNulls() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all = new ArrayList<>();
            int from = 0;
            for (int size : new int[] { 200, 300, 300 }) {
                List<String> batch = nestedUnderNullable(from, size, 200);
                lsm.insert(batch);
                lsm.flush();
                all.addAll(batch);
                from += size;
            }
            lsm.mergeAll();
            assertTrue("hot must move out of users' map", usersClean(lsm));
            assertSameNesting(all, lsm.scan(EV_USERS_HOT));
        }
    }

    /**
     * An input written before the node collapsed holds neither the key's column nor the map to tell its records'
     * levels by, so the merge leaves the key to be read from the map too. A projection of the key cannot tell those
     * records' null ev from a missing one either way: every column it reads was born after them, which holds for any
     * nested field a later component introduces, collapsed or not.
     */
    @Test
    public void anInputFromBeforeTheCollapseKeepsTheKeyInTheMap() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all = new ArrayList<>();
            int from = 0;
            // Too few records for users to collapse in the first flush.
            for (int size : new int[] { 10, 200, 300, 300 }) {
                List<String> batch = nestedUnderNullable(from, size, 210);
                lsm.insert(batch);
                lsm.flush();
                all.addAll(batch);
                from += size;
            }
            lsm.mergeAll();
            assertFalse("the first input's records have no levels to give the key's column", usersClean(lsm));
            assertSameRecords(all, lsm.scan());
        }
    }

    /**
     * A restart drops what the flush metadata counted and the promotions it had chosen but not yet made; the ones made
     * are in the schema. Restarted within and after every flush, with a root key that rotates every 60 records,
     * every record still reads back as written, and a merge still moves hot.
     */
    @Test
    public void restartsWithinAndBetweenFlushesLoseNoValue() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all = new ArrayList<>();
            int from = 0;
            for (int size : new int[] { 200, 150, 150, 300, 40, 40, 300 }) {
                List<String> batch = new ArrayList<>();
                for (String record : nestedUnderNullable(from, size, 200)) {
                    int d = id(record);
                    batch.add(record.replace("{\"id\": " + d, "{\"id\": " + d + ", \"day" + d / 60 + "\": " + d));
                }
                // Deactivating flushes the half written so far, so each flush becomes two components.
                lsm.insert(batch.subList(0, size / 2));
                lsm.restart();
                lsm.insert(batch.subList(size / 2, size));
                lsm.flush();
                lsm.restart();
                all.addAll(batch);
                from += size;
                assertSameRecords(all, lsm.scan());
                assertSameNesting(all, lsm.scan(EV_USERS_HOT));
            }
            lsm.mergeAll();
            lsm.restart();
            assertTrue("hot must move out of users' map", usersClean(lsm));
            assertSameRecords(all, lsm.scan());
            assertSameNesting(all, lsm.scan(EV_USERS_HOT));
        }
    }

    /**
     * Arrays of objects inside a collapsed node, and a collapsed node inside those arrays, with nulls and missing values
     * at every level: a key promoted under an array and an array-valued promoted key stay in the map, a scalar beside
     * them moves.
     */
    @Test
    public void arraysInsideCollapsedNodesReadBackAsWritten() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all = new ArrayList<>();
            int from = 0;
            for (int size : new int[] { 200, 300, 300 }) {
                List<String> batch = new ArrayList<>();
                for (int d = from; d < from + size; d++) {
                    batch.add(withArrays(d));
                }
                lsm.insert(batch);
                lsm.flush();
                all.addAll(batch);
                from += size;
                assertSameRecords(all, lsm.scan());
                assertSamePath(all, lsm.scan(ATTRS_WARM_LIST), "attrs", "warm");
                assertSamePath(all, lsm.scan(ATTRS_WARM_LIST), "attrs", "list");
            }
            lsm.mergeAll();
            FlushColumnMetadata schema = lsm.newestSchema();
            ObjectSchemaNode attrs = (ObjectSchemaNode) schema.getRoot().getChild(index(schema, "attrs"));
            assertTrue("warm must move out of attrs' map", attrs.holdsEveryValueOfItsKey(index(schema, "warm"),
                    PromotionBoundaries.read(lsm.annotations().get(0))));
            assertSameRecords(all, lsm.scan());
            assertSamePath(all, lsm.scan(ATTRS_WARM_LIST), "attrs", "warm");
            assertSamePath(all, lsm.scan(ATTRS_WARM_LIST), "attrs", "list");
        }
    }

    private static final ARecordType ATTRS_WARM_LIST;
    static {
        ARecordType attrs = new ARecordType("attrs", new String[] { "warm", "list" },
                new IAType[] { BuiltinType.ANY, BuiltinType.ANY }, true);
        ATTRS_WARM_LIST =
                new ARecordType("ALL", new String[] { "id", "attrs" }, new IAType[] { BuiltinType.ANY, attrs }, true);
    }

    /**
     * attrs missing, null, or a node of fresh keys, each an array holding an object with a collapsing tags node, a
     * null and an empty object; from record 200 also "warm", a scalar, and from 500 "list", an array of objects or
     * nulls. A component records one boundary per node, below its lowest key still in the map, so warm must be
     * promoted before list for its move to show. warm is never null: the map's values are a union whose nulls go to
     * the arrays' columns, which a merge does not read, so a null warm in the map would keep warm there.
     */
    private static String withArrays(int d) {
        if (d % 7 == 0) {
            return "{\"id\": " + d + "}";
        }
        if (d % 7 == 1) {
            return "{\"id\": " + d + ", \"attrs\": null}";
        }
        String tags = d % 11 == 0 ? "null" : "{\"t" + d + "\": " + d + ", \"hot\": " + (d % 3 == 0 ? "null" : d) + "}";
        StringBuilder attrs =
                new StringBuilder("{\"k" + d + "\": [{\"x\": " + d + ", \"tags\": " + tags + "}, null, {}]");
        if (d >= 200 && d % 13 != 0) {
            attrs.append(", \"warm\": ").append(String.valueOf(d));
        }
        if (d >= 500) {
            attrs.append(", \"list\": [{\"a\": ").append(d).append("}, ").append(d % 2 == 0 ? "null" : "{\"a\": null}")
                    .append("]");
        }
        return "{\"id\": " + d + ", \"attrs\": " + attrs + "}}";
    }

    /** The value at {@code path} in each record, or where it stops: MISSING or NULL, at whichever level. */
    private static void assertSamePath(List<String> expected, List<String> actual, String... path) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("record " + i + " " + String.join(".", path), at(mapper.readTree(expected.get(i)), path),
                    at(mapper.readTree(actual.get(i)), path));
        }
    }

    private static String at(JsonNode node, String... path) {
        for (String name : path) {
            node = node.get(name);
            if (node == null) {
                return "MISSING at " + name;
            }
            if (node.isNull()) {
                return "NULL at " + name;
            }
        }
        return node.toString();
    }

    private static final ARecordType EV_USERS_HOT;
    static {
        ARecordType users = new ARecordType("users", new String[] { "hot" }, new IAType[] { BuiltinType.ANY }, true);
        ARecordType ev = new ARecordType("ev", new String[] { "users" }, new IAType[] { users }, true);
        EV_USERS_HOT = new ARecordType("ALL", new String[] { "id", "ev" }, new IAType[] { BuiltinType.ANY, ev }, true);
    }

    /** Records whose ev is missing, null, or holds users with a fresh key and, from {@code hotFrom}, "hot". */
    private static List<String> nestedUnderNullable(int from, int count, int hotFrom) {
        List<String> batch = new ArrayList<>();
        for (int d = from; d < from + count; d++) {
            batch.add(d % 5 == 0 ? "{\"id\": " + d + "}"
                    : d % 5 == 1 ? "{\"id\": " + d + ", \"ev\": null}"
                            : "{\"id\": " + d + ", \"ev\": {\"users\": {\"k" + d + "\": " + d
                                    + (d >= hotFrom ? ", \"hot\": " + d : "") + "}}}");
        }
        return batch;
    }

    private static boolean usersClean(ColumnLsmHarness lsm) throws Exception {
        FlushColumnMetadata schema = lsm.newestSchema();
        ObjectSchemaNode ev = (ObjectSchemaNode) schema.getRoot().getChild(index(schema, "ev"));
        ObjectSchemaNode users = (ObjectSchemaNode) ev.getChild(index(schema, "users"));
        assertTrue("hot must be promoted out of users", PromotionTest.promoted(schema, users, "hot"));
        return users.holdsEveryValueOfItsKey(index(schema, "hot"), PromotionBoundaries.read(lsm.annotations().get(0)));
    }

    /** ev read through a projection of ev.users.hot: missing, null, or users' hot, exactly as written. */
    private static void assertSameNesting(List<String> expected, List<String> actual) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            JsonNode want = mapper.readTree(expected.get(i)).get("ev");
            JsonNode got = mapper.readTree(actual.get(i)).get("ev");
            assertEquals("record " + i + " ev", nesting(want), nesting(got));
        }
    }

    private static String nesting(JsonNode ev) {
        if (ev == null) {
            return "MISSING";
        }
        if (ev.isNull()) {
            return "NULL";
        }
        return String.valueOf(ev.path("users").get("hot"));
    }

    private static int index(FlushColumnMetadata schema, String name) throws Exception {
        return schema.getFieldNamesDictionary().getFieldNameIndex(PromotionTest.name(name));
    }

    private static int id(String record) throws Exception {
        return new ObjectMapper().readTree(record).get("id").asInt();
    }

    /** Writes one flush per size, documents numbered on from 0, each carrying {@code extra(d)}. */
    static List<String> write(ColumnLsmHarness lsm, IntFunction<String> extra, int... flushSizes) throws Exception {
        List<String> all = new ArrayList<>();
        int from = 0;
        for (int size : flushSizes) {
            List<String> batch = records(from, size, extra);
            lsm.insert(batch);
            lsm.flush();
            all.addAll(batch);
            from += size;
        }
        return all;
    }

    /** Whether the newest component's boundaries put {@code field}, promoted out of the root, in its column alone. */
    static boolean clean(ColumnLsmHarness lsm, String field) throws Exception {
        FlushColumnMetadata schema = lsm.newestSchema();
        ObjectSchemaNode root = schema.getRoot();
        assertTrue(field + " must be promoted", PromotionTest.promoted(schema, root, field));
        int index = schema.getFieldNamesDictionary().getFieldNameIndex(PromotionTest.name(field));
        return root.holdsEveryValueOfItsKey(index, PromotionBoundaries.read(lsm.annotations().get(0)));
    }

    /** Each record names {@code quotedName} once: a stale map entry must not repeat a field read from its column. */
    static void assertOneOccurrence(List<String> records, String quotedName) {
        for (String record : records) {
            int first = record.indexOf(quotedName);
            if (first >= 0) {
                assertEquals(record, -1, record.indexOf(quotedName, first + 1));
            }
        }
    }
}
