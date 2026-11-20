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
package org.apache.asterix.test.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import org.apache.asterix.test.common.TestExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Collapse and promotion change only where values are stored, never what a query returns. So every query here runs
 * over two datasets loaded with exactly the same writes: a columnar one flushed under {@code cc-multisegment.conf}'s
 * collapse settings, and a row one, which nothing collapses, whose answers it must equal, field order aside. The data
 * is shaped to promote: each document brings a fresh key, so every node collapses within a few dozen documents, and
 * fields that only start after that turn dense -- scalars, nulls, a field of two types, objects, arrays, at the root,
 * nested, and under an array -- across flushes, updates, deletes and a merge.
 */
public class PromotionDifferentialTest {
    private static final String CONFIG_FILE_NAME = "src/test/resources/cc-multisegment.conf";
    private static final Path STORAGE_LOG = Paths.get("target", "storage_debug.log");
    private static final String QUERY_SERVICE = "http://127.0.0.1:19002/query/service";
    private static final String CONNECTOR = "http://127.0.0.1:19002/connector?dataverseName=diff&datasetName=";
    private static final String[] DATASETS = { "Root", "Nested", "Arr" };
    /** Row; collapsed columnar. */
    private static final String[] SIDES = { "Row", "Col" };
    private static final Logger LOGGER = LogManager.getLogger();
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static long logOffset;
    private static long logStart;

    /** Root: late scalars, nulls, a field of two types, an object, an array, a sparse field. */
    private static final String ROOT_RECORD =
            "object_add_fields({\"id\": x}, " + "[{\"field-name\": \"k\" || to_string(x), \"field-value\": x}]) "
                    + "LET late = CASE WHEN x > 2000 THEN {\"status\": \"s\" || to_string(x % 5), \"amount\": x % 100, "
                    + "\"maybe\": CASE WHEN x % 4 = 0 THEN null ELSE x END, "
                    + "\"mixed\": CASE WHEN x % 3 = 0 THEN to_string(x) ELSE x END, "
                    + "\"obj\": {\"a\": x % 3, \"b\": \"b\" || to_string(x % 2)}, \"arr\": [x % 3, x % 5]} ELSE {} END, "
                    + "sparse = CASE WHEN x > 2000 AND x % 10 = 0 THEN {\"sparse\": x} ELSE {} END";

    /** The queries, each over {@code {DS}}, a dataset of the same family; ordered, so answers compare in order. */
    private static final String[][] QUERIES = {
            // Root
            { "Root", "SELECT VALUE c FROM {DS} c ORDER BY c.id" },
            { "Root", "SELECT c.id, c.status, c.amount, c.maybe FROM {DS} c ORDER BY c.id" },
            { "Root", "SELECT VALUE c.id FROM {DS} c WHERE c.status = \"s3\" ORDER BY c.id" },
            { "Root", "SELECT VALUE c.id FROM {DS} c WHERE c.amount > 50 AND c.amount <= 70 ORDER BY c.id" },
            { "Root", "SELECT VALUE c.id FROM {DS} c WHERE c.maybe IS NULL ORDER BY c.id" },
            { "Root", "SELECT VALUE c.id FROM {DS} c WHERE c.maybe IS MISSING ORDER BY c.id" },
            { "Root", "SELECT VALUE c.id FROM {DS} c WHERE c.mixed = \"2400\" OR c.mixed = 2401 ORDER BY c.id" },
            { "Root", "SELECT VALUE c.id FROM {DS} c WHERE c.mixed > 3000 ORDER BY c.id" },
            { "Root", "SELECT VALUE c.sparse FROM {DS} c WHERE c.sparse IS NOT UNKNOWN ORDER BY c.id" },
            { "Root",
                    "SELECT status, COUNT(*) AS n, SUM(c.amount) AS s, MIN(c.maybe) AS lo, MAX(c.maybe) AS hi "
                            + "FROM {DS} c GROUP BY c.status AS status ORDER BY status" },
            { "Root", "SELECT c.id, c.obj.a FROM {DS} c WHERE c.obj.b = \"b1\" ORDER BY c.id" },
            { "Root", "SELECT c.id, v FROM {DS} c UNNEST c.arr AS v WHERE v > 2 ORDER BY c.id, v" },
            { "Root", "SELECT VALUE SUM(object_length(c)) FROM {DS} c" },
            { "Root",
                    "SELECT a.id AS a, b.id AS b FROM {DS} a JOIN {DS} b ON a.amount = b.amount "
                            + "AND a.status = b.status WHERE a.id < 2100 AND b.id > 3150 ORDER BY a, b" },
            { "Root", "SELECT VALUE c.k1999 FROM {DS} c WHERE c.k1999 = 1999" },
            { "Root", "SELECT VALUE c.k2500 FROM {DS} c WHERE c.k2500 IS NOT UNKNOWN" },
            // Nested: the collapsing node two levels down, with EVENT_DATA missing or null in some documents
            { "Nested", "SELECT VALUE c FROM {DS} c ORDER BY c.id" },
            { "Nested", "SELECT VALUE c.id FROM {DS} c WHERE c.ev.users.role = \"r1\" ORDER BY c.id" },
            { "Nested", "SELECT VALUE c.id FROM {DS} c WHERE c.ev.users.score > 25 ORDER BY c.id" },
            { "Nested", "SELECT VALUE c.id FROM {DS} c WHERE c.ev IS NULL ORDER BY c.id" },
            { "Nested", "SELECT VALUE c.id FROM {DS} c WHERE c.ev.users.score IS NULL ORDER BY c.id" },
            { "Nested", "SELECT role, COUNT(*) AS n FROM {DS} c GROUP BY c.ev.users.role AS role ORDER BY role" },
            // Arr: the collapsing node under an array
            { "Arr", "SELECT VALUE c FROM {DS} c ORDER BY c.id" },
            { "Arr", "SELECT c.id, i.hot FROM {DS} c UNNEST c.items AS i WHERE i.hot > 2500 ORDER BY c.id" } };

    @BeforeClass
    public static void setUp() throws Exception {
        logOffset = Files.exists(STORAGE_LOG) ? Files.size(STORAGE_LOG) : 0;
        logStart = System.currentTimeMillis();
        LangExecutionUtil.setUp(CONFIG_FILE_NAME, new TestExecutor());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
    }

    @Test
    public void collapsedAndPromotedDataAnswersAsRowDataDoes() throws Exception {
        execute("DROP DATAVERSE diff IF EXISTS; CREATE DATAVERSE diff; USE diff; CREATE TYPE OpenType AS {id: int};");
        for (String name : DATASETS) {
            createTwins(name);
        }
        // Mixed components: the late fields arrive diluted, then dense, then rewritten and deleted in part.
        writeAndFlush(1, 2300);
        writeAndFlush(2301, 2900);
        write(2100, 2150);
        both("DELETE FROM {DS} c WHERE c.id >= 2200 AND c.id <= 2210;");
        writeAndFlush(2901, 3200);
        compareAll("mixed components");

        for (String name : DATASETS) {
            compact(name);
        }
        compareAll("after a merge");

        writeAndFlush(3201, 3400);
        compareAll("after a merge and a flush");

        String log = readLogSince();
        assertTrue("nothing collapsed, so the columnar side tested nothing new", log.contains("collapsing an object"));
        assertTrue("nothing was promoted, so this tested no promoted key", log.contains("promoting a key out of"));
    }

    private static void compareAll(String stage) throws Exception {
        compare(stage, QUERIES, List.of());
    }

    /**
     * Runs each query over its family's datasets and requires the collapsed one to answer as the row dataset does, and
     * an answer at all, unless the query is one of {@code mayBeEmpty} -- a point lookup of a
     * deleted key.
     */
    private static void compare(String stage, String[][] queries, List<String> mayBeEmpty) throws Exception {
        List<String> failures = new ArrayList<>();
        List<String> empty = new ArrayList<>();
        for (String[] query : queries) {
            JsonNode row = results(query[1].replace("{DS}", "Row" + query[0]));
            JsonNode col = results(query[1].replace("{DS}", "Col" + query[0]));
            if (row.isEmpty() && !mayBeEmpty.contains(query[1])) {
                empty.add(query[1]);
            }
            if (!row.equals(col)) {
                failures.add(stage + ": " + query[1] + "\n  row against collapsed " + firstDifference(row, col));
            }
        }
        if (!failures.isEmpty()) {
            fail(failures.size() + " of " + queries.length + " queries answered differently\n"
                    + String.join("\n", failures));
        }
        assertTrue(stage + ": queries with no answer compare nothing: " + empty, empty.isEmpty());
    }

    private static String firstDifference(JsonNode row, JsonNode col) {
        for (int i = 0; i < Math.max(row.size(), col.size()); i++) {
            JsonNode r = row.get(i);
            JsonNode c = col.get(i);
            if (r == null || !r.equals(c)) {
                return "at " + i + " of " + row.size() + "/" + col.size() + ": row " + r + ", column " + c;
            }
        }
        return "";
    }

    /**
     * The collapsing nodes four levels down, two of them on one path, under ancestors that are missing, null, empty
     * or present in every combination; late fields of every kind; updates that move a record to another shape;
     * deletes; a merge; a secondary index on promoted fields; and every way of reaching a record -- a full scan, a key
     * range, a key lookup, an index lookup.
     */
    @Test
    public void deeplyNestedNullsAndMissingAnswerAsRowDataDoes() throws Exception {
        execute("DROP DATAVERSE diff IF EXISTS; CREATE DATAVERSE diff; USE diff; CREATE TYPE OpenType AS {id: int};");
        createTwins("Deep");
        for (String side : SIDES) {
            execute("USE diff; CREATE INDEX " + side + "S1 ON " + side + "Deep(a.b.c.d.s1: string);");
            execute("USE diff; CREATE INDEX " + side + "N2 ON " + side + "Deep(a.b.c.d.n2: int);");
        }
        deep(1, 1500, 0);
        flushDeep();
        deep(1501, 2500, 0);
        flushDeep();
        deep(900, 1100, 5);
        both("DELETE FROM {DS} c WHERE c.id >= 1200 AND c.id <= 1220;", "Deep");
        deep(2501, 3000, 0);
        flushDeep();
        compareDeep("mixed components");

        compact("Deep");
        compareDeep("after a merge");

        deep(3001, 3300, 0);
        deep(1, 50, 7);
        both("DELETE FROM {DS} c WHERE c.id >= 2600 AND c.id <= 2610;", "Deep");
        flushDeep();
        compareDeep("after a merge and a flush");

        String log = readLogSince();
        assertTrue("nothing collapsed", log.contains("collapsing an object"));
        assertTrue("nothing was promoted", log.contains("promoting a key out of"));
    }

    /** Deep record {@code x}; {@code shift} moves it to another of the twelve shapes, as an update. */
    private static ObjectNode deepRecord(int x, int shift) {
        ObjectNode record = MAPPER.createObjectNode().put("id", x);
        int variant = (x + shift) % 12;
        if (variant == 0) {
            return record;
        }
        if (variant == 1) {
            record.putNull("a");
            return record;
        }
        ObjectNode a = record.putObject("a");
        if (variant == 2) {
            return record;
        }
        if (variant == 3) {
            a.putNull("b");
            return record;
        }
        ObjectNode b = a.putObject("b").put("kb" + x, x);
        if (x > 1000) {
            b.put("bs", "b" + (x % 6));
        }
        if (variant == 4) {
            return record;
        }
        if (variant == 5) {
            b.putNull("c");
            return record;
        }
        ObjectNode c = b.putObject("c");
        if (x > 1000 && x % 4 != 0) {
            c.put("cs", x % 9);
        }
        if (variant == 6) {
            c.putNull("d");
            return record;
        }
        ObjectNode d = c.putObject("d");
        if (variant == 7) {
            return record;
        }
        d.put("kd" + x, x);
        if (x > 1000) {
            d.put("s1", "v" + ((x + shift) % 4));
            if (x % 3 == 0) {
                d.putNull("n2");
            } else {
                d.put("n2", (x + shift) % 100);
            }
            if (x % 10 < 7) {
                d.put("m3", x);
            }
            if (x % 2 == 0) {
                d.put("t4", x);
            } else {
                d.put("t4", "t" + x);
            }
            ObjectNode o5 = d.putObject("o5").put("p", x % 5);
            if (x % 3 == 0) {
                o5.putNull("q");
            } else {
                o5.putObject("q").put("r", x % 7);
            }
            d.putArray("a6").add(x % 3).addObject().put("z", x % 2);
        }
        if (x > 2200) {
            d.put("late2", x % 11);
        }
        return record;
    }

    private static void deep(int from, int to, int shift) throws Exception {
        for (int start = from; start <= to; start += 250) {
            ArrayNode batch = MAPPER.createArrayNode();
            for (int x = start; x <= Math.min(to, start + 249); x++) {
                batch.add(deepRecord(x, shift));
            }
            both("UPSERT INTO {DS} (" + batch + ");", "Deep");
        }
    }

    private static void flushDeep() throws Exception {
        flush("Deep");
    }

    private static void compareDeep(String stage) throws Exception {
        String d = "c.a.b.c.d";
        List<String[]> queries = new ArrayList<>();
        for (String q : new String[] {
                // full scans
                "SELECT VALUE c FROM {DS} c ORDER BY c.id",
                "SELECT c.id, " + d + ".s1, " + d + ".n2, " + d + ".m3, " + d + ".t4, " + d + ".late2 "
                        + "FROM {DS} c ORDER BY c.id",
                "SELECT c.id, " + d + ".o5 AS o, " + d + ".o5.q.r AS r, " + d + ".o5.q AS q FROM {DS} c ORDER BY c.id",
                "SELECT c.id, c.a.b.c AS cc FROM {DS} c ORDER BY c.id",
                "SELECT c.id, c.a.b.bs AS bs, c.a.b.c.cs AS cs FROM {DS} c ORDER BY c.id",
                "SELECT c.id, c.a AS a FROM {DS} c ORDER BY c.id",
                // null, missing and unknown at every level
                "SELECT VALUE c.id FROM {DS} c WHERE c.a IS NULL ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE c.a.b IS NULL ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE c.a.b.c IS NULL ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE " + d + " IS NULL ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE " + d + " IS MISSING ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE " + d + ".n2 IS NULL ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE " + d + ".n2 IS MISSING ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE " + d + ".m3 IS MISSING ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE " + d + ".s1 IS UNKNOWN ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE " + d + ".o5.q IS NULL ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE " + d + ".o5.q.r IS UNKNOWN ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE c.a.b.bs IS MISSING ORDER BY c.id",
                // value predicates, two of them answerable from a secondary index
                "SELECT VALUE c.id FROM {DS} c WHERE " + d + ".s1 = \"v2\" ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE " + d + ".n2 = 42 ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE " + d + ".n2 BETWEEN 10 AND 30 ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE " + d + ".t4 = \"t1501\" OR " + d + ".t4 > 2900 ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE " + d + ".o5.q.r = 3 ORDER BY c.id",
                "SELECT VALUE c.id FROM {DS} c WHERE c.a.b.c.cs = 5 AND c.a.b.bs = \"b2\" ORDER BY c.id",
                "SELECT VALUE " + d + ".kd1234 FROM {DS} c WHERE " + d + ".kd1234 = 1234",
                "SELECT VALUE c.a.b.kb2405 FROM {DS} c WHERE c.a.b.kb2405 IS NOT UNKNOWN",
                // unnest, aggregates
                "SELECT c.id, p, e FROM {DS} c UNNEST " + d + ".a6 AS e AT p ORDER BY c.id, p",
                "SELECT s, COUNT(*) AS n, SUM(" + d + ".n2) AS t, COUNT(" + d + ".m3) AS m FROM {DS} c " + "GROUP BY "
                        + d + ".s1 AS s ORDER BY s",
                "SELECT VALUE SUM(object_length(" + d + ")) FROM {DS} c",
                // key ranges across updated and deleted keys
                "SELECT VALUE c FROM {DS} c WHERE c.id >= 990 AND c.id < 1230 ORDER BY c.id",
                "SELECT c.id, " + d + ".n2, " + d + ".s1 FROM {DS} c WHERE c.id BETWEEN 2490 AND 2620 ORDER BY c.id",
                "SELECT VALUE c FROM {DS} c WHERE c.id > 2950 ORDER BY c.id" }) {
            queries.add(new String[] { "Deep", q });
        }
        // Key lookups: every shape, updated keys, deleted keys, both sides of each flush.
        List<String> mayBeEmpty = new ArrayList<>();
        for (int id : new int[] { 1, 2, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 1001, 1004, 1006, 1008, 1099, 1205,
                1499, 1501, 2201, 2604, 3002, 3300 }) {
            String whole = "SELECT VALUE c FROM {DS} c WHERE c.id = " + id;
            String deepPart =
                    "SELECT " + d + ".n2 AS n, " + d + ".o5.q AS q, c.a.b.c AS cc FROM {DS} c WHERE c.id = " + id;
            queries.add(new String[] { "Deep", whole });
            queries.add(new String[] { "Deep", deepPart });
            mayBeEmpty.add(whole);
            mayBeEmpty.add(deepPart);
        }
        compare(stage, queries.toArray(new String[0][]), mayBeEmpty);
    }

    /**
     * Documents drawn from a seed: objects and arrays nested at random, nodes that collapse at random depths and in
     * the items of arrays, keys that turn dense and are promoted while others stay in the map, nulls, missing fields
     * and type changes at every level; upserts, updates, deletes, flushes and merges in a random order; and secondary
     * indexes on keys of a collapsed node, one built while the data streams in and one over components already
     * flushed. {@code -Ddiff.seed} replays or varies a run.
     */
    @Test
    public void randomDocumentsAnswerAsRowDataDoes() throws Exception {
        long seed = Long.getLong("diff.seed", 20261106L);
        LOGGER.info("random documents from seed {}", seed);
        RandomShape shape = new RandomShape(seed);
        execute("DROP DATAVERSE diff IF EXISTS; CREATE DATAVERSE diff; USE diff; CREATE TYPE OpenType AS {id: int};");
        createTwins("Rand");
        String indexed = shape.indexed.path();
        for (String side : SIDES) {
            execute("USE diff; CREATE INDEX " + side + "H ON " + side + "Rand(" + indexed + ".h0: int);");
            execute("USE diff; CREATE INDEX " + side + "C ON " + side + "Rand(" + indexed + ".c0: int);");
        }
        Random ops = new Random(seed + 1);
        TreeSet<Integer> touched = new TreeSet<>();
        int next = 1;
        for (int round = 0; round < RandomShape.ROUNDS; round++) {
            upsertRandom(shape, next, next + RandomShape.PER_ROUND - 1, 0);
            next += RandomShape.PER_ROUND;
            // Updates move a document to another draw: another shape, other keys, another type at a path.
            List<Integer> updated = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                updated.add(1 + ops.nextInt(next - 1));
            }
            upsertRandom(shape, updated, round + 1);
            touched.addAll(updated);
            if (round % 2 == 1) {
                List<Integer> deleted = new ArrayList<>();
                for (int i = 0; i < 15; i++) {
                    deleted.add(1 + ops.nextInt(next - 1));
                }
                both("DELETE FROM {DS} c WHERE c.id IN " + deleted + ";", "Rand");
                touched.addAll(deleted);
            }
            if (round == 1 || round == 2 || round == RandomShape.ROUNDS - 1 || ops.nextInt(10) < 6) {
                flush("Rand");
            }
            if (round == 3 || round == 6) {
                compact("Rand");
            }
            if (round == 4) {
                // Built from the components on disk, collapsed ones among them, not from the records as they arrive.
                for (String side : SIDES) {
                    execute("USE diff; CREATE INDEX " + side + "L ON " + side + "Rand(" + indexed + ".c1: int);");
                }
            }
            if (round == 2 || round == 4 || round == RandomShape.ROUNDS - 1) {
                compareRandom("seed " + seed + ", round " + round, shape, round >= 4, touched, next);
            }
        }
        String log = readLogSince();
        assertTrue("seed " + seed + ": nothing collapsed", log.contains("collapsing an object"));
        assertTrue("seed " + seed + ": nothing was promoted", log.contains("promoting a key out of"));
    }

    private static void upsertRandom(RandomShape shape, int from, int to, int version) throws Exception {
        List<Integer> ids = new ArrayList<>();
        for (int id = from; id <= to; id++) {
            ids.add(id);
        }
        upsertRandom(shape, ids, version);
    }

    private static void upsertRandom(RandomShape shape, List<Integer> ids, int version) throws Exception {
        for (int start = 0; start < ids.size(); start += 200) {
            ArrayNode batch = MAPPER.createArrayNode();
            for (int id : ids.subList(start, Math.min(ids.size(), start + 200))) {
                batch.add(shape.document(id, version));
            }
            both("UPSERT INTO {DS} (" + batch + ");", "Rand");
        }
    }

    private static void compareRandom(String stage, RandomShape shape, boolean lateIndex, TreeSet<Integer> touched,
            int next) throws Exception {
        List<String[]> queries = new ArrayList<>();
        List<String> mayBeEmpty = new ArrayList<>();
        for (String q : new String[] { "SELECT VALUE c FROM {DS} c ORDER BY c.id",
                "SELECT VALUE c FROM {DS} c WHERE c.id >= 700 AND c.id < 900 ORDER BY c.id",
                "SELECT VALUE COUNT(*) FROM {DS} c" }) {
            queries.add(new String[] { "Rand", q });
        }
        for (RandomShape.Node node : shape.nodes) {
            if (node.underArray) {
                // No path reaches it from the record; the whole-record and the array queries read it.
                continue;
            }
            for (String q : shape.queries(node)) {
                queries.add(new String[] { "Rand", q });
            }
        }
        // Key lookups: documents updated, deleted or both, and some never touched.
        List<Integer> ids = new ArrayList<>(touched).subList(0, Math.min(25, touched.size()));
        for (int id : ids) {
            String q = "SELECT VALUE c FROM {DS} c WHERE c.id = " + id;
            queries.add(new String[] { "Rand", q });
            mayBeEmpty.add(q);
        }
        for (int id = 1; id < next; id += next / 10) {
            String q = "SELECT VALUE c FROM {DS} c WHERE c.id = " + id;
            queries.add(new String[] { "Rand", q });
            mayBeEmpty.add(q);
        }
        mayBeEmpty.addAll(shape.sparseQueries);
        compare(stage, queries.toArray(new String[0][]), mayBeEmpty);
        assertUsesIndex("c." + shape.indexed.path() + ".h0 = 7", "ColH");
        assertUsesIndex("c." + shape.indexed.path() + ".c0 = 7", "ColC");
        if (lateIndex) {
            assertUsesIndex("c." + shape.indexed.path() + ".c1 = 7", "ColL");
        }
    }

    /** An index lookup and a scan could answer alike only by luck if no plan used the index at all. */
    private static void assertUsesIndex(String predicate, String index) throws Exception {
        String plan = results("EXPLAIN SELECT VALUE c.id FROM ColRand c WHERE " + predicate).toString();
        assertTrue(predicate + " is answered from " + index + ":\n" + plan, plan.contains(index));
    }

    /** A random tree of fields fixed by the seed; each document fills it with values, nulls and gaps. */
    private static final class RandomShape {
        static final int ROUNDS = 8;
        static final int PER_ROUND = 400;
        private static final int MAX_DEPTH = 6;
        /** Documents before this id rarely hold the dense keys, so a node collapses before they become dense. */
        private static final int DENSE_FROM = 1200;
        private static final String[] DENSE = { "h0", "h1" };
        private static final String[] SPARSE = { "c0", "c1" };

        enum Kind {
            OBJECT,
            INT,
            STRING,
            MIXED,
            INT_ARRAY,
            OBJECT_ARRAY
        }

        static final class Node {
            final String name;
            final Kind kind;
            final Node parent;
            /** Whether it, or each item of it, gets unbounded keys and collapses. */
            final boolean collapses;
            final boolean underArray;
            final List<Node> children = new ArrayList<>();

            Node(String name, Kind kind, Node parent, boolean collapses) {
                this.name = name;
                this.kind = kind;
                this.parent = parent;
                this.collapses = collapses;
                underArray = parent != null && (parent.underArray || parent.kind == Kind.OBJECT_ARRAY);
            }

            /** The dotted path from the record, for a node not under an array. */
            String path() {
                return parent.parent == null ? name : parent.path() + "." + name;
            }
        }

        private final long seed;
        private final Node root = new Node("", Kind.OBJECT, null, false);
        final List<Node> nodes = new ArrayList<>();
        /** The deepest node of a path of objects, which collapses; the secondary indexes are on its keys. */
        final Node indexed;
        /** Queries whose answer may be empty for a seed: a filter on a value, or a path present in few documents. */
        final List<String> sparseQueries = new ArrayList<>();

        RandomShape(long seed) {
            this.seed = seed;
            Random random = new Random(seed);
            grow(root, 1, random);
            // Whatever the draw: a collapsing node at the end of a path of objects, and one in the items of an array.
            Node at = root;
            int depth = 2 + random.nextInt(MAX_DEPTH - 2);
            for (int d = 1; d <= depth; d++) {
                at = add(at, "s" + d, Kind.OBJECT, d == depth);
                if (d < depth) {
                    grow(at, d + 1, random);
                }
            }
            indexed = at;
            Node items = add(root, "items", Kind.OBJECT_ARRAY, true);
            grow(items, 2, random);
        }

        private Node add(Node parent, String name, Kind kind, boolean collapses) {
            Node node = new Node(name, kind, parent, collapses);
            parent.children.add(node);
            nodes.add(node);
            return node;
        }

        private void grow(Node parent, int depth, Random random) {
            int count = 1 + random.nextInt(4);
            for (int i = 0; i < count; i++) {
                int roll = random.nextInt(100);
                Kind kind;
                if (depth >= MAX_DEPTH) {
                    kind = roll < 40 ? Kind.INT : roll < 70 ? Kind.STRING : roll < 85 ? Kind.MIXED : Kind.INT_ARRAY;
                } else {
                    kind = roll < 30 ? Kind.OBJECT
                            : roll < 50 ? Kind.INT
                                    : roll < 65 ? Kind.STRING
                                            : roll < 75 ? Kind.MIXED : roll < 85 ? Kind.INT_ARRAY : Kind.OBJECT_ARRAY;
                }
                boolean nested = kind == Kind.OBJECT || kind == Kind.OBJECT_ARRAY;
                Node node = add(parent, "f" + i, kind, nested && random.nextInt(100) < 35);
                if (nested) {
                    grow(node, depth + 1, random);
                }
            }
        }

        /** Document {@code id} as its {@code version}-th write draws it. */
        ObjectNode document(int id, int version) {
            Random random = new Random(seed * 1_000_003L + id * 7_919L + version);
            ObjectNode document = MAPPER.createObjectNode().put("id", id);
            fill(document, root, id, random);
            return document;
        }

        private void fill(ObjectNode object, Node node, int id, Random random) {
            for (Node child : node.children) {
                int roll = random.nextInt(100);
                if (roll < 12) {
                    continue;
                }
                if (roll < 20) {
                    object.putNull(child.name);
                    continue;
                }
                switch (child.kind) {
                    case INT:
                        object.put(child.name, random.nextInt(50));
                        break;
                    case STRING:
                        object.put(child.name, "v" + random.nextInt(8));
                        break;
                    case MIXED:
                        int type = random.nextInt(3);
                        if (type == 0) {
                            object.put(child.name, random.nextInt(50));
                        } else if (type == 1) {
                            object.put(child.name, "v" + random.nextInt(8));
                        } else {
                            object.put(child.name, random.nextInt(40) / 4.0);
                        }
                        break;
                    case INT_ARRAY:
                        ArrayNode ints = object.putArray(child.name);
                        for (int i = random.nextInt(4); i > 0; i--) {
                            ints.add(random.nextInt(10));
                        }
                        break;
                    case OBJECT:
                        objectOf(object.putObject(child.name), child, id, random);
                        break;
                    case OBJECT_ARRAY:
                        ArrayNode items = object.putArray(child.name);
                        for (int i = random.nextInt(4); i > 0; i--) {
                            objectOf(items.addObject(), child, id, random);
                        }
                        break;
                }
            }
        }

        private void objectOf(ObjectNode object, Node node, int id, Random random) {
            fill(object, node, id, random);
            if (!node.collapses) {
                return;
            }
            // Unbounded keys collapse the node; dense ones are promoted out of its map; sparse ones stay in it.
            for (int i = 0; i < 2; i++) {
                object.put("r" + random.nextInt(1000), random.nextInt(100));
            }
            int densePercent = id >= DENSE_FROM ? 92 : 3;
            for (String key : DENSE) {
                if (random.nextInt(100) < densePercent) {
                    object.put(key, random.nextInt(60));
                }
            }
            for (String key : SPARSE) {
                if (random.nextInt(100) < 25) {
                    object.put(key, random.nextInt(30));
                }
            }
        }

        /** Projections, unknowns, filters and groups on a node reached by a path. */
        List<String> queries(Node node) {
            String p = "c." + node.path();
            List<String> queries = new ArrayList<>();
            queries.add("SELECT c.id, " + p + " AS v FROM {DS} c ORDER BY c.id");
            add(queries, "SELECT VALUE c.id FROM {DS} c WHERE " + p + " IS NULL ORDER BY c.id");
            queries.add("SELECT VALUE c.id FROM {DS} c WHERE " + p + " IS MISSING ORDER BY c.id");
            switch (node.kind) {
                case INT:
                    queries.addAll(keyQueries(p));
                    break;
                case STRING:
                    add(queries, "SELECT VALUE c.id FROM {DS} c WHERE " + p + " = \"v3\" ORDER BY c.id");
                    queries.add(groupBy(p));
                    break;
                case MIXED:
                    add(queries, "SELECT VALUE c.id FROM {DS} c WHERE " + p + " > 5 ORDER BY c.id");
                    add(queries, "SELECT VALUE c.id FROM {DS} c WHERE " + p + " = \"v2\" ORDER BY c.id");
                    break;
                case INT_ARRAY:
                    add(queries,
                            "SELECT c.id, i, e FROM {DS} c UNNEST " + p + " AS e AT i WHERE e > 2 ORDER BY c.id, i");
                    break;
                case OBJECT_ARRAY:
                    add(queries, "SELECT c.id, i, e FROM {DS} c UNNEST " + p + " AS e AT i ORDER BY c.id, i");
                    if (node.collapses) {
                        add(queries, "SELECT c.id, i, e.c0 FROM {DS} c UNNEST " + p + " AS e AT i WHERE e.h0 = 7 "
                                + "ORDER BY c.id, i");
                    }
                    break;
                case OBJECT:
                    add(queries, "SELECT VALUE object_length(" + p + ") FROM {DS} c ORDER BY c.id");
                    if (node.collapses) {
                        for (String key : DENSE) {
                            queries.add("SELECT c.id, " + p + "." + key + " AS v FROM {DS} c ORDER BY c.id");
                            queries.addAll(keyQueries(p + "." + key));
                        }
                        for (String key : SPARSE) {
                            queries.add("SELECT c.id, " + p + "." + key + " AS v FROM {DS} c ORDER BY c.id");
                            queries.addAll(keyQueries(p + "." + key));
                        }
                        add(queries, "SELECT VALUE c.id FROM {DS} c WHERE " + p + ".r7 IS NOT UNKNOWN ORDER BY c.id");
                        queries.add(groupBy(p + ".h0"));
                    }
                    break;
            }
            return queries;
        }

        /** The groups of a path's values; null and missing are left out, as their groups tell only the same quirk. */
        private static String groupBy(String p) {
            return "SELECT v, COUNT(*) AS n FROM {DS} c WHERE " + p + " IS NOT UNKNOWN GROUP BY " + p
                    + " AS v ORDER BY v";
        }

        /** Filters on an integer: an equality and a range, each answerable from an index on the path. */
        List<String> keyQueries(String p) {
            List<String> queries = new ArrayList<>();
            add(queries, "SELECT VALUE c.id FROM {DS} c WHERE " + p + " = 7 ORDER BY c.id");
            add(queries, "SELECT c.id, " + p + " AS v FROM {DS} c WHERE " + p + " >= 10 AND " + p + " < 20 "
                    + "ORDER BY c.id");
            return queries;
        }

        private void add(List<String> queries, String sparse) {
            queries.add(sparse);
            sparseQueries.add(sparse);
        }
    }

    private static void writeAndFlush(int from, int to) throws Exception {
        write(from, to);
        for (String name : DATASETS) {
            flush(name);
        }
    }

    private static void createTwins(String family) throws Exception {
        for (String side : SIDES) {
            String format = side.equals("Row") ? "row" : "column";
            execute("USE diff; CREATE DATASET " + side + family + "(OpenType) PRIMARY KEY id WITH {\"storage-format\": "
                    + "{\"format\": \"" + format + "\"}};");
        }
    }

    /** Flushes the family's columnar dataset. */
    private static void flush(String family) throws Exception {
        flushDataset("Col" + family);
    }

    private static void flushDataset(String dataset) throws Exception {
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(CONNECTOR + dataset)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals("the connector API flushes the dataset: " + response.body(), 200, response.statusCode());
    }

    private static void compact(String family) throws Exception {
        execute("USE diff; COMPACT DATASET Col" + family + ";");
    }

    private static void write(int from, int to) throws Exception {
        String range = "FROM RANGE(" + from + ", " + to + ") x";
        // Rewritten documents get a different status, so an update is visible in every query that reads it.
        String root = ROOT_RECORD.replace("\"s\" || to_string(x % 5)",
                "CASE WHEN " + from + " = 2100 THEN \"u\" ELSE \"s\" END || to_string(x % 5)");
        both("UPSERT INTO {DS} (SELECT VALUE object_concat(" + root.substring(0, root.indexOf(" LET ")) + ", late, "
                + "sparse) " + range + " " + root.substring(root.indexOf(" LET ")) + ");", "Root");
        both("UPSERT INTO {DS} (SELECT VALUE CASE x % 5 WHEN 0 THEN {\"id\": x} WHEN 1 THEN {\"id\": x, \"ev\": null} "
                + "ELSE {\"id\": x, \"ev\": {\"users\": object_add_fields(CASE WHEN x > 2000 THEN {\"role\": \"r\" "
                + "|| to_string(x % 3), \"score\": CASE WHEN x % 7 = 0 THEN null ELSE x % 50 END} ELSE {} END, "
                + "[{\"field-name\": \"k\" || to_string(x), \"field-value\": x}])}} END " + range + ");", "Nested");
        both("UPSERT INTO {DS} (SELECT VALUE {\"id\": x, \"items\": [object_add_fields(CASE WHEN x > 2000 THEN "
                + "{\"hot\": x} ELSE {} END, [{\"field-name\": \"k\" || to_string(x), \"field-value\": x}])]} " + range
                + ");", "Arr");
    }

    /** Runs {@code statement} over the three datasets of {@code family}, or of every family. */
    private static void both(String statement, String... family) throws Exception {
        for (String name : family.length > 0 ? family : DATASETS) {
            for (String side : SIDES) {
                execute("USE diff; " + statement.replace("{DS}", side + name));
            }
        }
    }

    private static JsonNode results(String query) throws Exception {
        JsonNode response = execute("USE diff; " + query);
        return response.get("results");
    }

    private static JsonNode execute(String statement) throws Exception {
        HttpResponse<String> response = HTTP.send(
                HttpRequest.newBuilder(URI.create(QUERY_SERVICE))
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers
                                .ofString("statement=" + URLEncoder.encode(statement, StandardCharsets.UTF_8)))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        JsonNode json = MAPPER.readTree(response.body());
        if (!"success".equals(json.path("status").asText())) {
            fail("statement failed: " + statement + "\n" + response.body());
        }
        return json;
    }

    /** The storage log written since setUp, including what rolled over into archives meanwhile. */
    private static String readLogSince() throws Exception {
        StringBuilder log = new StringBuilder();
        try (var archives = Files.list(STORAGE_LOG.getParent())) {
            for (Path archive : archives.filter(f -> f.getFileName().toString().startsWith("storage_debug-")
                    && f.toFile().lastModified() >= logStart).toList()) {
                // A roll writes the archive plain, then compresses it.
                if (!archive.toString().endsWith(".gz")) {
                    log.append(Files.readString(archive));
                    continue;
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try (GZIPInputStream in = new GZIPInputStream(Files.newInputStream(archive))) {
                    in.transferTo(out);
                } catch (EOFException e) {
                    // Still being compressed: the plain archive it is made from is listed too, so keep what was read.
                }
                log.append(out.toString(StandardCharsets.UTF_8));
            }
        }
        byte[] bytes = Files.readAllBytes(STORAGE_LOG);
        // A roll since setUp started this file afresh.
        int start = log.length() == 0 && logOffset <= bytes.length ? (int) logOffset : 0;
        return log.append(new String(bytes, start, bytes.length - start, StandardCharsets.UTF_8)).toString();
    }
}
