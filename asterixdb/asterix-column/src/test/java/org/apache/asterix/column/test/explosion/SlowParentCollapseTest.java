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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.common.config.StorageProperties.Option;
import org.apache.hyracks.data.std.api.IValueReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A key set that grows slowly while each key's value grows fast: {@code by_day} gains a day key every few dozen or
 * hundred documents, too few names for either streak, while every day is keyed by event ID.
 */
public class SlowParentCollapseTest {
    private static final int DAYS = 5;
    // Small windows keep every collapsed day within what the harness's page zero can hold.
    private static final Map<Option, Object> SMALL_WINDOWS = Map.of(Option.STORAGE_COLUMN_COLLAPSE_WINDOW_DOCUMENTS, 8,
            Option.STORAGE_COLUMN_COLLAPSE_WINDOW_NEW_NAMES, 4, Option.STORAGE_COLUMN_COLLAPSE_MIN_CHILDREN, 16);

    @Before
    public void configure() {
        Map<Option, Object> settings = new HashMap<>(SMALL_WINDOWS);
        settings.put(Option.STORAGE_COLUMN_COLLAPSE_GROWING_CHILDREN, 2);
        CollapseSettings.install(settings);
    }

    @After
    public void restore() {
        CollapseSettings.reset();
    }

    /** Busy days: each day collapses on its own. */
    @Test
    public void parentCollapsesOnceItsKeysRepeatTheBlowUp() throws Exception {
        assertByDayCollapses(200, false);
    }

    /** Quiet days: too few documents a day for any day to collapse, yet each keeps minting event IDs. */
    @Test
    public void parentCollapsesWhenItsKeysGrowTooSlowlyToCollapse() throws Exception {
        assertByDayCollapses(20, false);
    }

    /** Quiet days holding their event IDs under a fixed field: the day mints one name, its events field the rest. */
    @Test
    public void parentCollapsesWhenItsKeysSitUnderAFixedField() throws Exception {
        assertByDayCollapses(20, true);
    }

    private static void assertByDayCollapses(int documentsPerDay, boolean nested) throws Exception {
        Random random = new Random(42);
        List<String> lines = new ArrayList<>();
        for (int d = 0; d < DAYS * documentsPerDay; d++) {
            lines.add(byDayRecord(random, d, documentsPerDay, nested));
        }
        FlushColumnMetadata md = WideValueCollapseTest.schemaOf(lines);
        ObjectSchemaNode byDay = (ObjectSchemaNode) child(md.getRoot(), 1);
        assertTrue("by_day must collapse once a second day grows", byDay.isCollapsed());
        // The first two days keep their columns; every later day is an entry of by_day's map, or promoted after it.
        assertEquals("days before the map", 2, byDay.getChildren().indexOf(byDay.getMapNode()));

        ColumnFlushHarness harness = new ColumnFlushHarness();
        ColumnFlushHarness.Component component = harness.writeComponent(lines);
        ObjectMapper mapper = new ObjectMapper();
        List<IValueReference> out = harness.assembleRecords(component);
        for (int d = 0; d < lines.size(); d++) {
            assertEquals("record " + d, mapper.readTree(lines.get(d)),
                    mapper.readTree(ColumnFlushHarness.json(out.get(d))));
        }
    }

    /**
     * Busy days written in flushes far smaller than a day. by_day decides to collapse when a day starts growing, a
     * document or two after that day's first; the rest of the flush brings no new day to build the map with, so the
     * decision has to be recorded without one.
     */
    @Test
    public void parentCollapsesAcrossSmallFlushes() throws Exception {
        int documentsPerDay = 200;
        int flushSize = 8;
        Random random = new Random(42);
        // Each day keeps about 160 columns before it collapses: more than the default 4 KB page zero holds.
        ColumnFlushHarness harness = new ColumnFlushHarness(64 * 1024);
        IValueReference schema = null;
        for (int d = 0; d < DAYS * documentsPerDay; d += flushSize) {
            List<String> lines = new ArrayList<>();
            for (int i = d; i < d + flushSize; i++) {
                lines.add(byDayRecord(random, i, documentsPerDay, false));
            }
            int fileId = harness.createFile();
            FlushColumnMetadata md =
                    schema == null ? harness.freshMetadata(fileId) : harness.continueMetadata(fileId, schema);
            schema = harness.writeComponentWith(md, fileId, lines).serializedMetadata();
        }
        ObjectSchemaNode byDay =
                (ObjectSchemaNode) child(harness.continueMetadata(harness.createFile(), schema).getRoot(), 1);
        assertTrue("by_day must collapse once a second day grows", byDay.isCollapsed());
        assertEquals("days before the map", 2, byDay.getChildren().indexOf(byDay.getMapNode()));
    }

    /**
     * A flush that saw a day grow, rolled back: the day is gone from the schema, so it must not still count towards
     * collapsing the node. The node keyed by day is the root here, as only the node a rollback restores in place, not
     * one it reads back anew, could carry its counts over.
     */
    @Test
    public void anAbortedFlushDoesNotCountTowardsTheCollapse() throws Exception {
        Map<Option, Object> settings = new HashMap<>(SMALL_WINDOWS);
        settings.put(Option.STORAGE_COLUMN_COLLAPSE_GROWING_CHILDREN, 3);
        CollapseSettings.install(settings);
        assertFalse("a day growing again after the rollback counts once", rootCollapsesAfter(true));
        assertTrue("the fixture must collapse the root when nothing is rolled back", rootCollapsesAfter(false));
    }

    /** Writes a growing day per flush, three in all; with {@code abortSecond}, the second flush rolls back. */
    private static boolean rootCollapsesAfter(boolean abortSecond) throws Exception {
        Random random = new Random(42);
        ColumnFlushHarness harness = new ColumnFlushHarness(64 * 1024);
        FlushColumnMetadata md = harness.freshMetadata(harness.createFile());
        int documentsPerDay = 20;
        String[] days = { "2024-01-01", "2024-01-02", abortSecond ? "2024-01-02" : "2024-01-03" };
        for (int f = 0; f < days.length; f++) {
            List<String> lines = new ArrayList<>();
            for (int d = 0; d < documentsPerDay; d++) {
                lines.add("{\"id\": " + (f * documentsPerDay + d) + ", \"" + days[f] + "\": " + uuidMap(random) + "}");
            }
            harness.writeNextComponent(md, lines);
            if (abortSecond && f == 1) {
                md.abort();
            } else {
                md.swapSerializedColumnsMetadata();
            }
        }
        return md.getRoot().isCollapsed();
    }

    private static String byDayRecord(Random random, int d, int documentsPerDay, boolean nested) {
        String day = "2024-01-0" + (1 + d / documentsPerDay);
        String events = nested ? "{\"events\": " + uuidMap(random) + "}" : uuidMap(random);
        return "{\"id\": " + d + ", \"by_day\": {\"" + day + "\": " + events + "}}";
    }

    /** Two map fields that collapse side by side are a record holding maps, not a key set repeating them. */
    @Test
    public void recordWithTwoMapFieldsStaysColumnar() throws Exception {
        Random random = new Random(42);
        List<String> lines = new ArrayList<>();
        for (int d = 0; d < 400; d++) {
            lines.add("{\"id\": " + d + ", \"attrs\": " + uuidMap(random) + ", \"tags\": " + uuidMap(random) + "}");
        }
        FlushColumnMetadata md = WideValueCollapseTest.schemaOf(lines);
        ObjectSchemaNode root = md.getRoot();
        assertFalse("the record must stay columnar", root.isCollapsed());
        assertTrue("attrs must collapse", ((ObjectSchemaNode) child(root, 1)).isCollapsed());
        assertTrue("tags must collapse", ((ObjectSchemaNode) child(root, 2)).isCollapsed());
    }

    /**
     * A record that gains object fields over time, each filling in its optional fields: every one of them grows, but a
     * record gains only a few, short of the default eight growing children.
     */
    @Test
    public void recordGainingObjectFieldsStaysColumnar() throws Exception {
        CollapseSettings.install(SMALL_WINDOWS);
        Random random = new Random(42);
        String[] objects = { "address", "payment", "shipping", "loyalty", "support" };
        List<String> lines = new ArrayList<>();
        for (int d = 0; d < 200; d++) {
            StringBuilder sb = new StringBuilder("{\"id\": ").append(d);
            for (int o = 0; o <= Math.min(objects.length - 1, d / 20); o++) {
                sb.append(", \"").append(objects[o]).append("\": {");
                for (int f = 0; f < 4; f++) {
                    sb.append(f == 0 ? "" : ", ").append('"').append(objects[o]).append(random.nextInt(30)).append('_')
                            .append(f).append("\": ").append(d);
                }
                sb.append('}');
            }
            lines.add(sb.append('}').toString());
        }
        ObjectSchemaNode root = WideValueCollapseTest.schemaOf(lines).getRoot();
        assertFalse("the record must stay columnar", root.isCollapsed());
    }

    /**
     * The same record gaining object fields, each holding a map: every one grows through its map, as a day holding its
     * events does, but a record gains only a few.
     */
    @Test
    public void recordGainingMapHoldingFieldsStaysColumnar() throws Exception {
        CollapseSettings.install(SMALL_WINDOWS);
        Random random = new Random(42);
        String[] objects = { "address", "payment", "shipping", "loyalty", "support" };
        List<String> lines = new ArrayList<>();
        for (int d = 0; d < 200; d++) {
            StringBuilder sb = new StringBuilder("{\"id\": ").append(d);
            for (int o = 0; o <= Math.min(objects.length - 1, d / 20); o++) {
                sb.append(", \"").append(objects[o]).append("\": {\"attrs\": ").append(uuidMap(random)).append('}');
            }
            lines.add(sb.append('}').toString());
        }
        ObjectSchemaNode root = WideValueCollapseTest.schemaOf(lines).getRoot();
        assertFalse("the record must stay columnar", root.isCollapsed());
        assertTrue("while each attrs collapses",
                ((ObjectSchemaNode) ((ObjectSchemaNode) child(root, 1)).getChildren().get(0)).isCollapsed());
    }

    private static AbstractSchemaNode child(ObjectSchemaNode node, int index) {
        return node.getChildren().get(index);
    }

    private static String uuidMap(Random r) {
        StringBuilder sb = new StringBuilder("{");
        for (int k = 0; k < 2; k++) {
            sb.append(k == 0 ? "" : ", ").append('"').append(new UUID(r.nextLong(), r.nextLong())).append("\": ")
                    .append(r.nextInt(100));
        }
        return sb.append('}').toString();
    }
}
