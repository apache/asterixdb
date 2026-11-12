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
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.common.config.StorageProperties.Option;
import org.apache.hyracks.data.std.api.IValueReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The sparsity trigger, under the production detector defaults unless a test says otherwise.
 * <p>
 * The rate trigger cannot see growth spread across flushes smaller than its windows, or slower than one new name per
 * window: its evidence lives in counters every flush resets. Both cases below went uncollapsed before this trigger
 * existed (129 and 81 columns respectively).
 */
public class SparsityTriggerTest {

    @Before
    public void enable() {
        CollapseSettings.install();
    }

    @After
    public void reset() {
        CollapseSettings.reset();
    }

    @Test
    public void growthAcrossSmallFlushesCollapses() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        IValueReference schema = null;
        int columns = 0;
        boolean collapsed = false;
        int doc = 0;
        for (int flush = 0; flush < 8; flush++) {
            int fileId = harness.createFile();
            FlushColumnMetadata md =
                    schema == null ? harness.freshKeyedMetadata(fileId) : harness.continueKeyedMetadata(fileId, schema);
            List<String> lines = new ArrayList<>();
            for (int i = 0; i < 16; i++, doc++) {
                lines.add("{\"id\": " + doc + ", \"linked_users\": {\"u" + doc + "\": " + doc + "}}");
            }
            ColumnFlushHarness.Component c = harness.writeComponentWith(md, fileId, lines);
            schema = c.serializedMetadata();
            columns = c.getColumnCount();
            collapsed |= anyCollapsed(md.getRoot());
        }
        System.out.printf("%nSPARSITY small flushes: %d docs in flushes of 16 -> %d columns, collapsed=%s%n", doc,
                columns, collapsed);
        assertTrue("one new key per document is unbounded growth however small the flushes", collapsed);
        assertTrue("collapsing must stop the growth, not just mark it: " + columns + " columns for " + doc + " keys",
                columns < 90);
    }

    /**
     * One new key per flush, minted by the flush's first document: the trigger needs a few visits before it decides,
     * and by then the flush has no new key left to build the map with. The collapse must still outlive the flush.
     */
    @Test
    public void aKeyPerFlushCollapses() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        IValueReference schema = null;
        int columns = 0;
        int flushes = 80;
        int doc = 0;
        for (int flush = 0; flush < flushes; flush++) {
            int fileId = harness.createFile();
            FlushColumnMetadata md =
                    schema == null ? harness.freshKeyedMetadata(fileId) : harness.continueKeyedMetadata(fileId, schema);
            List<String> lines = new ArrayList<>();
            for (int i = 0; i < 16; i++, doc++) {
                lines.add("{\"id\": " + doc + ", \"daily\": {\"d" + flush + "\": " + i + "}}");
            }
            ColumnFlushHarness.Component c = harness.writeComponentWith(md, fileId, lines);
            schema = c.serializedMetadata();
            columns = c.getColumnCount();
        }
        System.out.printf("%nSPARSITY key a flush: %d flushes of 16 -> %d columns%n", flushes, columns);
        assertTrue("the collapse must be in the schema the next flush loads",
                anyCollapsed(harness.continueKeyedMetadata(harness.createFile(), schema).getRoot()));
        assertTrue("and must stop the growth: " + columns + " columns for " + flushes + " keys", columns < 70);
    }

    @Test
    public void aKeyADayCollapses() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        List<String> lines = new ArrayList<>();
        int days = 80;
        for (int day = 0; day < days; day++) {
            for (int i = 0; i < 150; i++) {
                lines.add("{\"id\": " + (day * 150 + i) + ", \"daily\": {\"d" + day + "\": " + i + "}}");
            }
        }
        int fileId = harness.createFile();
        // No primary key: the default page-zero writer inlines the key column, which overflows the harness's 4 KB
        // page at this tuple count -- a harness limit, not what this test is about.
        FlushColumnMetadata md = harness.freshMetadata(fileId);
        ColumnFlushHarness.Component c = harness.writeComponentWith(md, fileId, lines);
        System.out.printf("%nSPARSITY key a day: %d days x 150 docs -> %d columns, collapsed=%s%n", days,
                c.getColumnCount(), anyCollapsed(md.getRoot()));
        assertTrue("a new key every day is unbounded, however slowly it arrives", anyCollapsed(md.getRoot()));
        assertTrue("and collapsing must bound it: " + c.getColumnCount() + " columns", c.getColumnCount() < days);
    }

    /**
     * Seventy record types of two fields each. Once warmed up, a small batch sees only a few types, so most of the
     * node's 140 children are absent from it -- past the lowered threshold set here -- yet the schema is bounded. What
     * sets it apart is that every field a batch carries is an old one; that freshness condition is the only thing
     * standing between this schema and a collapse, which is what makes this test a guard on it.
     */
    @Test
    public void boundedPolymorphicRecordsStayColumnar() throws Exception {
        CollapseSettings.install(Map.of(Option.STORAGE_COLUMN_COLLAPSE_SPARSITY_MIN_ABSENT, 0.8));
        try {
            ColumnFlushHarness harness = new ColumnFlushHarness();
            int types = 70;
            IValueReference schema = null;
            int columns = 0;
            boolean collapsed = false;
            int doc = 0;
            for (int flush = 0; flush < 21; flush++) {
                // One large flush covers every type; the rest are small, and see a handful of types each.
                int size = flush == 0 ? 2 * types : 10;
                int fileId = harness.createFile();
                FlushColumnMetadata md = schema == null ? harness.freshKeyedMetadata(fileId)
                        : harness.continueKeyedMetadata(fileId, schema);
                List<String> lines = new ArrayList<>();
                if (flush == 0) {
                    // Every type's fields in the first document. Spread over the first documents instead, the warm-up
                    // trips the RATE trigger -- two fast windows of new names -- which cannot tell a bounded schema
                    // arriving gradually from an unbounded one; this test isolates the sparsity trigger's guard.
                    StringBuilder all = new StringBuilder("{\"id\": " + doc++ + ", \"event\": {");
                    for (int t = 0; t < types; t++) {
                        all.append(t == 0 ? "" : ", ").append("\"t").append(t).append("_a\": 1, \"t").append(t)
                                .append("_b\": \"x\"");
                    }
                    lines.add(all.append("}}").toString());
                }
                for (int i = lines.size(); i < size; i++, doc++) {
                    int t = doc % types;
                    lines.add("{\"id\": " + doc + ", \"event\": {\"t" + t + "_a\": " + doc + ", \"t" + t + "_b\": \"x"
                            + doc + "\"}}");
                }
                ColumnFlushHarness.Component c = harness.writeComponentWith(md, fileId, lines);
                schema = c.serializedMetadata();
                columns = c.getColumnCount();
                collapsed |= anyCollapsed(md.getRoot());
            }
            System.out.printf("%nSPARSITY bounded polymorphic: %d types x 2 fields, small flushes -> %d columns, "
                    + "collapsed=%s%n", types, columns, collapsed);
            assertFalse("a bounded schema whose batches carry only old fields must stay columnar", collapsed);
            assertEquals("every field keeps its column, plus the primary key", 2 * types + 1, columns);
        } finally {
            CollapseSettings.install();
        }
    }

    /**
     * The same bounded schema, its record types arriving interleaved -- two new fields per document until all seventy
     * types have been seen -- in one flush. This is a RATE-trigger case: the warm-up produces fast windows, and with
     * two required (the old default) it collapsed. Seventy documents span three windows, the last of them slow.
     */
    @Test
    public void boundedSchemaArrivingGraduallyStaysColumnar() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        int types = 70;
        List<String> lines = new ArrayList<>();
        for (int doc = 0; doc < 4 * types; doc++) {
            int t = doc % types;
            lines.add("{\"id\": " + doc + ", \"event\": {\"t" + t + "_a\": " + doc + ", \"t" + t + "_b\": \"x" + doc
                    + "\"}}");
        }
        int fileId = harness.createFile();
        // Key-less for the same harness reason as aKeyADayCollapses: an inlined key column overflows its 4 KB page.
        FlushColumnMetadata md = harness.freshMetadata(fileId);
        ColumnFlushHarness.Component c = harness.writeComponentWith(md, fileId, lines);
        System.out.printf("%nRATE gradual bounded: %d types arriving 2 fields/doc -> %d columns, collapsed=%s%n", types,
                c.getColumnCount(), anyCollapsed(md.getRoot()));
        assertFalse("a bounded schema must not collapse just because its types arrive gradually",
                anyCollapsed(md.getRoot()));
        assertEquals("every field keeps its column, plus the node's id field", 2 * types + 1, c.getColumnCount());
    }

    private static boolean anyCollapsed(AbstractSchemaNode n) {
        if (n instanceof ObjectSchemaNode o) {
            if (o.isCollapsed()) {
                return true;
            }
            for (AbstractSchemaNode ch : o.getChildren()) {
                if (anyCollapsed(ch)) {
                    return true;
                }
            }
        }
        return false;
    }
}
