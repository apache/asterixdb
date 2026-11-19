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

import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.hyracks.data.std.api.IValueReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * {@code storage.column.collapse.enabled}, on by default: an older binary cannot read a collapsed schema, so turning it
 * off keeps a node that never collapsed downgradable -- and must not strand a node that already collapsed.
 */
public class AdaptiveSchemaGateTest {
    private int doc;

    @Before
    public void reset() {
        CollapseSettings.reset();
        doc = 0;
    }

    @After
    public void restore() {
        CollapseSettings.reset();
    }

    @Test
    public void onByDefault() throws Exception {
        // Lowered thresholds, so that a few flushes collapse, but the flag left at its default.
        CollapseSettings.installKeepingFlag(CollapseSettings.lowered(16, 1));
        ColumnFlushHarness harness = new ColumnFlushHarness();
        assertTrue("the stream must collapse with the flag unset", anyCollapsed(flush(harness, null, 8).root));
    }

    @Test
    public void offNothingCollapses() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        Flushed last = flush(harness, null, 8);
        assertFalse("collapsed with the flag off", anyCollapsed(last.root));
        assertTrue("every new key must stay its own column: " + last.columns + " columns for " + doc + " keys",
                last.columns > doc);
    }

    @Test
    public void turningItOffKeepsACollapsedNodeCollapsed() throws Exception {
        CollapseSettings.install();
        ColumnFlushHarness harness = new ColumnFlushHarness();
        Flushed on = flush(harness, null, 8);
        assertTrue("the stream must collapse while the flag is on", anyCollapsed(on.root));

        CollapseSettings.reset();
        Flushed off = flush(harness, on.schema, 8);
        assertTrue("a collapsed node must stay collapsed once the flag is off", anyCollapsed(off.root));
        assertEquals("new keys must keep going to the map, not to new columns", on.columns, off.columns);
    }

    private record Flushed(IValueReference schema, ObjectSchemaNode root, int columns) {
    }

    /** One new key per document across small flushes: unbounded growth (see SparsityTriggerTest). */
    private Flushed flush(ColumnFlushHarness harness, IValueReference schema, int flushes) throws Exception {
        ObjectSchemaNode root = null;
        int columns = 0;
        for (int flush = 0; flush < flushes; flush++) {
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
            root = md.getRoot();
        }
        return new Flushed(schema, root, columns);
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
