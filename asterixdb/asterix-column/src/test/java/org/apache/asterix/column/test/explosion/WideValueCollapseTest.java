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
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.common.buffer.DummyBufferCache;
import org.apache.asterix.column.common.buffer.TestWriteMultiPageOp;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.ColumnTransformer;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.common.config.StorageProperties.Option;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Keys whose values are objects, so every new name costs a column per leaf.
 * <p>
 * The name-based bounds let such a node mint every name of {@code growingWindows} full windows before collapsing: a
 * timestamp-keyed node with 100-leaf values reached tens of thousands of columns that way, enough that its flush could
 * not reserve a buffer per column. Counting the columns a window's new names created closes those windows early.
 */
public class WideValueCollapseTest {
    private static final int LEAVES = 8;
    private static final int KEYS_PER_DOCUMENT = 3;
    private static final int DOCUMENTS = 200;
    private static final int WINDOW_MAX_NEW_COLUMNS = 32;

    @Before
    public void configure() {
        // minChildren stays at its default (64): these keys pass the gate on columns long before that many children.
        CollapseSettings.install(Map.of(Option.STORAGE_COLUMN_COLLAPSE_MIN_COLUMNS, 128,
                Option.STORAGE_COLUMN_COLLAPSE_WINDOW_MAX_NEW_COLUMNS, WINDOW_MAX_NEW_COLUMNS));
    }

    @After
    public void restore() {
        CollapseSettings.reset();
    }

    @Test
    public void objectValuedKeysCollapseOnColumnsNotNames() throws Exception {
        List<String> lines = new ArrayList<>();
        for (int d = 0; d < DOCUMENTS; d++) {
            StringBuilder sb = new StringBuilder("{\"id\": ").append(d).append(", \"bookings\": {");
            for (int k = 0; k < KEYS_PER_DOCUMENT; k++) {
                sb.append(k == 0 ? "" : ", ").append("\"t").append(d).append('_').append(k).append("\": ");
                leaves(sb, "f", d);
            }
            lines.add(sb.append("}}").toString());
        }
        // Bounded on the schema first: without the column bound this shape outgrows what the harness can write.
        int columns = schemaOf(lines).getNumberOfColumns();
        // The first document's keys, then growingWindows (4) windows of the column bound, give or take one key each,
        // plus id and the map's columns. By names alone the node would wait 128 documents: about 3,000 columns.
        int bound = KEYS_PER_DOCUMENT * LEAVES + 4 * (WINDOW_MAX_NEW_COLUMNS + LEAVES) + 2 * LEAVES;
        System.out.printf("%nWIDE VALUES: %d keys/doc of %d leaves x %d docs -> %d columns (bound %d)%n",
                KEYS_PER_DOCUMENT, LEAVES, DOCUMENTS, columns, bound);
        assertTrue("the node must collapse within a few windows of columns: " + columns, columns <= bound);

        ColumnFlushHarness harness = new ColumnFlushHarness();
        ColumnFlushHarness.Component component = harness.writeComponent(lines);

        ObjectMapper mapper = new ObjectMapper();
        List<IValueReference> out = harness.assembleRecords(component);
        assertEquals("every record must survive", lines.size(), out.size());
        for (int d = 0; d < lines.size(); d++) {
            assertEquals("record " + d, mapper.readTree(lines.get(d)),
                    mapper.readTree(ColumnFlushHarness.json(out.get(d))));
        }
    }

    /**
     * A wide schema whose fields are all objects and all arrive in the first document mints hundreds of columns at
     * once, then none: the first window is fast, and nothing after it is.
     */
    @Test
    public void wideStableNestedSchemaStaysColumnar() throws Exception {
        FlushColumnMetadata md = schemaOf(stableNestedRecords(70, 100));
        assertFalse("a stable schema must stay columnar", anyCollapsed(md.getRoot()));
        assertEquals("every field and leaf keeps its column", 1 + 70 * LEAVES, md.getNumberOfColumns());
    }

    private static List<String> stableNestedRecords(int fields, int documents) {
        List<String> lines = new ArrayList<>();
        for (int d = 0; d < documents; d++) {
            StringBuilder sb = new StringBuilder("{\"id\": ").append(d);
            for (int f = 0; f < fields; f++) {
                sb.append(", \"field").append(f).append("\": ");
                leaves(sb, "f" + f + "_", d);
            }
            lines.add(sb.append('}').toString());
        }
        return lines;
    }

    private static void leaves(StringBuilder sb, String prefix, int d) {
        sb.append('{');
        for (int i = 0; i < LEAVES; i++) {
            sb.append(i == 0 ? "" : ", ").append('"').append(prefix).append(i).append("\": ").append(d + i);
        }
        sb.append('}');
    }

    /** The schema alone, without writing pages: the harness's page zero cannot hold this many columns. */
    static FlushColumnMetadata schemaOf(List<String> lines) throws Exception {
        DummyBufferCache bufferCache = new DummyBufferCache(ColumnFlushHarness.PAGE_SIZE);
        int fileId = bufferCache.createFile();
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        FlushColumnMetadata md = new FlushColumnMetadata(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, null,
                Collections.emptyList(), null, new ColumnValuesWriterFactory(multiPageOpRef), multiPageOpRef,
                CollapseSettings.current());
        md.init(new TestWriteMultiPageOp(bufferCache, fileId));
        ColumnTransformer transformer = new ColumnTransformer(md, md.getRoot(), new BitSet());
        RecordLazyVisitablePointable pointable = new RecordLazyVisitablePointable(true);
        for (IValueReference record : ColumnFlushHarness.parse(lines)) {
            pointable.set(record);
            transformer.transform(pointable);
        }
        return md;
    }

    private static boolean anyCollapsed(AbstractSchemaNode node) {
        if (node instanceof ObjectSchemaNode object) {
            if (object.isCollapsed()) {
                return true;
            }
            for (AbstractSchemaNode child : object.getChildren()) {
                if (anyCollapsed(child)) {
                    return true;
                }
            }
        }
        return false;
    }
}
