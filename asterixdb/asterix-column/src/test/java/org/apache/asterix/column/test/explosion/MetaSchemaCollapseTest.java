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

import static org.apache.asterix.column.test.explosion.ColumnLsmHarnessTest.assertSameRecords;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.metadata.PromotionBoundaries;
import org.apache.asterix.column.metadata.PromotionBoundaries.Boundary;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

/**
 * The meta record is shredded by the same schema code as the record, under a root of its own: an object in it whose
 * keys keep arriving -- extended attributes, say -- collapses, a dense key of it is promoted, and a merge moves that
 * key's earlier values out of the map, as they would in the record.
 */
public class MetaSchemaCollapseTest {
    private static final ARecordType META_TYPE = new ARecordType("MetaType", new String[0], new IAType[0], true);
    private static final int HOT_FROM = 200;

    @Before
    public void configure() {
        CollapseSettings.install(CollapseSettings.lowered(16, 1));
    }

    @After
    public void restore() {
        CollapseSettings.reset();
    }

    @Test
    public void anUnboundedMetaObjectCollapsesPromotesAndMerges() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness(32 * 1024, META_TYPE)) {
            List<String> expected = new ArrayList<>();
            int from = 0;
            for (int size : new int[] { HOT_FROM, 300, 300 }) {
                List<String> records = new ArrayList<>();
                List<String> metas = new ArrayList<>();
                for (int d = from; d < from + size; d++) {
                    String meta = "{\"cas\": " + d + ", \"xattrs\": {\"k" + d + "\": " + d
                            + (d < HOT_FROM ? "" : ", \"hot\": " + d) + "}}";
                    records.add("{\"id\": " + d + ", \"a\": " + d + "}");
                    metas.add(meta);
                    expected.add(
                            "{\"id\": " + d + ", \"a\": " + d + ", \"" + ColumnLsmHarness.META + "\": " + meta + "}");
                }
                lsm.insert(records, metas);
                lsm.flush();
                from += size;
            }

            FlushColumnMetadata schema = lsm.newestSchema();
            assertFalse("the record must not collapse", schema.getRoot().isCollapsed());
            ObjectSchemaNode xattrs = xattrs(schema);
            assertTrue("xattrs, keyed anew in every meta record, must collapse", xattrs.isCollapsed());
            assertTrue("hot, in every meta record from " + HOT_FROM + ", must be promoted",
                    PromotionTest.promoted(schema, xattrs, "hot"));
            assertSameRecords(expected, lsm.scan());

            lsm.mergeAll();
            schema = lsm.newestSchema();
            int hot = schema.getFieldNamesDictionary().getFieldNameIndex(PromotionTest.name("hot"));
            Int2ObjectMap<Boundary> boundaries = PromotionBoundaries.read(lsm.annotations().get(0));
            // An entry, not its absence: a component without one for a node reads as never having had its map.
            assertTrue("the merged component must record a boundary for xattrs",
                    boundaries.containsKey(xattrs(schema).getMapNode().getKeyColumnIndex()));
            assertTrue("the merge must leave hot in its column alone",
                    xattrs(schema).holdsEveryValueOfItsKey(hot, boundaries));
            assertSameRecords(expected, lsm.scan());
        }
    }

    private static ObjectSchemaNode xattrs(FlushColumnMetadata schema) throws Exception {
        int index = schema.getFieldNamesDictionary().getFieldNameIndex(PromotionTest.name("xattrs"));
        return (ObjectSchemaNode) schema.getMetaRoot().getChild(index);
    }
}
