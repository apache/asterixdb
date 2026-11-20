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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntFunction;

import org.apache.asterix.column.assembler.value.ValueGetterFactory;
import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.metadata.PromotionBoundaries;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.common.exceptions.NoOpWarningCollector;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/** The LSM harness itself: what goes in through flushes and merges comes back out, whole and projected. */
public class ColumnLsmHarnessTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Before
    public void configure() {
        CollapseSettings.install(CollapseSettings.lowered(16, 1));
    }

    @After
    public void restore() {
        CollapseSettings.reset();
    }

    @Test
    public void recordsSurviveFlushesAndAMerge() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all = new ArrayList<>();
            for (int f = 0; f < 3; f++) {
                List<String> batch = records(f * 100, 100, d -> ", \"name\": \"n" + d + "\"");
                lsm.insert(batch);
                lsm.flush();
                all.addAll(batch);
            }
            assertEquals(3, lsm.diskComponents().size());
            assertSameRecords(all, lsm.scan());
            lsm.mergeAll();
            assertEquals(1, lsm.diskComponents().size());
            assertSameRecords(all, lsm.scan());
        }
    }

    /**
     * The promotion lifecycle through real flushes and a merge: the root collapses, "hot" arrives in its map and is
     * promoted, and every record still reads back, whole and with "hot" projected; every component carries
     * boundaries.
     */
    @Test
    public void aPromotedKeyReadsBackAcrossFlushesAndAMerge() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all = new ArrayList<>();
            IntFunction<String> hot = d -> ", \"hot\": " + d;
            int[][] flushes = { { 0, 200 }, { 200, 300 }, { 500, 300 } };
            for (int[] flush : flushes) {
                List<String> batch = records(flush[0], flush[1], flush[0] == 0 ? d -> "" : hot);
                lsm.insert(batch);
                lsm.flush();
                all.addAll(batch);
            }
            FlushColumnMetadata schema = lsm.newestSchema();
            assertTrue("the root must collapse", schema.getRoot().isCollapsed());
            assertTrue("hot must be promoted", PromotionTest.promoted(schema, schema.getRoot(), "hot"));
            for (IValueReference annotations : lsm.annotations()) {
                assertNotNull("every flushed component carries boundaries", annotations);
            }
            ARecordType requested = new ARecordType("ALL", new String[] { "id", "hot" },
                    new IAType[] { BuiltinType.ANY, BuiltinType.ANY }, true);
            assertSameRecords(all, lsm.scan());
            assertSameProjection(all, lsm.scan(requested), "hot");
            lsm.mergeAll();
            assertNotNull("a merge of components with boundaries has boundaries", lsm.annotations().get(0));
            assertSameRecords(all, lsm.scan());
            assertSameProjection(all, lsm.scan(requested), "hot");
        }
    }

    /**
     * Each component decides for itself whether a promoted key is in its column alone: one that is reads the column
     * without the map, and can filter on the key, whatever the other components hold.
     */
    @Test
    public void eachComponentReadsAPromotedKeyByWhatItHolds() throws Exception {
        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all = new ArrayList<>();
            List<String> beforeCollapse = new ArrayList<>();
            for (int d = -50; d < 0; d++) {
                beforeCollapse.add("{\"id\": " + d + ", \"name\": \"n" + d + "\"}");
            }
            IntFunction<String> hot = d -> ", \"hot\": " + d;
            // Oldest first: before the root collapses; collapsing, without hot; hot arrives and is promoted partway;
            // hot promoted from the first document on.
            for (List<String> batch : List.of(beforeCollapse, records(0, 200, d -> ""), records(200, 300, hot),
                    records(500, 300, hot))) {
                lsm.insert(batch);
                lsm.flush();
                all.addAll(batch);
            }
            FlushColumnMetadata schema = lsm.newestSchema();
            assertTrue("hot must be promoted", PromotionTest.promoted(schema, schema.getRoot(), "hot"));
            ARecordType requested = new ARecordType("ALL", new String[] { "id", "hot" },
                    new IAType[] { BuiltinType.ANY, BuiltinType.ANY }, true);
            // Newest first.
            List<IValueReference> annotations = lsm.annotations();
            int promotedThroughout = columnsRead(lsm, requested, () -> PromotionBoundaries.read(annotations.get(0)));
            int promotedPartway = columnsRead(lsm, requested, () -> PromotionBoundaries.read(annotations.get(1)));
            assertTrue("a component that may hold hot in its map reads the map as well",
                    promotedPartway > promotedThroughout);
            assertEquals("written before hot's promotion, it may hold hot in its map", promotedPartway,
                    columnsRead(lsm, requested, () -> PromotionBoundaries.read(annotations.get(2))));
            assertEquals("written before the root had a map, it holds nothing in one", promotedThroughout,
                    columnsRead(lsm, requested, () -> PromotionBoundaries.read(annotations.get(3))));
            assertEquals("deciding over every component, one holding hot in its map makes all read it", promotedPartway,
                    columnsRead(lsm, requested,
                            () -> PromotionBoundaries.ofComponents(new ArrayList<>(lsm.diskComponents()))));
            assertSameRecords(all, lsm.scan());
            assertSameProjection(all, lsm.scan(requested), "hot");
        }
    }

    /** The columns a search of {@code requested} reads in a component whose boundaries {@code boundaries} gives. */
    private static int columnsRead(ColumnLsmHarness lsm, ARecordType requested, PromotionBoundaries.Source boundaries)
            throws Exception {
        return QueryColumnMetadata
                .create(ColumnFlushHarness.KEYED_TYPE, 1,
                        ColumnUtil.getColumnMetadataCopy(lsm.diskComponents().get(0).getMetadata()),
                        new ColumnValueReaderFactory(), ValueGetterFactory.INSTANCE, requested, Collections.emptyMap(),
                        NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                        NoOpWarningCollector.INSTANCE, null, ColumnProjectorType.QUERY, boundaries)
                .getNumberOfColumns();
    }

    /** Document {@code d}: its id, a fresh key, and {@code extra(d)}. */
    static List<String> records(int from, int count, IntFunction<String> extra) {
        List<String> lines = new ArrayList<>();
        for (int d = from; d < from + count; d++) {
            lines.add("{\"id\": " + d + ", \"k" + d + "\": " + d + extra.apply(d) + "}");
        }
        return lines;
    }

    static void assertSameRecords(List<String> expected, List<String> actual) throws Exception {
        assertEquals("record count", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("record " + i, MAPPER.readTree(expected.get(i)), MAPPER.readTree(actual.get(i)));
        }
    }

    static void assertSameProjection(List<String> expected, List<String> actual, String field) throws Exception {
        assertEquals("record count", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("record " + i + " " + field, MAPPER.readTree(expected.get(i)).get(field),
                    MAPPER.readTree(actual.get(i)).get(field));
        }
    }
}
