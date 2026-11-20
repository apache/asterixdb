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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.IntFunction;

import org.apache.asterix.column.common.buffer.TestWriteMultiPageOp;
import org.apache.asterix.column.common.row.DummyLSMBTreeTupleReference;
import org.apache.asterix.column.metadata.PromotionBoundaries;
import org.apache.asterix.column.metadata.PromotionBoundaries.Boundary;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnTupleWriter;
import org.apache.asterix.common.config.StorageProperties.Option;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.write.DefaultColumnWriteContext;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

/**
 * Which keys are promoted out of a collapsed node. Every document carries a fresh key, so the root collapses within a
 * few dozen documents and the fields under test, which start after that, can only arrive in its map.
 */
public class PromotionTest {
    private static final int LATE_START = 200;

    @Before
    public void configure() {
        CollapseSettings.install(CollapseSettings.lowered(16, 1));
    }

    @After
    public void restore() {
        CollapseSettings.reset();
    }

    @Test
    public void aDenseKeyIsPromotedAndWarmKeysAreNot() throws Exception {
        Random random = new Random(42);
        FlushColumnMetadata md = flush(null, 0, 2000, d -> {
            StringBuilder sb = new StringBuilder(", \"hot\": ").append(d);
            for (int w = 0; w < 8; w++) {
                // A fifth of the documents: under the 30% threshold, but well over it in the odd short stretch.
                if (random.nextDouble() < 0.2) {
                    sb.append(", \"warm").append(w).append("\": ").append(d);
                }
            }
            return sb.toString();
        }).metadata;
        ObjectSchemaNode root = md.getRoot();
        assertTrue("the fixture must collapse", root.isCollapsed());
        assertTrue("hot is in every document and must be promoted", promoted(md, root, "hot"));
        assertEquals(ATypeTag.BIGINT, root.getChild(index(md, "hot")).getTypeTag());
        for (int w = 0; w < 8; w++) {
            assertFalse("warm" + w + " must stay in the map", promoted(md, root, "warm" + w));
        }
    }

    @Test
    public void aPromotedColumnTakesTheKeysType() throws Exception {
        FlushColumnMetadata md = flush(null, 0, 600, d -> ", \"hot\": \"s" + d + "\"").metadata;
        assertTrue(promoted(md, md.getRoot(), "hot"));
        assertEquals(ATypeTag.STRING, md.getRoot().getChild(index(md, "hot")).getTypeTag());
    }

    @Test
    public void aKeyThatIsOnlyEverNullIsNotPromoted() throws Exception {
        FlushColumnMetadata md = flush(null, 0, 600, d -> ", \"nothing\": null").metadata;
        assertFalse("there is no value type to build a column for", promoted(md, md.getRoot(), "nothing"));
    }

    /** The total cap counts promotions made by earlier flushes, which the schema records only as its children. */
    @Test
    public void promotionsAreCappedInTotal() throws Exception {
        Map<Option, Object> settings = new HashMap<>(CollapseSettings.lowered(16, 1));
        settings.put(Option.STORAGE_COLUMN_COLLAPSE_PROMOTE_MAX_TOTAL, 3);
        CollapseSettings.install(settings);
        IntFunction<String> tenDense = d -> {
            StringBuilder sb = new StringBuilder();
            for (int h = 0; h < 10; h++) {
                sb.append(", \"hot").append(h).append("\": ").append(d);
            }
            return sb.toString();
        };
        Flushed flushed = flush(null, 0, 600, tenDense);
        int first = promotedCount(flushed.metadata, 10);
        assertTrue("the first flush must promote, but within the total: " + first, first >= 1 && first <= 3);
        for (int f = 1; f < 4; f++) {
            flushed = flush(flushed, f * 600, 600, tenDense);
        }
        assertEquals("promotions across flushes must stop at the total", 3, promotedCount(flushed.metadata, 10));
    }

    /**
     * A key a day, flushed daily: each flush sees its day in every document. Two days are promoted before the second
     * goes cold, then none: one cold column may be a field the application dropped, two are a pattern.
     */
    @Test
    public void rotatingKeysStopPromotingOnceTwoGoCold() throws Exception {
        int days = 12;
        IntFunction<String> day = d -> ", \"day" + d / 100 + "\": " + d;
        Flushed flushed = flush(null, 0, LATE_START, day);
        for (int f = 0; f < days; f++) {
            flushed = flush(flushed, LATE_START + f * 100, 100, day);
        }
        int promoted = 0;
        for (int f = 0; f < days; f++) {
            promoted += promoted(flushed.metadata, flushed.metadata.getRoot(), "day" + (LATE_START / 100 + f)) ? 1 : 0;
        }
        assertEquals("promotion must stop once two promoted days go cold", 2, promoted);
    }

    /**
     * The same, through one flush metadata kept across flushes, as an index keeps it between activations. Its visit
     * count then never starts over, so only deciding on a sample of at least 32 visits keeps a day's first decision
     * from coming before the previous day's column has gone cold: with windows of eight visits, a decision on the
     * first window's sample would come at the eighth.
     */
    @Test
    public void rotatingKeysStopPromotingAcrossFlushesOfOneMetadata() throws Exception {
        int days = 12;
        ColumnFlushHarness harness = new ColumnFlushHarness();
        int fileId = harness.createFile();
        FlushColumnMetadata md = harness.freshMetadata(fileId);
        harness.writeComponentWith(md, fileId, records(0, LATE_START, d -> ""));
        for (int f = 0; f < days; f++) {
            harness.writeNextComponent(md, records(LATE_START + f * 100, 100, d -> ", \"day" + d / 100 + "\": " + d));
        }
        int promoted = 0;
        for (int f = 0; f < days; f++) {
            promoted += promoted(md, md.getRoot(), "day" + (LATE_START / 100 + f)) ? 1 : 0;
        }
        assertEquals("promotion must stop once two promoted days go cold", 2, promoted);
    }

    /**
     * A promoted key's counts stay in the batch's counter, but it routes to its column now: chosen again, it would wait
     * for a promotion that never comes, taking a place under the total cap from a key that is still in the map.
     */
    @Test
    public void aPromotedKeyIsNotChosenAgain() throws Exception {
        Map<Option, Object> settings = new HashMap<>(CollapseSettings.lowered(16, 1));
        settings.put(Option.STORAGE_COLUMN_COLLAPSE_PROMOTE_MAX_TOTAL, 2);
        CollapseSettings.install(settings);
        ColumnFlushHarness harness = new ColumnFlushHarness();
        int fileId = harness.createFile();
        FlushColumnMetadata md = harness.freshMetadata(fileId);
        harness.writeComponentWith(md, fileId, records(0, LATE_START, d -> ""));
        harness.writeNextComponent(md,
                records(LATE_START, 600, d -> ", \"hot0\": " + d + (d < LATE_START + 300 ? "" : ", \"hot1\": " + d)));
        assertTrue("hot0 must be promoted", promoted(md, md.getRoot(), "hot0"));
        assertTrue("hot1 must be promoted within the total of two", promoted(md, md.getRoot(), "hot1"));
    }

    /**
     * A key above the density bar can miss a run of a few dozen visits by chance; over the months a node runs, one
     * such run must not be mistaken for a key that rotated away, which would stop the node promoting for good.
     */
    @Test
    public void aDenseKeyMissingARunOfVisitsDoesNotStopPromotion() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        int fileId = harness.createFile();
        FlushColumnMetadata md = harness.freshMetadata(fileId);
        harness.writeComponentWith(md, fileId, records(0, LATE_START, d -> ""));
        harness.writeNextComponent(md, records(LATE_START, 800,
                d -> (d >= 400 && d < 440 ? "" : ", \"hot0\": " + d) + (d < 600 ? "" : ", \"hot1\": " + d)));
        assertTrue("hot0 must be promoted before its gap", promoted(md, md.getRoot(), "hot0"));
        assertTrue("hot1 must be promoted after hot0's gap of 40 visits", promoted(md, md.getRoot(), "hot1"));
    }

    /**
     * A flush that fails is rolled back to the schema the last good one wrote, while the schema nodes keep what they
     * had counted. The flush that writes the records again promotes the key the failed one had, mid-flush again, and
     * its boundary says so; a key promoted by a good flush before it stays in its column alone.
     */
    @Test
    public void aFlushAfterAnAbortedOneRecordsItsOwnBoundary() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        int fileId = harness.createFile();
        FlushColumnMetadata md = harness.freshMetadata(fileId);
        harness.writeComponentWith(md, fileId, records(0, LATE_START, d -> ""));
        harness.writeNextComponent(md, records(LATE_START, 300, d -> ", \"hot\": " + d));
        // What the bulkloader does once a component is written: the schema an abort returns to.
        md.serializeColumnsMetadata();
        md.swapSerializedColumnsMetadata();
        assertTrue("the fixture must promote hot", promoted(md, md.getRoot(), "hot"));

        List<String> failed = records(LATE_START + 300, 300, d -> ", \"hot\": " + d + ", \"hot2\": " + d);
        harness.writeNextComponent(md, failed);
        assertTrue("the failed flush must promote hot2", promoted(md, md.getRoot(), "hot2"));
        md.abort();
        assertTrue("the abort keeps hot, promoted by a good flush", promoted(md, md.getRoot(), "hot"));
        assertFalse("the abort drops hot2's column", promoted(md, md.getRoot(), "hot2"));

        harness.writeNextComponent(md, failed);
        assertTrue("the flush written again promotes hot2", promoted(md, md.getRoot(), "hot2"));
        Int2ObjectMap<Boundary> boundaries = PromotionBoundaries.read(copy(md.serializeColumnsAnnotations()));
        assertTrue("hot was in its column for the whole flush",
                md.getRoot().holdsEveryValueOfItsKey(index(md, "hot"), boundaries));
        assertFalse("hot2 was promoted mid-flush, after writes to the map",
                md.getRoot().holdsEveryValueOfItsKey(index(md, "hot2"), boundaries));
    }

    /**
     * A key chosen as a flush ends, after its last value in it, is promoted by its next write, which a restart puts out
     * of reach: the choice is not part of the schema. The key, still dense, is chosen again, and every record reads
     * back, before and after a merge moves its old values into its column.
     */
    @Test
    public void aChoiceARestartDropsIsMadeAgain() throws Exception {
        // hot is in the first 40 of the second flush's 100 documents: the first decision, at the 80th visit, chooses
        // it with no value left to write.
        IntFunction<String> hot = d -> d < LATE_START + 40 || d >= LATE_START + 100 ? ", \"hot\": " + d : "";
        ColumnFlushHarness harness = new ColumnFlushHarness();
        int fileId = harness.createFile();
        FlushColumnMetadata md = harness.freshMetadata(fileId);
        harness.writeComponentWith(md, fileId, records(0, LATE_START, d -> ""));
        harness.writeNextComponent(md, records(LATE_START, 100, hot));
        assertFalse("the fixture must leave hot chosen but not promoted", promoted(md, md.getRoot(), "hot"));
        assertTrue("the fixture must leave hot chosen", pending(md.getRoot()));

        try (ColumnLsmHarness lsm = new ColumnLsmHarness()) {
            List<String> all = new ArrayList<>();
            for (int[] flush : new int[][] { { 0, LATE_START }, { LATE_START, 100 }, { LATE_START + 100, 300 } }) {
                List<String> batch = records(flush[0], flush[1], hot);
                lsm.insert(batch);
                lsm.flush();
                all.addAll(batch);
                if (flush[0] == LATE_START) {
                    lsm.restart();
                }
            }
            assertTrue("hot must be chosen again after the restart",
                    promoted(lsm.newestSchema(), lsm.newestSchema().getRoot(), "hot"));
            ColumnLsmHarnessTest.assertSameRecords(all, lsm.scan());
            lsm.mergeAll();
            assertTrue("the merge must leave hot in its column alone", PromotionMergeTest.clean(lsm, "hot"));
            ColumnLsmHarnessTest.assertSameRecords(all, lsm.scan());
        }
    }

    /**
     * A flush that collapsed a node and promoted out of it, rolled back: the node is no longer collapsed, and the
     * flush that writes the records again collapses it and promotes the dense key again.
     */
    @Test
    public void aCollapseRolledBackByAnAbortPromotesAgain() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        int fileId = harness.createFile();
        FlushColumnMetadata md = harness.freshMetadata(fileId);
        harness.writeComponentWith(md, fileId, records(0, 5, d -> ""));
        md.serializeColumnsMetadata();
        md.swapSerializedColumnsMetadata();
        assertFalse("the fixture must not collapse before the aborted flush", md.getRoot().isCollapsed());
        List<String> rolledBack = records(5, 600, d -> ", \"hot\": " + d);
        harness.writeNextComponent(md, rolledBack);
        assertTrue("the aborted flush must promote hot", promoted(md, md.getRoot(), "hot"));
        md.abort();
        assertFalse("the abort must undo the collapse", md.getRoot().isCollapsed());
        harness.writeNextComponent(md, rolledBack);
        assertTrue("the flush written again must collapse", md.getRoot().isCollapsed());
        assertTrue("and promote hot again", promoted(md, md.getRoot(), "hot"));
    }

    /**
     * A flush sizes page zero from a pre-pass that copies the schema as the flush starts and never promotes. A key
     * promoted during the flush whose value is an object adds its subtree's columns to the flush alone: a record whose
     * write promotes is estimated before that write, but each record's estimate must cover the columns already there.
     */
    @Test
    public void thePrePassCoversAnObjectPromotedDuringTheFlush() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness(128 * 1024);
        int fileId = harness.createFile();
        FlushColumnMetadata md = harness.freshMetadata(fileId);
        harness.writeComponentWith(md, fileId, records(0, LATE_START, d -> ""));
        assertTrue("the fixture must collapse the root before the flush", md.getRoot().isCollapsed());
        md.init(new TestWriteMultiPageOp(harness.getBufferCache(), harness.createFile()));
        FlushColumnTupleWriter writer = new FlushColumnTupleWriter(md, 128 * 1024, 100_000, 0.15, 8 * 128 * 1024,
                DefaultColumnWriteContext.INSTANCE);
        try {
            DummyLSMBTreeTupleReference tuple = new DummyLSMBTreeTupleReference();
            for (IValueReference record : ColumnFlushHarness.parse(records(LATE_START, 300, d -> {
                StringBuilder obj = new StringBuilder(", \"obj\": {\"f0\": ").append(d);
                for (int f = 1; f < 50; f++) {
                    obj.append(", \"f").append(f).append("\": ").append(d);
                }
                return obj.append('}').toString();
            }))) {
                tuple.set(record);
                int columnsBefore = md.getNumberOfColumns();
                writer.updateColumnMetadataForCurrentTuple(tuple);
                int estimate = writer.getAbsoluteNumberOfColumns(true);
                assertTrue("estimated " + estimate + " columns, " + columnsBefore + " were there before the write",
                        estimate >= columnsBefore);
                writer.writeTuple(tuple);
            }
            assertTrue("the fixture must promote obj", promoted(md, md.getRoot(), "obj"));
        } finally {
            writer.close();
        }
    }

    /** Whether {@code node}'s promoter holds a key chosen for promotion that has no column yet. */
    private static boolean pending(ObjectSchemaNode node) throws Exception {
        Field promoterField = ObjectSchemaNode.class.getDeclaredField("promoter");
        promoterField.setAccessible(true);
        Object promoter = promoterField.get(node);
        if (promoter == null) {
            return false;
        }
        Field pendingField = promoter.getClass().getDeclaredField("pending");
        pendingField.setAccessible(true);
        Set<?> pending = (Set<?>) pendingField.get(promoter);
        return pending != null && !pending.isEmpty();
    }

    /**
     * Flushes smaller than a decision's sample, as each of many storage partitions writes: the counts carry from
     * flush to flush, so a dense key is still promoted, and a key that rotates each flush still is not.
     */
    @Test
    public void aDenseKeyIsPromotedThroughFlushesSmallerThanASample() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        int fileId = harness.createFile();
        FlushColumnMetadata md = harness.freshMetadata(fileId);
        harness.writeComponentWith(md, fileId, records(0, LATE_START, d -> ""));
        int flushSize = 20;
        for (int f = 0; f < 10; f++) {
            harness.writeNextComponent(md, records(LATE_START + f * flushSize, flushSize,
                    d -> ", \"hot\": " + d + ", \"part" + d / flushSize + "\": " + d));
        }
        assertTrue("a key in every document must be promoted", promoted(md, md.getRoot(), "hot"));
        int parts = 0;
        for (int f = 0; f < 10; f++) {
            parts += promoted(md, md.getRoot(), "part" + (LATE_START / flushSize + f)) ? 1 : 0;
        }
        assertTrue("a key per flush must be promoted at most once, " + parts, parts <= 1);
    }

    /**
     * The flush that promotes a key wrote it to the map first, so that component's boundary must not clear the key's
     * column; a later flush writes the key only to its column, so its boundary does. A merge holds both components'
     * rows and takes the lower; an input without boundaries, written before they existed, leaves the merge without.
     */
    @Test
    public void boundariesTellWhereAPromotedKeyMayStillBeInTheMap() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        int fileId = harness.createFile();
        FlushColumnMetadata md = harness.freshMetadata(fileId);
        harness.writeComponentWith(md, fileId, records(0, LATE_START, d -> ""));
        IntFunction<String> hot = d -> ", \"hot\": " + d;
        harness.writeNextComponent(md, records(LATE_START, 300, hot));
        IValueReference promoting = copy(md.serializeColumnsAnnotations());
        assertTrue("the fixture must promote hot", promoted(md, md.getRoot(), "hot"));
        harness.writeNextComponent(md, records(LATE_START + 300, 300, hot));
        IValueReference after = copy(md.serializeColumnsAnnotations());

        int keyColumn = md.getRoot().getMapNode().getKeyColumnIndex();
        int hotChild = md.getRoot().getChildren().indexOf(md.getRoot().getChild(index(md, "hot")));
        assertFalse("the promoting flush may hold hot in its map",
                PromotionBoundaries.read(promoting).get(keyColumn).holds(hotChild));
        assertTrue("a later flush holds hot only in its column",
                PromotionBoundaries.read(after).get(keyColumn).holds(hotChild));
        assertFalse("a merge of the two holds rows of the promoting flush", PromotionBoundaries
                .read(PromotionBoundaries.merge(Arrays.asList(after, promoting))).get(keyColumn).holds(hotChild));
        assertNull("an input without boundaries leaves the merge without",
                PromotionBoundaries.merge(Arrays.asList(after, null)));
    }

    /**
     * Read with the boundaries of the components being read, a promoted key whose column holds all its values there
     * is read from its column alone, its map not even projected; where the column may not hold them all, both still
     * are. Either way every value comes back.
     */
    @Test
    public void aCleanPromotedKeyIsReadFromItsColumnAlone() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        int fileId = harness.createFile();
        FlushColumnMetadata md = harness.freshMetadata(fileId);
        harness.writeComponentWith(md, fileId, records(0, LATE_START, d -> ""));
        IntFunction<String> hot = d -> ", \"hot\": " + d;
        ColumnFlushHarness.Component promoting = harness.writeNextComponent(md, records(LATE_START, 300, hot));
        Int2ObjectMap<Boundary> promotingBoundaries = PromotionBoundaries.read(copy(md.serializeColumnsAnnotations()));
        ColumnFlushHarness.Component after = harness.writeNextComponent(md, records(LATE_START + 300, 300, hot));
        Int2ObjectMap<Boundary> afterBoundaries = PromotionBoundaries.read(copy(md.serializeColumnsAnnotations()));
        ARecordType requested = new ARecordType("ALL", new String[] { "hot" }, new IAType[] { BuiltinType.ANY }, true);

        assertTrue("a clean component's map need not be projected for hot", ColumnFlushHarness.projectedColumns(after,
                requested, afterBoundaries) < ColumnFlushHarness.projectedColumns(after, requested, null));
        assertEquals("the promoting component still reads both",
                ColumnFlushHarness.projectedColumns(promoting, requested, null),
                ColumnFlushHarness.projectedColumns(promoting, requested, promotingBoundaries));
        assertHotValues(harness.assembleProjected(after, requested, afterBoundaries), LATE_START + 300);
        assertHotValues(harness.assembleProjected(promoting, requested, promotingBoundaries), LATE_START);
    }

    /** Each record read back is {@code {"hot": d}}, d counting up from {@code from}. */
    private static void assertHotValues(List<IValueReference> records, int from) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        for (int i = 0; i < records.size(); i++) {
            assertEquals("record " + i, from + i,
                    mapper.readTree(ColumnFlushHarness.json(records.get(i))).get("hot").asInt());
        }
        assertEquals(300, records.size());
    }

    private static IValueReference copy(IValueReference value) throws Exception {
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        storage.append(value);
        return storage;
    }

    /** A promoted key's object value collapses from the start when the map's values had collapsed. */
    @Test
    public void aPromotedObjectTakesTheCollapseOfTheMapsValues() throws Exception {
        FlushColumnMetadata md = flush(null, 0, 600, d -> ", \"hot\": {\"e" + d + "\": " + d + "}").metadata;
        assertTrue(promoted(md, md.getRoot(), "hot"));
        ObjectSchemaNode hot = (ObjectSchemaNode) md.getRoot().getChild(index(md, "hot"));
        assertTrue("hot must be born collapsed", hot.isCollapsed());
        assertEquals("every event ID of hot goes to its map", 1, hot.getChildren().size());
    }

    /**
     * A key chosen for promotion that is not written again stops being counted, so the churn of one-off keys
     * eventually evicts it from the counter, whose slot is rewritten for a key of the same length. The choice must
     * survive that: the key is promoted when it does come back.
     */
    @Test
    public void aChosenKeySurvivesItsEvictionFromTheCounter() throws Exception {
        Flushed collapsed = flush(null, 0, LATE_START, d -> "");
        // The first decision is at the start of the first document past a full sample, on a window boundary of eight.
        int burst = (CollapseSettings.current().coldVisits + 7) / 8 * 8;
        // One batch: closing it resets the counter, which then reuses nothing.
        int last = LATE_START + 999;
        // In every document before that decision, then gone until the last one; meanwhile
        // four one-off keys a document, all five characters like it, drive the counter's minimum past its count.
        FlushColumnMetadata md = flush(collapsed, LATE_START, last - LATE_START + 1, d -> {
            if (d < LATE_START + burst || d == last) {
                return ", \"hotkk\": " + d;
            }
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < 4; j++) {
                sb.append(String.format(", \"q%04d\": %d", 4 * d + j, d));
            }
            return sb.toString();
        }).metadata;
        assertTrue("the chosen key must be promoted when it comes back", promoted(md, md.getRoot(), "hotkk"));
    }

    @Test
    public void nothingIsPromotedWhileCollapseIsDisabled() throws Exception {
        Flushed collapsed = flush(null, 0, LATE_START, d -> "");
        assertTrue(collapsed.metadata.getRoot().isCollapsed());
        CollapseSettings.reset();
        FlushColumnMetadata md = flush(collapsed, LATE_START, 600, d -> ", \"hot\": " + d).metadata;
        assertTrue("a collapsed node stays collapsed", md.getRoot().isCollapsed());
        assertFalse(promoted(md, md.getRoot(), "hot"));
    }

    private record Flushed(ColumnFlushHarness harness, FlushColumnMetadata metadata, IValueReference schema) {
    }

    /**
     * One flush of {@code count} documents from {@code from}, continuing {@code previous}'s schema if given. A
     * document carries its id and a fresh key, then {@code extra(d)} from {@link #LATE_START} on.
     */
    private static Flushed flush(Flushed previous, int from, int count, IntFunction<String> extra) throws Exception {
        List<String> lines = records(from, count, extra);
        ColumnFlushHarness harness = previous == null ? new ColumnFlushHarness() : previous.harness;
        int fileId = harness.createFile();
        FlushColumnMetadata md =
                previous == null ? harness.freshMetadata(fileId) : harness.continueMetadata(fileId, previous.schema);
        ColumnFlushHarness.Component component = harness.writeComponentWith(md, fileId, lines);
        return new Flushed(harness, md, component.serializedMetadata());
    }

    /**
     * {@code count} documents from {@code from}, each with its id and a fresh key, then {@code extra(d)} from
     * {@link #LATE_START} on.
     */
    private static List<String> records(int from, int count, IntFunction<String> extra) {
        List<String> lines = new ArrayList<>();
        for (int d = from; d < from + count; d++) {
            lines.add("{\"id\": " + d + ", \"k" + d + "\": " + d + (d >= LATE_START ? extra.apply(d) : "") + "}");
        }
        return lines;
    }

    private static int promotedCount(FlushColumnMetadata md, int keys) throws Exception {
        int promoted = 0;
        for (int h = 0; h < keys; h++) {
            promoted += promoted(md, md.getRoot(), "hot" + h) ? 1 : 0;
        }
        return promoted;
    }

    static boolean promoted(FlushColumnMetadata md, ObjectSchemaNode node, String name) throws Exception {
        int index = index(md, name);
        return index >= 0 && node.isPromoted(index);
    }

    private static int index(FlushColumnMetadata md, String name) throws Exception {
        return md.getFieldNamesDictionary().getFieldNameIndex(name(name));
    }

    /** A field name in the untagged UTF-8 form the write path uses. */
    static ArrayBackedValueStorage name(String fieldName) throws Exception {
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader())
                .serialize(new AMutableString(fieldName), storage.getDataOutput());
        return storage;
    }
}
