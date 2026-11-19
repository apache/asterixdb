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
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.StorageProperties.Option;
import org.apache.hyracks.data.std.api.IValueReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Documents that each carry far more new names than one detector window allows.
 * <p>
 * The name bound on a window used to be checked only when the next document began, so with two windows required the
 * node collapsed only after a second such document -- having minted every key of both as columns. The first growing
 * document is on its own indistinguishable from a wide but stable schema, which also arrives all at once, so it may mint
 * up to a whole fast streak's names ({@code GROWING_WINDOWS x WINDOW_MAX_NEW_NAMES}) before collapsing; the second is
 * held to one window's bound.
 */
public class WideDocumentCollapseTest {
    private static final int WINDOW_MAX_NEW_NAMES = 32;
    private static final int GROWING_WINDOWS = 2;
    // Under a streak's names (GROWING_WINDOWS x WINDOW_MAX_NEW_NAMES), so the first document alone does not collapse.
    private static final int KEYS_PER_DOCUMENT = 60;
    private static final int DOCUMENTS = 6;

    @Before
    public void configure() {
        CollapseSettings.install(Map.of(Option.STORAGE_COLUMN_COLLAPSE_MIN_CHILDREN, 16,
                // Large enough that the document-count path never fires, so only the name bound is under test.
                Option.STORAGE_COLUMN_COLLAPSE_WINDOW_DOCUMENTS, 1000, Option.STORAGE_COLUMN_COLLAPSE_WINDOW_NEW_NAMES,
                4, Option.STORAGE_COLUMN_COLLAPSE_WINDOW_MAX_NEW_NAMES, WINDOW_MAX_NEW_NAMES,
                Option.STORAGE_COLUMN_COLLAPSE_GROWING_WINDOWS, GROWING_WINDOWS));
    }

    @After
    public void restore() {
        CollapseSettings.reset();
    }

    private static List<String> wideRecords() {
        List<String> lines = new ArrayList<>(DOCUMENTS);
        for (int d = 0; d < DOCUMENTS; d++) {
            StringBuilder sb = new StringBuilder("{\"id\": ").append(d).append(", \"linked_users\": {");
            for (int k = 0; k < KEYS_PER_DOCUMENT; k++) {
                sb.append(k == 0 ? "" : ", ").append("\"u").append(d).append('_').append(k).append("\": ")
                        .append(d * 1000 + k);
            }
            lines.add(sb.append("}}").toString());
        }
        return lines;
    }

    /**
     * The collapse lands on the very last new key of the component's last document, so the map is created but never
     * receives an entry in this component. A column that exists in a component's schema with no values behind it cannot
     * be assembled; this checks an empty map does not have that problem.
     */
    @Test
    public void collapsingOnTheLastKeyOfTheLastDocument() throws Exception {
        List<String> lines = new ArrayList<>();
        // One window's worth each: the first fills window one, the second's last new key closes window two and collapses.
        lines.add(document(0, WINDOW_MAX_NEW_NAMES));
        lines.add(document(1, WINDOW_MAX_NEW_NAMES));
        ColumnFlushHarness harness = new ColumnFlushHarness();
        ColumnFlushHarness.Component component = harness.writeComponent(lines);

        List<IValueReference> out = harness.assembleRecords(component);
        assertEquals("both records must survive a collapse that leaves the map empty", 2, out.size());
        String last = ColumnFlushHarness.render(out.get(1));
        for (int k = 0; k < WINDOW_MAX_NEW_NAMES; k++) {
            assertTrue("record 1 lost key u1_" + k, last.contains("u1_" + k));
        }
    }

    private static String document(int d, int keys) {
        StringBuilder sb = new StringBuilder("{\"id\": ").append(d).append(", \"linked_users\": {");
        for (int k = 0; k < keys; k++) {
            sb.append(k == 0 ? "" : ", ").append("\"u").append(d).append('_').append(k).append("\": ")
                    .append(d * 1000 + k);
        }
        return sb.append("}}").toString();
    }

    /**
     * One document far wider than a fast streak: it collapses once it has minted a streak's worth of names, instead of
     * minting all of them -- which no merge would ever remove -- and still reads back whole.
     */
    @Test
    public void aSingleVeryWideDocumentCollapsesAtTheStreakBound() throws Exception {
        int keys = 5000;
        ColumnFlushHarness harness = new ColumnFlushHarness();
        ColumnFlushHarness.Component component = harness.writeComponent(List.of(document(0, keys)));

        // A streak's names, plus id, the node and the map's columns.
        int bound = GROWING_WINDOWS * WINDOW_MAX_NEW_NAMES + 8;
        assertTrue(component.getColumnCount() + " columns for one document of " + keys + " keys",
                component.getColumnCount() <= bound);
        String json = ColumnFlushHarness.json(harness.assembleRecords(component).get(0));
        for (int k = 0; k < keys; k++) {
            assertTrue("lost key u0_" + k, json.contains("\"u0_" + k + "\""));
        }
    }

    /** A record of just under a streak's worth of fields, in every document, stays columnar. */
    @Test
    public void aStableRecordUnderTheStreakBoundStaysColumnar() throws Exception {
        int fields = GROWING_WINDOWS * WINDOW_MAX_NEW_NAMES - 1;
        List<String> lines = new ArrayList<>();
        for (int d = 0; d < 20; d++) {
            StringBuilder sb = new StringBuilder("{\"id\": ").append(d).append(", \"linked_users\": {");
            for (int k = 0; k < fields; k++) {
                sb.append(k == 0 ? "" : ", ").append("\"f").append(k).append("\": ").append(d + k);
            }
            lines.add(sb.append("}}").toString());
        }
        ColumnFlushHarness.Component component = new ColumnFlushHarness().writeComponent(lines);
        // id and one column per field: no map.
        assertEquals(fields + 1, component.getColumnCount());
    }

    @Test
    public void theSecondWideDocumentCollapsesMidDocument() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        ColumnFlushHarness.Component component = harness.writeComponent(wideRecords());

        // The first document's keys, which no detector can withhold, then at most one window's worth from the second
        // before the node collapses -- plus id, the node and the map's columns. Twice KEYS_PER_DOCUMENT means the
        // second document minted every key too, which is what the old document-boundary check allowed.
        int bound = KEYS_PER_DOCUMENT + WINDOW_MAX_NEW_NAMES + 8;
        System.out.printf("%nWIDE DOCUMENT: %d keys/doc x %d docs -> %d columns (bound %d)%n", KEYS_PER_DOCUMENT,
                DOCUMENTS, component.getColumnCount(), bound);
        assertTrue(
                "the node must collapse inside the first document, not after it: " + component.getColumnCount()
                        + " columns for " + KEYS_PER_DOCUMENT + " keys per document",
                component.getColumnCount() <= bound);

        // Mid-record collapse splits the second record across columns and the map -- every key must still come back.
        List<IValueReference> out = harness.assembleRecords(component);
        assertEquals("every record must survive", DOCUMENTS, out.size());
        for (int d = 0; d < DOCUMENTS; d++) {
            String raw = ColumnFlushHarness.render(out.get(d));
            for (int k = 0; k < KEYS_PER_DOCUMENT; k++) {
                String key = "u" + d + "_" + k;
                assertTrue("record " + d + " lost key " + key, raw.contains(key));
            }
        }
    }
}
