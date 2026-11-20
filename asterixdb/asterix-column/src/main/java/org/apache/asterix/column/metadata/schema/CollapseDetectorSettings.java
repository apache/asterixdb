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
package org.apache.asterix.column.metadata.schema;

import java.util.function.Function;

import org.apache.asterix.common.config.StorageProperties.Option;
import org.apache.hyracks.api.config.IOption;

/**
 * The collapse detector's thresholds, from the {@code storage.column.collapse.*} options of the node an index lives on.
 * Read when the index's column manager is created, and handed to every flush of it through its
 * {@link org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata}.
 */
public final class CollapseDetectorSettings {
    /**
     * Whether flushes may collapse a node or promote a key out of one. On by default. A collapsed schema can't be read
     * by older binaries, so collapsing blocks downgrade. Turning this off stops new collapses and promotions; nodes
     * already collapsed stay collapsed.
     */
    public final boolean enabled;
    /**
     * Minimum children before a node is even considered for collapsing. A backstop, not the detector: a wide but
     * stable schema (many genuinely distinct business fields, all recurring) must stay columnar.
     */
    public final int minChildren;
    /**
     * Columns under a node that also admit it for collapsing, whatever its child count. A name whose value is an
     * object brings a column per leaf, so a node keyed by timestamps with 100-leaf values holds thousands of columns
     * long before it has {@link #minChildren} children.
     */
    public final int minColumns;
    /**
     * Documents per detector window. This also sets the floor on how many columns a node keeps after collapsing:
     * the detector cannot decide before it has watched {@code growingWindows * windowDocuments} documents, and
     * every name seen while watching has already been given a column.
     */
    public final int windowDocumentsLimit;
    /** New field names within a window that make it count as <em>fast</em> growth. */
    public final int windowNewNamesLimit;
    /**
     * Consecutive windows that each produced at least one new name, above which the node collapses even though no
     * single window looked fast. Catches key sets that grow by a trickle -- dates or counters accumulating over
     * time -- which are unbounded over the life of the index but barely move inside any one component.
     */
    public final int slowWindowsLimit;
    /**
     * A window also closes once this many new names have arrived, without waiting for the document count. Once an
     * earlier document has shown growth the bound is checked as each name is minted, so a later document carrying
     * thousands of new keys cannot mint them all as columns before the node collapses. The first growing document
     * still mints its names: until a later one arrives, nothing distinguishes it from a wide but stable schema.
     */
    public final int windowMaxNewNames;
    /**
     * The same bound counted in columns: a window also closes, as a fast one, once the subtrees of the names it
     * minted have created this many columns. A name whose value is an object costs a column per leaf, so a node
     * keyed by timestamps whose values are 100-leaf objects mints too few names per window to look fast, yet
     * creates tens of thousands of columns before the name count alone would collapse it.
     */
    public final int windowMaxNewColumns;
    /**
     * Consecutive fast-growing windows required before collapsing. Four, not two: a bounded schema whose record
     * types arrive interleaved -- a few new fields per document until every type has been seen -- produces fast
     * windows during that warm-up, and two was short enough for a 140-field schema of that kind to collapse. Each
     * extra window costs a genuine explosion one window's worth of extra columns before it collapses.
     */
    public final int growingWindowsLimit;
    /**
     * Children that must have started growing ({@link ObjectSchemaNode#isGrowing}) before their parent collapses
     * for them. Eight, not two: a record that gains a few object fields over time, each filling in its optional
     * fields, shows a growing child per field it gains; only a key set keeps producing them.
     */
    public final int growingChildrenLimit;
    /**
     * Fraction of a node's children that must have received no value in a batch for the sparsity trigger. The
     * rate trigger above measures growth inside a short window, held in counters every flush resets, so growth
     * spread across small flushes, or slower than one name per window -- a key a day -- never completes it. A
     * node whose children are mostly absent from every batch is unbounded whatever the rate, and the children it
     * counts are persisted in the schema, so this needs no state that a flush loses.
     */
    public final double sparsityMinAbsent;
    /**
     * Fraction of the children present in a batch that must be new in that batch. Absence alone also fits a
     * bounded schema of many record types -- a small batch sees only a few of them -- but such a schema's present
     * fields are old ones, while an unbounded key set's are mostly new.
     */
    public final double sparsityMinFresh;
    /** Visits of the node a batch must contain before its absence figure means anything. */
    public final int sparsityMinVisits;

    /** Share of a collapsed node's visits a key must appear in to be promoted back to its own column. */
    public final double promoteMinDensity;
    /** Most keys one node may ever promote. */
    public final int promoteMaxTotal;
    /**
     * Two uses:
     * <ul>
     * <li>A promoted column with no value for this many visits in a row is cold. Chosen so a key at
     * {@link #promoteMinDensity} misses that many in a row only about once in 10^12 visits.</li>
     * <li>The minimum sample before deciding a promotion. This gives yesterday's date column time to look cold before
     * today's date key can be promoted. Counted across batches, so small flushes still reach it.</li>
     * </ul>
     */
    public final int coldVisits;
    /** Lower bound for {@link #coldVisits}: smaller samples are too noisy for a permanent decision. */
    private static final int MIN_COLD_VISITS = 32;

    /** Every option at its default: for schemas written outside an index -- tests, tools. */
    public static final CollapseDetectorSettings DEFAULT = new CollapseDetectorSettings(IOption::defaultValue);

    /** @param config the value of each {@code STORAGE_COLUMN_COLLAPSE_*} option */
    public CollapseDetectorSettings(Function<IOption, Object> config) {
        enabled = (Boolean) config.apply(Option.STORAGE_COLUMN_COLLAPSE_ENABLED);
        minChildren = (Integer) config.apply(Option.STORAGE_COLUMN_COLLAPSE_MIN_CHILDREN);
        minColumns = (Integer) config.apply(Option.STORAGE_COLUMN_COLLAPSE_MIN_COLUMNS);
        windowDocumentsLimit = (Integer) config.apply(Option.STORAGE_COLUMN_COLLAPSE_WINDOW_DOCUMENTS);
        windowNewNamesLimit = (Integer) config.apply(Option.STORAGE_COLUMN_COLLAPSE_WINDOW_NEW_NAMES);
        slowWindowsLimit = (Integer) config.apply(Option.STORAGE_COLUMN_COLLAPSE_SLOW_WINDOWS);
        windowMaxNewNames = (Integer) config.apply(Option.STORAGE_COLUMN_COLLAPSE_WINDOW_MAX_NEW_NAMES);
        windowMaxNewColumns = (Integer) config.apply(Option.STORAGE_COLUMN_COLLAPSE_WINDOW_MAX_NEW_COLUMNS);
        growingWindowsLimit = (Integer) config.apply(Option.STORAGE_COLUMN_COLLAPSE_GROWING_WINDOWS);
        growingChildrenLimit = (Integer) config.apply(Option.STORAGE_COLUMN_COLLAPSE_GROWING_CHILDREN);
        sparsityMinAbsent = (Double) config.apply(Option.STORAGE_COLUMN_COLLAPSE_SPARSITY_MIN_ABSENT);
        sparsityMinFresh = (Double) config.apply(Option.STORAGE_COLUMN_COLLAPSE_SPARSITY_MIN_FRESH);
        sparsityMinVisits = (Integer) config.apply(Option.STORAGE_COLUMN_COLLAPSE_SPARSITY_MIN_VISITS);
        promoteMinDensity = (Double) config.apply(Option.STORAGE_COLUMN_COLLAPSE_PROMOTE_MIN_DENSITY);
        promoteMaxTotal = (Integer) config.apply(Option.STORAGE_COLUMN_COLLAPSE_PROMOTE_MAX_TOTAL);
        coldVisits = Math.max(MIN_COLD_VISITS, (int) Math.ceil(Math.log(1e-12) / Math.log(1 - promoteMinDensity)));
    }
}
