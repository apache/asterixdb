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

import java.util.BitSet;
import java.util.List;

import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Decides whether an object node's field-name set is unbounded, so that the node collapses: names it first sees from
 * then on route to its map instead of each getting a column. One per node, built on a flush's first visit of it while
 * collapsing is enabled, and dropped once the node collapses or a flush is rolled back; a node read by a query or a
 * merge never has one.
 * <p>
 * Three triggers, each for a different shape of growth, each with its own check:
 * <ul>
 * <li><b>Rate</b> ({@link #checkForRateGrowth}): new names in several consecutive windows of documents -- fast, or a
 * trickle that never stops. State in {@link RateTrigger}.</li>
 * <li><b>Sparsity</b> ({@link #checkForSparseGrowth}): most children absent from a batch, most of the present ones new
 * -- growth spread over flushes too small for a window. State in {@link SparsityTrigger}.</li>
 * <li><b>Growing children</b> ({@link #checkForGrowingChildren}): children minted after another started growing,
 * growing in turn -- a key a day, each day keyed by event ID.</li>
 * </ul>
 * The {@code on...} methods are the hooks {@link ObjectSchemaNode} calls; each only updates state and runs the checks.
 * <p>
 * Only the schema is persisted: a detector built after a load or a rollback starts from what the schema records, so
 * whether a node collapses can depend on how its documents were split into flushes, and on restarts.
 */
final class CollapseDetector {
    /**
     * A streak whose names recur must last this many times as long before it collapses the node. Rare optional fields
     * of a bounded record turn up for the first time one window after another, as a key set does, but come back, and
     * run out of new names well within the longer streak; a key set whose names recur too (user IDs) keeps minting
     * and still collapses, later.
     */
    private static final int RECURRING_STREAK_FACTOR = 4;
    /** Names recur once this fraction, as its inverse, of those minted during the streak have come back. */
    private static final int RECURRING_SHARE = 4;

    private final ObjectSchemaNode node;
    private final CollapseDetectorSettings settings;
    private final RateTrigger rate = new RateTrigger();
    private final SparsityTrigger sparsity = new SparsityTrigger();

    /**
     * The node's column count for {@link CollapseDetectorSettings#minColumns}: exact when the detector is built, then
     * grown by the subtrees of the names the node mints. The node's own count cannot serve: it learns of a child's
     * columns only when that child is visited again, which a key seen once never is.
     */
    private int knownColumns;

    // Growth: isGrowing, growing children
    /** Visits of the node, counted only up to the second: names minted from then on are growth. */
    private int visits;
    /** Names minted after the node's first visit; see {@link #isGrowing}. */
    private int namesAfterFirstVisit;
    /**
     * Children from this index on were minted after one of the node's children had started growing; none until one
     * has. Children minted before it that grow later do not count, so map fields that have sat side by side in a
     * record from the start never collapse it.
     */
    private int firstChildAfterGrowth = Integer.MAX_VALUE;
    /**
     * Children counted towards {@link CollapseDetectorSettings#growingChildrenLimit}. Built from the schema, it is one
     * if any child has a collapse at or under it, the only growth the schema records. Which of them grew first, and
     * which sat beside it from the start and would not have counted, it does not record, so they must not all count:
     * whether a record collapses would then depend on when its node last restarted.
     */
    private int growingChildren;

    CollapseDetector(ObjectSchemaNode node, CollapseDetectorSettings settings) {
        this.node = node;
        this.settings = settings;
        List<AbstractSchemaNode> children = node.getChildren();
        knownColumns = node.getNumberOfColumns();
        rate.windowFirstChild = children.size();
        sparsity.batchFirstChild = children.size();
        // A node with children has had its first visit: whatever it mints from now on is growth.
        visits = children.isEmpty() ? 0 : 1;
        for (AbstractSchemaNode child : children) {
            if (child instanceof ObjectSchemaNode object && object.isGrowing()) {
                growingChildren = 1;
                firstChildAfterGrowth = children.size();
                break;
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Hooks
    // ---------------------------------------------------------------------------------------------------------------

    /** Called once per document that contains the node. */
    void onDocumentVisited(FlushColumnMetadata columnMetadata) throws HyracksDataException {
        sparsity.visitsInBatch++;
        if (visits < 2) {
            visits++;
        }
        if (rate.onDocument()) {
            checkForRateGrowth(columnMetadata);
        }
        if (!node.isCollapsed()) {
            checkForSparseGrowth(columnMetadata);
        }
    }

    /** Called as the node's child {@code childIndex} receives a value; {@code existing} if it was not just minted. */
    void onChildPresent(int childIndex, boolean existing) {
        sparsity.onChildPresent(childIndex);
        rate.onChildPresent(childIndex, existing);
    }

    /**
     * Called as the node mints a name. The rate window may close mid-document here -- but only after an earlier
     * document has already shown growth.
     * <p>
     * Within the first growing document there is no telling an unbounded key set from a wide but stable schema: both
     * arrive all at once, and only the next documents show whether new names keep coming. Closing windows inside that
     * first document would split its discovery into several "fast" windows and collapse a stable schema. So the first
     * document mints its names regardless -- an unavoidable cost of needing evidence -- and the bound applies from the
     * second, where it stops that document minting every one of its keys before the node can collapse. If the node
     * collapses here, the rest of the current document's new names route to the map.
     */
    void onNewName(FlushColumnMetadata columnMetadata) throws HyracksDataException {
        sparsity.newNamesInBatch++;
        if (visits > 1) {
            namesAfterFirstVisit++;
        }
        if (rate.onNewName()) {
            checkForRateGrowth(columnMetadata);
        }
    }

    /**
     * Called with the columns created by the subtree of a name the node has just minted. The rate window may close
     * here, as in {@link #onNewName}. Only a new name's subtree counts: columns added deeper under an existing child
     * are that child's growth, and charging them here would collapse a wide parent for an unbounded node nested
     * inside it.
     */
    void onNewChildColumns(int columns, FlushColumnMetadata columnMetadata) throws HyracksDataException {
        knownColumns += columns;
        if (rate.onNewColumns(columns)) {
            checkForRateGrowth(columnMetadata);
        }
    }

    /** Called when the node's child {@code childIndex} has just started growing ({@link ObjectSchemaNode#isGrowing}). */
    void onChildGrowing(int childIndex, FlushColumnMetadata columnMetadata) {
        checkForGrowingChildren(childIndex, columnMetadata);
    }

    /** Called when a batch closes: the sparsity figures describe one batch at a time. */
    void resetBatchStatistics() {
        sparsity.reset(node.getChildren().size());
    }

    /** See {@link ObjectSchemaNode#isGrowing}. */
    boolean isGrowing() {
        return namesAfterFirstVisit >= settings.windowNewNamesLimit || growingChildren > 0;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Triggers
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Rate: closes the current window, and collapses the node when its field-name set has kept growing across several
     * consecutive windows.
     * <p>
     * A raw child count is deliberately not the trigger. A schema with thousands of genuinely distinct fields that all
     * recur in every document is wide but bounded, and must stay columnar; it stops producing new names after the
     * first window, so neither streak survives. A node keyed by UUIDs or opaque IDs keeps producing new names
     * indefinitely and trips the fast streak within two windows.
     * <p>
     * The slow streak exists because the rate is not the signal — persistence is. A node gaining one key a day is
     * just as unbounded as one gaining a key per document; it simply takes longer to prove. Requiring several
     * consecutive windows that each produced <em>something</em> new separates that from ordinary schema evolution,
     * where a field is added once and then nothing new arrives for a long time.
     */
    private void checkForRateGrowth(FlushColumnMetadata columnMetadata) {
        int children = node.getChildren().size();
        if (rate.closeWindow(children) && (children >= settings.minChildren || knownColumns >= settings.minColumns)) {
            node.markCollapsed(columnMetadata, "rate");
        }
    }

    /**
     * Sparsity: collapses when most of the node's children have received no value so far in the batch and most of
     * those that have were new in it -- unless its old names come back.
     * <p>
     * Evaluated as documents arrive rather than when the batch closes. A decision taken at the close of a flush's last
     * batch would be lost -- the pending collapse is not serialized, and no key would arrive to build the map before
     * the schema was written -- so a flush made of one batch, the small-flush case this trigger exists for, would never
     * act on it. And a batch can span many of a slow key set's keys -- a key a day, in a batch of a hundred days --
     * which only a few documents into the batch are mostly absent. Deciding mid-batch leaves the rest of the batch to
     * route its new names to the map.
     * <p>
     * Those few documents also show a bounded record of many rare optional fields mostly absent, its few present ones
     * new while the fields are still turning up for the first time. What sets it apart is that its old fields come and
     * go: present in this batch after being absent from the last. A key set's old keys do not -- a day's key is present
     * all through its day and never after it -- so a share of returning names holds the trigger off.
     */
    private void checkForSparseGrowth(FlushColumnMetadata columnMetadata) {
        int children = node.getChildren().size();
        if (sparsity.visitsInBatch >= settings.sparsityMinVisits && children >= settings.minChildren
                && sparsity.isSparse(children)) {
            node.markCollapsed(columnMetadata, "sparsity");
        }
    }

    /**
     * Growing children: a node keyed by day, whose days are keyed by event ID, mints a name a day -- too slowly for
     * either rate streak -- while each day grows and leaves its columns behind. One growing child says nothing about
     * the node, which may be a record holding a map field; children minted after another had started growing, and
     * growing in turn, show the node's keys repeating the blow-up once there are enough of them.
     */
    private void checkForGrowingChildren(int childIndex, FlushColumnMetadata columnMetadata) {
        if (firstChildAfterGrowth == Integer.MAX_VALUE) {
            firstChildAfterGrowth = node.getChildren().size();
        } else if (childIndex < firstChildAfterGrowth) {
            return;
        }
        if (++growingChildren >= settings.growingChildrenLimit) {
            node.markCollapsed(columnMetadata, "growing children");
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Trigger state
    // ---------------------------------------------------------------------------------------------------------------

    /** The rate trigger's windows of documents and its streaks of windows with new names. */
    private final class RateTrigger {
        private int windowDocuments;
        private int windowNewNames;
        private int windowNewColumns;
        /** The children the node had when the current window opened. */
        private int windowFirstChild;
        private int consecutiveFastWindows;
        private int consecutiveGrowingWindows;
        /** Children from this index on were minted during the current streak; none without one. */
        private int streakFirstChild = Integer.MAX_VALUE;
        /**
         * Children minted during the streak, before the current window, that have had a value again since; null until
         * one has. See {@link #namesRecur}.
         */
        private BitSet recurredInStreak;

        /** @return whether the window is full and must close */
        boolean onDocument() {
            return ++windowDocuments >= settings.windowDocumentsLimit || windowNewNames >= settings.windowMaxNewNames
                    || windowNewColumns >= settings.windowMaxNewColumns;
        }

        /** @return whether the window must close mid-document */
        boolean onNewName() {
            return ++windowNewNames >= settings.windowMaxNewNames && consecutiveFastWindows > 0;
        }

        /** @return whether the window must close mid-document */
        boolean onNewColumns(int columns) {
            windowNewColumns += columns;
            return windowNewColumns >= settings.windowMaxNewColumns && consecutiveFastWindows > 0;
        }

        void onChildPresent(int childIndex, boolean existing) {
            if (existing && childIndex >= streakFirstChild && childIndex < windowFirstChild) {
                if (recurredInStreak == null) {
                    recurredInStreak = new BitSet();
                }
                recurredInStreak.set(childIndex);
            }
        }

        /** Closes the window; @return whether a streak is long enough to collapse the node */
        boolean closeWindow(int children) {
            boolean fast =
                    windowNewNames >= settings.windowNewNamesLimit || windowNewColumns >= settings.windowMaxNewColumns;
            if (windowNewNames > 0 && consecutiveGrowingWindows == 0) {
                streakFirstChild = windowFirstChild;
            }
            consecutiveFastWindows = fast ? consecutiveFastWindows + 1 : 0;
            consecutiveGrowingWindows = windowNewNames > 0 ? consecutiveGrowingWindows + 1 : 0;
            int proof = namesRecur() ? RECURRING_STREAK_FACTOR : 1;
            if (consecutiveGrowingWindows == 0) {
                endStreak();
            }
            windowDocuments = 0;
            windowNewNames = 0;
            windowNewColumns = 0;
            windowFirstChild = children;
            return consecutiveFastWindows >= settings.growingWindowsLimit * proof
                    || consecutiveGrowingWindows >= settings.slowWindowsLimit * proof;
        }

        /** Whether a share of the names minted during the streak, before the current window, have come back since. */
        private boolean namesRecur() {
            int minted = windowFirstChild - streakFirstChild;
            return minted > 0 && recurredInStreak != null && recurredInStreak.cardinality() * RECURRING_SHARE >= minted;
        }

        private void endStreak() {
            streakFirstChild = Integer.MAX_VALUE;
            if (recurredInStreak != null) {
                recurredInStreak.clear();
            }
        }
    }

    /** The sparsity trigger's view of the current batch, and of which children the previous one held. */
    private final class SparsityTrigger {
        /** Children that received a value in the current batch, by child index; reset when the batch closes. */
        private BitSet presentInBatch = new BitSet();
        private int presentInBatchCount;
        /** Children that received a value in the previous batch; null before a batch has closed. */
        private BitSet presentInPreviousBatch;
        /** The children the node had when the current batch opened: those below it are old in the batch. */
        private int batchFirstChild;
        /**
         * Old children's first values in each batch, and of them, those absent from the batch before. Summed over the
         * detector's life, not reset with the batch: the trigger decides a few documents into a batch, when one batch's
         * count would rest on a handful of old names, and a record of rare fields that has shown its names returning
         * stays held off.
         */
        private long oldPresent;
        private long returned;
        private int newNamesInBatch;
        private int visitsInBatch;

        void onChildPresent(int childIndex) {
            if (presentInBatch.get(childIndex)) {
                return;
            }
            presentInBatch.set(childIndex);
            presentInBatchCount++;
            if (childIndex < batchFirstChild) {
                oldPresent++;
                if (presentInPreviousBatch != null && !presentInPreviousBatch.get(childIndex)) {
                    returned++;
                }
            }
        }

        /** Most children absent, most present ones new, and the old ones not coming back. */
        boolean isSparse(int children) {
            double absent = 1.0 - (double) presentInBatchCount / children;
            double fresh = presentInBatchCount == 0 ? 0 : (double) newNamesInBatch / presentInBatchCount;
            boolean namesReturn = oldPresent > 0 && returned * RECURRING_SHARE >= oldPresent;
            return absent >= settings.sparsityMinAbsent && fresh >= settings.sparsityMinFresh && !namesReturn;
        }

        void reset(int children) {
            BitSet previous = presentInPreviousBatch == null ? new BitSet() : presentInPreviousBatch;
            presentInPreviousBatch = presentInBatch;
            presentInBatch = previous;
            presentInBatch.clear();
            presentInBatchCount = 0;
            newNamesInBatch = 0;
            visitsInBatch = 0;
            batchFirstChild = children;
        }
    }
}
