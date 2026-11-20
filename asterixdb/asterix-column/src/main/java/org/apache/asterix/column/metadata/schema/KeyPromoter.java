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

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.util.SpaceSavingTopK;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

/**
 * Promotes keys of a collapsed node back to columns of their own when most documents carry them. One per collapsed
 * node, used only by flushes; dropped when a flush is rolled back.
 * <p>
 * A promotion is permanent, so:
 * <ul>
 * <li>a key must beat the density threshold by a statistical margin,</li>
 * <li>a node promotes at most {@link CollapseDetectorSettings#promoteMaxTotal} keys,</li>
 * <li>a node stops promoting once its promoted columns go cold: its keys rotate (a date per day).</li>
 * </ul>
 * Decisions are also made during a batch, not only at its end, so a flush of a single batch still promotes.
 * <p>
 * Promoted keys are the node's children after its map: once a node collapses, only a promotion adds a child.
 */
final class KeyPromoter {
    private static final Logger LOGGER = LogManager.getLogger();
    /** Keys tracked per node. Bounded: counting every key exactly would recreate the explosion. */
    private static final int PROMOTE_CANDIDATES = 64;
    /**
     * Standard deviations a key must be above {@link CollapseDetectorSettings#promoteMinDensity}. Without a margin, a
     * key below the threshold can clear it by chance in a small sample.
     */
    private static final double PROMOTE_CONFIDENCE = 3;
    /** Cold promoted columns that stop promotion. One may just be a dropped field; two show the keys rotate. */
    private static final int COLD_COLUMNS_TO_STOP = 2;

    private final ObjectSchemaNode node;
    private final CollapseDetectorSettings settings;
    private final SpaceSavingTopK keyCounter = new SpaceSavingTopK(PROMOTE_CANDIDATES);
    /** Visits counted in {@link #keyCounter}: this batch, plus earlier batches too small to decide on. */
    private int visitsInBatch;
    /**
     * Keys chosen for promotion that have no column yet. The column is created on the key's next write, which gives it
     * its type (see {@link #takePending}). Survives across flushes; lost on restart, where a dense key is chosen again.
     */
    private Set<ByteBuffer> pending;
    /** Visits of the node since this promoter was built; the clock for {@link #lastSeen}. Never reset. */
    private int visits;
    /** Promoted child index -> {@link #visits} when it last had a value. */
    private final Int2IntMap lastSeen = new Int2IntOpenHashMap();
    /**
     * Set once {@link #COLD_COLUMNS_TO_STOP} promoted columns are cold; the node then promotes nothing more. A restart
     * clears it, but the cold columns are found cold again before the next decision.
     */
    private boolean keysRotate;
    /** The flush that {@link #childrenAtFlushStart} belongs to. */
    private int flushNumber;
    /**
     * Child count when the current flush first visited the node. Keys promoted before then are only in their columns
     * in this flush; keys promoted later also have values in its map. See {@link #getBoundary}.
     */
    private int childrenAtFlushStart;

    KeyPromoter(ObjectSchemaNode node, CollapseDetectorSettings settings) {
        this.node = node;
        this.settings = settings;
    }

    /** Called once per document that contains the collapsed node. */
    void onDocumentVisited(FlushColumnMetadata columnMetadata) {
        if (flushNumber != columnMetadata.getFlushNumber()) {
            flushNumber = columnMetadata.getFlushNumber();
            childrenAtFlushStart = node.getChildren().size();
        }
        visits++;
        if (++visitsInBatch % settings.windowDocumentsLimit == 0) {
            decide();
        }
    }

    /** Called as promoted child {@code childIndex} receives a value. */
    void onPromotedChildSeen(int childIndex) {
        lastSeen.put(childIndex, visits);
    }

    /** Counts a key written into the node's map towards its promotion. */
    void recordKey(IValueReference fieldName, ATypeTag valueTypeTag) {
        // Record a type only for non-null values; a null says nothing about the column's type.
        boolean concrete = valueTypeTag != ATypeTag.NULL && valueTypeTag != ATypeTag.MISSING;
        keyCounter.add(fieldName, concrete ? valueTypeTag : null);
    }

    /** Whether {@code fieldName} is pending promotion; if so, it gets its column from this write and stops pending. */
    boolean takePending(IValueReference fieldName) {
        if (pending == null || !pending
                .remove(ByteBuffer.wrap(fieldName.getByteArray(), fieldName.getStartOffset(), fieldName.getLength()))) {
            return false;
        }
        // Otherwise its counts would pick it again in this batch.
        keyCounter.remove(fieldName);
        return true;
    }

    /**
     * Called when a batch closes: decides, then resets the counts. A batch with fewer than
     * {@link CollapseDetectorSettings#coldVisits} visits is too small to decide on, so its counts carry over.
     */
    void evaluateBatch() {
        if (visitsInBatch < settings.coldVisits) {
            return;
        }
        decide();
        keyCounter.reset();
        visitsInBatch = 0;
    }

    /**
     * Promoted children below this index have all their values in their own columns in flush {@code flushNumber}'s
     * component; the map holds none of them.
     */
    int getBoundary(int flushNumber) {
        return this.flushNumber == flushNumber ? childrenAtFlushStart : node.getChildren().size();
    }

    /** Marks keys dense enough to promote. Uses the counter's lower bound ({@code count - error}) plus the margin. */
    private void decide() {
        // No map yet means no key has been counted.
        if (node.getMapNode() == null || visitsInBatch < settings.coldVisits || keysRotate()) {
            return;
        }
        double expected = settings.promoteMinDensity * visitsInBatch;
        double threshold = expected + PROMOTE_CONFIDENCE * Math.sqrt(expected * (1 - settings.promoteMinDensity));
        int promoted = node.getChildren().size() - 1 - node.getMapChildIndex() + (pending == null ? 0 : pending.size());
        if (promoted >= settings.promoteMaxTotal) {
            return;
        }
        // A key that was only ever null is not offered: there is no type for its column.
        for (byte[] key : keyCounter.keysAtLeast(threshold)) {
            if (promoted >= settings.promoteMaxTotal) {
                break;
            }
            if (pending == null) {
                pending = new HashSet<>();
            }
            if (pending.add(ByteBuffer.wrap(key))) {
                promoted++;
            }
        }
    }

    /**
     * Whether {@link #COLD_COLUMNS_TO_STOP} promoted columns have had no value for
     * {@link CollapseDetectorSettings#coldVisits} visits. A per-day key looks like a regular field on its day; only
     * later days tell them apart.
     */
    private boolean keysRotate() {
        if (keysRotate) {
            return true;
        }
        int cold = 0;
        for (int i = node.getMapChildIndex() + 1; i < node.getChildren().size(); i++) {
            if (visits - lastSeen.getOrDefault(i, 0) >= settings.coldVisits && ++cold == COLD_COLUMNS_TO_STOP) {
                keysRotate = true;
                pending = null;
                LOGGER.info("keys promoted out of a collapsed object node went cold; its keys rotate, so it promotes "
                        + "no more");
                return true;
            }
        }
        return false;
    }
}
