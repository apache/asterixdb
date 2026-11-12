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
package org.apache.asterix.column.assembler;

import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * Reassembles a {@link org.apache.asterix.column.metadata.schema.collection.MapSchemaNode} back into ordinary record
 * fields.
 * <p>
 * A map holds two child assemblers: the key column and the shared value node. Per entry they fire in column order —
 * key first, then value — so the key is held and paired with the value that follows it.
 * <p>
 * The reconstructed fields are spliced <em>into the parent record</em> rather than nested under a field of their own,
 * because collapsing is a storage decision and must be invisible to the reader: a document written as
 * {@code {id, uuid1, uuid2}} has to read back with that same shape. The parent's per-record field accounting still
 * sees the map as a single child, so the entry count may vary freely from record to record.
 */
public class MapValueAssembler extends AbstractNestedValueAssembler {
    private final int firstValueIndex;
    private AbstractValueAssembler keyAssembler;
    /**
     * The key awaiting its value, or null. Held by reference: the key assembler runs once per entry, ahead of the
     * value, so its buffer is not rewritten before the pair is emitted.
     */
    private IValueReference pendingKey;
    private boolean unionValue;
    /** Child assemblers that report straight to this map once per round, see {@link #setReporters}. */
    private int reporters = 1;
    private int roundReports;
    private boolean entryInRound;
    private boolean nullAncestor;
    private int nullClampedLevel;
    private int missingClampedLevel;
    /** Null means "emit every entry"; otherwise only these keys are reconstructed. */
    private List<IValueReference> requestedKeys;

    MapValueAssembler(int level, AssemblerInfo info, int firstValueIndex) {
        super(level, info);
        this.firstValueIndex = firstValueIndex;
    }

    final int getFirstValueIndex() {
        return firstValueIndex;
    }

    /** Identifies which child assembler carries the field names; everything else is a value. */
    void setKeyAssembler(AbstractValueAssembler keyAssembler) {
        this.keyAssembler = keyAssembler;
    }

    /**
     * Restricts reconstruction to the names a projection asked for. Without this a query for one field rebuilds every
     * entry in the object — for a node with thousands of keys that is thousands of fields built and discarded per
     * document.
     */
    void setRequestedKeys(List<IValueReference> requestedKeys) {
        this.requestedKeys = requestedKeys;
    }

    /**
     * A union value is spread over one column per arm, and every arm the entry's value is not in reports MISSING for
     * that entry. Those reports must not retire the pending key, or the arm that does hold the value finds no key and
     * is dropped. Only the next key (or the next record) may replace it.
     */
    void setUnionValue(boolean unionValue) {
        this.unionValue = unionValue;
    }

    /**
     * The number of child assemblers that report straight to this map: the key plus one per value arm. Object and
     * array-of-union arms already fold their own readers, so each counts once.
     * <p>
     * Every one of them reports exactly once per round -- a value, a null or an entry-level missing for an entry, or
     * an ancestor-level null or missing when the map itself (or something above it) is absent. The enclosing object
     * counts the map as a single slot, so an ancestor event must reach it once, not once per reporter: an array item
     * would otherwise turn every extra report into a phantom trailing item. Rounds are counted rather than ancestor
     * reports because a reader whose column is absent from the current leaf reports an ancestor-level missing on
     * every round, entry rounds included; counting only those would fill partway through a record.
     */
    void setReporters(int reporters) {
        this.reporters = reporters;
    }

    @Override
    void reset() {
        storage.reset();
        pendingKey = null;
    }

    @Override
    void addNullToAncestor(int nullLevel) throws HyracksDataException {
        nullAncestor = true;
        nullClampedLevel = Math.max(nullClampedLevel, nullLevel);
        onReport(false);
    }

    @Override
    void addMissingToAncestor(int missingLevel) throws HyracksDataException {
        missingClampedLevel = Math.max(missingClampedLevel, missingLevel);
        onReport(false);
    }

    /**
     * Ends the round once every reporter has reported. Only a round in which no reporter saw an entry is an ancestor
     * event, and it is forwarded once; in an entry round the ancestor-level reports came from columns absent from this
     * leaf and say nothing about the map.
     */
    private void onReport(boolean entry) throws HyracksDataException {
        entryInRound |= entry;
        if (++roundReports < reporters) {
            return;
        }
        boolean forward = !entryInRound;
        // Null wins, as in ObjectValueAssembler: one reader seeing a null ancestor means the ancestor is null.
        boolean isNull = nullAncestor;
        int clampedLevel = isNull ? nullClampedLevel : missingClampedLevel;
        clearRound();
        if (!forward) {
            return;
        }
        if (isNull) {
            super.addNullToAncestor(clampedLevel);
        } else {
            super.addMissingToAncestor(clampedLevel);
        }
    }

    private void clearRound() {
        roundReports = 0;
        entryInRound = false;
        nullAncestor = false;
        nullClampedLevel = 0;
        missingClampedLevel = 0;
    }

    @Override
    public void clearStateOnEnd() {
        // A group that ends mid-round must not leak its partial count into the next record.
        clearRound();
    }

    @Override
    void addValue(AbstractValueAssembler value) throws HyracksDataException {
        if (value == keyAssembler) {
            IValueReference key = value.getValue();
            pendingKey = isRequested(key) ? key : null;
        } else if (pendingKey != null) {
            getParent().addOpenField(pendingKey, value.getValue());
            pendingKey = null;
        }
        onReport(true);
    }

    @Override
    void addNull(AbstractValueAssembler value) throws HyracksDataException {
        if (value == keyAssembler) {
            // Keys are field names, written only as strings: a null key means the key column is corrupt.
            throw new HyracksDataException("null key in a collapsed object's map");
        }
        if (pendingKey != null) {
            getParent().addOpenField(pendingKey, NULL);
            pendingKey = null;
        }
        onReport(true);
    }

    @Override
    void addMissing() throws HyracksDataException {
        /*
         * A written entry never has a MISSING value. Under a union this is a non-holding arm (see above); otherwise it
         * is the trailing entry that closes the record's group, which arrives after the last pair was emitted. A key
         * still pending here would mean an entry lost its value.
         */
        if (!unionValue && pendingKey != null) {
            throw new HyracksDataException("map entry with a key but no value");
        }
        onReport(true);
    }

    @Override
    void addValueToParent() throws HyracksDataException {
        // Fields were already spliced into the parent as they arrived. This only advances the parent's count of
        // visited children by one, so the map costs a single slot however many entries it produced.
        getParent().addMissing();
    }

    @Override
    public IValueReference getValue() {
        return storage;
    }

    /**
     * The assembled key carries a type tag; requested names are held untagged, so the tag byte is skipped when
     * comparing. Requested sets are tiny — usually a single name — so a linear scan beats hashing the key.
     */
    private boolean isRequested(IValueReference key) {
        if (requestedKeys == null) {
            return true;
        }
        int keyStart = key.getStartOffset() + 1;
        int keyLength = key.getLength() - 1;
        for (int i = 0; i < requestedKeys.size(); i++) {
            IValueReference requested = requestedKeys.get(i);
            if (requested.getLength() != keyLength) {
                continue;
            }
            if (Arrays.equals(key.getByteArray(), keyStart, keyStart + keyLength, requested.getByteArray(),
                    requested.getStartOffset(), requested.getStartOffset() + keyLength)) {
                return true;
            }
        }
        return false;
    }
}
