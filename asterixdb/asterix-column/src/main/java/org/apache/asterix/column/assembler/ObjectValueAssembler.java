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

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class ObjectValueAssembler extends AbstractNestedValueAssembler {

    private final RecordBuilder recordBuilder;
    private final ARecordType recordType;

    private int effectiveFieldCount;
    private int visitedCount;
    private boolean hasNonMissingValue;
    private boolean firstTime;
    private boolean hasNullAncestor;
    // These are used to know the highest level of null/missing ancestor to propagate
    // because some of the fields may be allMissing (as in column was not present)
    private int nullClampedLevel;
    private int missingClampedLevel;

    ObjectValueAssembler(int level, AssemblerInfo info) {
        super(level, info);
        this.recordBuilder = new RecordBuilder();
        this.recordType = (ARecordType) info.getDeclaredType();
        clearState();
    }

    @Override
    public void reset() {
        recordBuilder.reset(recordType);
        storage.reset();
        if (firstTime) {
            clearState();
        }
    }

    @Override
    void addValue(AbstractValueAssembler value) throws HyracksDataException {
        visitedCount++;
        hasNonMissingValue = true;
        addField(value, value.getValue());

        if (isChunkComplete()) {
            finalizeChunk();
        }
    }

    @Override
    void addNull(AbstractValueAssembler value) throws HyracksDataException {
        visitedCount++;
        hasNonMissingValue = true;
        addField(value, NULL);

        if (isChunkComplete()) {
            finalizeChunk();
        }
    }

    @Override
    void addMissing() throws HyracksDataException {
        visitedCount++;
        hasNonMissingValue = true;
        if (isChunkComplete()) {
            finalizeChunk();
        }
    }

    @Override
    void addNullToAncestor(int nullLevel) throws HyracksDataException {
        handleAncestorAddition(nullLevel, true);
    }

    @Override
    void addMissingToAncestor(int missingLevel) throws HyracksDataException {
        handleAncestorAddition(missingLevel, false);
    }

    void setNumberOfEffectiveFields(int numberOfEffectiveFields) {
        this.effectiveFieldCount = numberOfEffectiveFields;
    }

    @Override
    void addValueToParent() throws HyracksDataException {
        storage.reset();
        recordBuilder.write(storage.getDataOutput(), true);
        getParent().addValue(this);
        clearStateOnEnd();
    }

    @Override
    public IValueReference getValue() {
        return storage;
    }

    @Override
    public void clearStateOnEnd() {
        clearState();
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void addField(AbstractValueAssembler value, IValueReference fieldValue) throws HyracksDataException {
        int fieldIndex = value.getFieldIndex();
        if (fieldIndex >= 0) {
            recordBuilder.addField(fieldIndex, fieldValue);
        } else {
            recordBuilder.addField(value.getFieldName(), fieldValue);
        }
    }

    private void handleAncestorAddition(int level, boolean isNull) throws HyracksDataException {
        firstTime = false;
        visitedCount++;

        if (isNull) {
            hasNullAncestor = true;
            nullClampedLevel = Math.max(nullClampedLevel, level);
        } else {
            missingClampedLevel = Math.max(missingClampedLevel, level);
        }

        if (isChunkComplete() && !hasNonMissingValue) {
            propagateToAncestor(); // propagate only if no values were seen
        }

        if (isChunkComplete()) {
            finalizeChunk();
        }
    }

    private boolean isChunkComplete() {
        return visitedCount == effectiveFieldCount;
    }

    private void finalizeChunk() {
        clearChunkState();
        firstTime = true; // ready for next chunk
    }

    private void propagateToAncestor() throws HyracksDataException {
        // propagate nulls preferentially
        if (hasNullAncestor) {
            super.addNullToAncestor(nullClampedLevel);
        } else {
            super.addMissingToAncestor(missingClampedLevel);
        }
    }

    private void clearChunkState() {
        visitedCount = 0;
        hasNullAncestor = false;
        hasNonMissingValue = false;
        nullClampedLevel = 0;
        missingClampedLevel = 0;
    }

    private void clearState() {
        clearChunkState();
        firstTime = true;
    }
}