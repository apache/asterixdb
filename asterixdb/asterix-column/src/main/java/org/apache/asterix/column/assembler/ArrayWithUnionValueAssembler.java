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

import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Handles assembling array elements whose items are union types.
 * Manages propagation of nulls/missings to parent assemblers and
 * chunk completion logic per union branch.
 */
public class ArrayWithUnionValueAssembler extends ArrayValueAssembler {

    private final int numberOfUnionChildren;

    private int numberOfAddedValues;
    private boolean nonMissingValueAdded;
    private boolean firstTime = true;

    private boolean nullAncestor;
    private int nullClampedLevel;
    private int missingClampedLevel;
    private boolean parentMissing = true;

    ArrayWithUnionValueAssembler(int level, AssemblerInfo info, int firstValueIndex, AbstractSchemaNode itemNode) {
        super(level, info, firstValueIndex);
        this.numberOfUnionChildren = ((UnionSchemaNode) itemNode).getChildren().size();
    }

    @Override
    public void reset() {
        // Reset called before reading new array item
        if (firstTime) {
            resetCounters();
        }
        nonMissingValueAdded = false;
        super.reset();
    }

    @Override
    void addValue(AbstractValueAssembler value) throws HyracksDataException {
        parentMissing = false;
        nonMissingValueAdded = true;
        numberOfAddedValues++;

        super.addValue(value);

        if (isChunkComplete()) {
            completeChunk();
        }
    }

    @Override
    void addNull(AbstractValueAssembler value) throws HyracksDataException {
        parentMissing = false;
        nonMissingValueAdded = true;
        numberOfAddedValues++;

        super.addNull(value);

        if (isChunkComplete()) {
            completeChunk();
        }
    }

    @Override
    void addMissing() throws HyracksDataException {
        parentMissing = false;
        numberOfAddedValues++;

        if (isChunkComplete()) {
            // Only propagate missing if no non-missing values appeared in this chunk
            if (!nonMissingValueAdded) {
                super.addMissing();
            }
            completeChunk();
        }
    }

    @Override
    void addNullToAncestor(int nullLevel) throws HyracksDataException {
        numberOfAddedValues++;
        firstTime = false;
        nullAncestor = true;
        nullClampedLevel = Math.max(nullLevel, nullClampedLevel);

        if (isChunkComplete()) {
            if (parentMissing) {
                propagateNullOrMissingToParent(true);
            }
            completeChunk();
        }
    }

    @Override
    void addMissingToAncestor(int missingLevel) throws HyracksDataException {
        numberOfAddedValues++;
        firstTime = false;
        missingClampedLevel = Math.max(missingLevel, missingClampedLevel);

        if (isChunkComplete()) {
            if (parentMissing) {
                propagateNullOrMissingToParent(false);
            }
            completeChunk();
        }
    }

    private void propagateNullOrMissingToParent(boolean isNull) throws HyracksDataException {
        AbstractNestedValueAssembler parent = getParent();
        if (parent == null) {
            return;
        }

        // If any ancestor was null, always propagate null
        if (isNull || nullAncestor) {
            boolean atCurrentLevel = (nullClampedLevel + 1) == level;
            if (atCurrentLevel) {
                parent.start();
                parent.addNull(this);
            } else {
                parent.addNullToAncestor(nullClampedLevel);
            }
        } else {
            boolean atCurrentLevel = (missingClampedLevel + 1) == level;
            if (atCurrentLevel) {
                parent.start();
                parent.addMissing();
            } else {
                parent.addMissingToAncestor(missingClampedLevel);
            }
        }
    }

    private boolean isChunkComplete() {
        return numberOfAddedValues == numberOfUnionChildren;
    }

    private void completeChunk() {
        // Reset chunk-local state
        resetCounters();
        firstTime = true;
    }

    private void resetCounters() {
        numberOfAddedValues = 0;
        nonMissingValueAdded = false;
        parentMissing = true;
        nullAncestor = false;
        nullClampedLevel = 0;
        missingClampedLevel = 0;
    }

    @Override
    void addValueToParent() throws HyracksDataException {
        super.addValueToParent();
        clearStateOnEnd();
    }

    @Override
    public void clearStateOnEnd() {
        resetCounters();
        firstTime = true;
    }
}