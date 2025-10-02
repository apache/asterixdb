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

public class ArrayWithUnionValueAssembler extends ArrayValueAssembler {
    private final int numberOfUnionChildren;
    private int numberOfAddedValues;
    private boolean nonMissingValueAdded;

    ArrayWithUnionValueAssembler(int level, AssemblerInfo info, int firstValueIndex, AbstractSchemaNode itemNode) {
        super(level, info, firstValueIndex);
        this.numberOfUnionChildren = ((UnionSchemaNode) itemNode).getChildren().size();
    }

    @Override
    void reset() {
        numberOfAddedValues = 0;
        nonMissingValueAdded = false;
        super.reset();
    }

    @Override
    void addValue(AbstractValueAssembler value) throws HyracksDataException {
        nonMissingValueAdded = true;
        numberOfAddedValues++;
        super.addValue(value);
        if (numberOfAddedValues == numberOfUnionChildren) {
            // Completed a chunk; since we saw a non-missing, just reset the counters
            nonMissingValueAdded = false;
            numberOfAddedValues = 0;
        }
    }

    @Override
    void addNull(AbstractValueAssembler value) throws HyracksDataException {
        nonMissingValueAdded = true;
        numberOfAddedValues++;
        super.addNull(value);
        if (numberOfAddedValues == numberOfUnionChildren) {
            // Completed a chunk; since we saw a non-missing, just reset the counters
            nonMissingValueAdded = false;
            numberOfAddedValues = 0;
        }
    }

    @Override
    void addMissing() throws HyracksDataException {
        numberOfAddedValues++;
        if (numberOfAddedValues == numberOfUnionChildren) {
            if (!nonMissingValueAdded) {
                super.addMissing();
            }
            // Reset for the next chunk
            numberOfAddedValues = 0;
            nonMissingValueAdded = false;
        }
    }
}