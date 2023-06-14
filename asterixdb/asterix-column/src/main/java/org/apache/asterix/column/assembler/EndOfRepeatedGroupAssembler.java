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

import org.apache.asterix.column.assembler.value.MissingValueGetter;
import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class EndOfRepeatedGroupAssembler extends AbstractPrimitiveValueAssembler {
    //    private final List<ArrayValueAssembler> arrays;
    private final ArrayValueAssembler arrayAssembler;
    private final int delimiterIndex;
    private EndOfRepeatedGroupAssembler previousGroup;

    EndOfRepeatedGroupAssembler(IColumnValuesReader reader, ArrayValueAssembler arrayAssembler, int delimiterIndex) {
        super(reader.getLevel(), new AssemblerInfo(), reader, MissingValueGetter.INSTANCE);
        this.arrayAssembler = arrayAssembler;
        this.delimiterIndex = delimiterIndex;
        previousGroup = null;
    }

    @Override
    public void reset(AbstractBytesInputStream in, int numberOfTuples) throws HyracksDataException {
        // NoOp
    }

    @Override
    public int next(AssemblerState state) throws HyracksDataException {
        // Get the current delimiter index from the reader
        int delimiterIndex = reader.getDelimiterIndex();
        /*
         * Check if this "Group Ender" is relevant in this round -- meaning if the 'arrayAssembler' is actively
         * accepting items (i.e., the 'arrayAssembler' is not NULL or MISSING)
         */
        if (delimiterIndex > this.delimiterIndex) {
            // The group ender is not relevant, check if it is a delegate for an upper nesting level
            if (arrayAssembler.isDelegate()) {
                // Yes it is a delegate, end the arrayAssembler to signal to the parent assembler to finalize
                arrayAssembler.end();
            }
            // Move ot the next assembler
            return NEXT_ASSEMBLER;
        }

        // Initially, we are assuming to go to the next assembler
        int nextIndex = NEXT_ASSEMBLER;
        // Is it the end of this group?
        if (reader.isDelimiter() && delimiterIndex == this.delimiterIndex) {
            // Yes, end 'arrayAssembler'
            arrayAssembler.end();
            // And exit from this group and move to the next assembler
            state.exitRepeatedGroup(previousGroup);
        } else {
            // No, return to the first value of the group
            nextIndex = arrayAssembler.getFirstValueIndex();
            if (!state.isCurrentGroup(this)) {
                // Set the group delimiterIndex to indicate we are iterating over a group now (i.e., not the first round)
                previousGroup = state.enterRepeatedGroup(this);
            }
        }
        return nextIndex;
    }

    @Override
    public void skip(int count) throws HyracksDataException {
        // noOp
    }
}
