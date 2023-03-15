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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.assembler.value.IValueGetter;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;

class RepeatedPrimitiveValueAssembler extends AbstractPrimitiveValueAssembler {
    private final List<ArrayValueAssembler> arrays;

    RepeatedPrimitiveValueAssembler(int level, AssemblerInfo info, IColumnValuesReader reader,
            IValueGetter primitiveValue) {
        super(level, info, reader, primitiveValue);
        this.arrays = new ArrayList<>();
    }

    public void addArray(ArrayValueAssembler assembler) {
        arrays.add(assembler);
    }

    @Override
    public int next() throws HyracksDataException {
        if (!reader.next()) {
            throw new IllegalStateException("No more values");
        } else if (reader.isNull() && (!arrays.isEmpty() || reader.getLevel() + 1 == level)) {
            /*
             * There are two cases here for where the null belongs to:
             * 1- If the null is an array item, then add it
             * 2- If the null is an ancestor, then we only add null if this column is the array delegate
             * (i.e., !arrays.isEmpty())
             */
            addNullToAncestor(reader.getLevel());
        } else if (reader.isMissing() && reader.getLevel() + 1 == level) {
            /*
             * Add a missing item
             */
            addMissingToAncestor(reader.getLevel());
        } else if (reader.isValue()) {
            addValueToParent();
        }

        if (isDelegate()) {
            getParent().end();
        }

        //Initially, go to the next primitive assembler
        int nextIndex = NEXT_ASSEMBLER;
        if (!arrays.isEmpty()) {
            /*
             * This assembler is a delegate of a repeated group
             * The delimiter index tells us that this assembler is responsible for a finished group
             */
            int delimiterIndex = reader.getDelimiterIndex();
            if (delimiterIndex < arrays.size() && reader.isDelimiter()) {
                //Also finish the next group
                delimiterIndex++;
            }

            int numberOfFinishedGroups = Math.min(delimiterIndex, arrays.size());
            for (int i = 0; i < numberOfFinishedGroups; i++) {
                //I'm the delegate for this group of repeated values and the group(s) is finished
                ArrayValueAssembler assembler = arrays.get(i);
                assembler.end();
            }

            //Is the repeated group (determined by the delimiter index) still unfinished?
            if (delimiterIndex < arrays.size()) {
                //Yes, go to the first value of the unfinished repeated group
                nextIndex = arrays.get(delimiterIndex).getFirstValueIndex();
            }
        }

        //Go to next value
        return nextIndex;
    }
}
