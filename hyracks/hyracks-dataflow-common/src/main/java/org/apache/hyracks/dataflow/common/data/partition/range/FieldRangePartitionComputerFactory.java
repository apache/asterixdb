/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.common.data.partition.range;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class FieldRangePartitionComputerFactory implements ITuplePartitionComputerFactory {
    private static final long serialVersionUID = 1L;
    private final int[] rangeFields;
    private IRangeMap rangeMap;
    private IBinaryComparatorFactory[] comparatorFactories;

    public FieldRangePartitionComputerFactory(int[] rangeFields, IBinaryComparatorFactory[] comparatorFactories,
            IRangeMap rangeMap) {
        this.rangeFields = rangeFields;
        this.comparatorFactories = comparatorFactories;
        this.rangeMap = rangeMap;
    }

    @Override
    public ITuplePartitionComputer createPartitioner() {
        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        return new ITuplePartitionComputer() {
            @Override
            /**
             * Determine the range partition. 
             */
            public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
                if (nParts == 1) {
                    return 0;
                }
                int slotIndex = getRangePartition(accessor, tIndex);
                // Map range partition to node partitions.
                double rangesPerPart = 1;
                if (rangeMap.getSplitCount() + 1 > nParts) {
                    rangesPerPart = ((double) rangeMap.getSplitCount() + 1) / nParts;
                }
                return (int) Math.floor(slotIndex / rangesPerPart);
            }

            /*
             * Determine the range partition.
             */
            public int getRangePartition(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                int slotIndex = 0;
                for (int i = 0; i < rangeMap.getSplitCount(); ++i) {
                    int c = compareSlotAndFields(accessor, tIndex, i);
                    if (c < 0) {
                        return slotIndex;
                    }
                    slotIndex++;
                }
                return slotIndex;
            }

            public int compareSlotAndFields(IFrameTupleAccessor accessor, int tIndex, int fieldIndex)
                    throws HyracksDataException {
                int c = 0;
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int slotLength = accessor.getFieldSlotsLength();
                for (int f = 0; f < comparators.length; ++f) {
                    int fIdx = rangeFields[f];
                    int fStart = accessor.getFieldStartOffset(tIndex, fIdx);
                    int fEnd = accessor.getFieldEndOffset(tIndex, fIdx);
                    c = comparators[f].compare(accessor.getBuffer().array(), startOffset + slotLength + fStart, fEnd
                            - fStart, rangeMap.getByteArray(fieldIndex, f), rangeMap.getStartOffset(fieldIndex, f),
                            rangeMap.getLength(fieldIndex, f));
                    if (c != 0) {
                        return c;
                    }
                }
                return c;
            }

        };
    }
}