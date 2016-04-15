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

package org.apache.hyracks.dataflow.std.parallel.base;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;

/**
 * @author michael
 */
public class FieldRangePartitionDelayComputerFactory implements ITuplePartitionComputerFactory {

    private static final long serialVersionUID = 1L;
    private final int[] rangeFields;
    private IRangeMap rangeMap;
    private IBinaryComparatorFactory[] comparatorFactories;

    public FieldRangePartitionDelayComputerFactory(int[] rangeFields, IBinaryComparatorFactory[] comparatorFactories) {
        this.rangeFields = rangeFields;
        this.comparatorFactories = comparatorFactories;
    }

    @Override
    public ITuplePartitionComputer createPartitioner(IHyracksTaskContext ctx, int index) {

        try {
            IStateObject rangeState = ctx.getGlobalState(index);
            rangeMap = ((ParallelRangeMapTaskState) rangeState).getRangeMap();
        } catch (HyracksDataException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

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
