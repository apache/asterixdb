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

package org.apache.hyracks.dataflow.common.data.partition.range;

import java.io.Serializable;
import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

abstract class AbstractFieldRangePartitionComputerFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RangeMapSupplier rangeMapSupplier;

    private final IBinaryComparatorFactory[] comparatorFactories;

    protected final SourceLocation sourceLoc;

    AbstractFieldRangePartitionComputerFactory(RangeMapSupplier rangeMapSupplier,
            IBinaryComparatorFactory[] comparatorFactories, SourceLocation sourceLoc) {
        this.rangeMapSupplier = rangeMapSupplier;
        this.comparatorFactories = comparatorFactories;
        this.sourceLoc = sourceLoc;
    }

    private IBinaryComparator[] createBinaryComparators() {
        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        return comparators;
    }

    private abstract class AbstractFieldRangePartitionComputer {

        final IHyracksTaskContext taskContext;

        final RangeMapPartitionComputer rangeMapPartitionComputer;

        private AbstractFieldRangePartitionComputer(IHyracksTaskContext taskContext) {
            this.taskContext = taskContext;
            this.rangeMapPartitionComputer = new RangeMapPartitionComputer();
        }

        public void initialize() throws HyracksDataException {
            rangeMapPartitionComputer.initialize(taskContext);
        }
    }

    abstract class AbstractFieldRangeSinglePartitionComputer extends AbstractFieldRangePartitionComputer
            implements ITuplePartitionComputer {

        AbstractFieldRangeSinglePartitionComputer(IHyracksTaskContext taskContext) {
            super(taskContext);
        }

        @Override
        public final int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
            return nParts == 1 ? 0 : computePartition(accessor, tIndex, nParts);
        }

        protected abstract int computePartition(IFrameTupleAccessor accessor, int tIndex, int nParts)
                throws HyracksDataException;
    }

    abstract class AbstractFieldRangeMultiPartitionComputer extends AbstractFieldRangePartitionComputer
            implements ITupleMultiPartitionComputer {

        private BitSet result;

        AbstractFieldRangeMultiPartitionComputer(IHyracksTaskContext taskContext) {
            super(taskContext);
        }

        @Override
        public void initialize() throws HyracksDataException {
            super.initialize();
            if (result == null) {
                result = new BitSet();
            }
        }

        @Override
        public final BitSet partition(IFrameTupleAccessor accessor, int tIndex, int nParts)
                throws HyracksDataException {
            result.clear();
            if (nParts == 1) {
                result.set(0);
            } else {
                int pStart = computeStartPartition(accessor, tIndex, nParts);
                int pEnd = computeEndPartition(accessor, tIndex, nParts);
                result.set(pStart, pEnd + 1);
            }
            return result;
        }

        protected abstract int computeStartPartition(IFrameTupleAccessor accessor, int tIndex, int nParts)
                throws HyracksDataException;

        protected abstract int computeEndPartition(IFrameTupleAccessor accessor, int tIndex, int nParts)
                throws HyracksDataException;
    }

    final class RangeMapPartitionComputer {

        private RangeMap rangeMap;

        private IBinaryComparator[] comparators;

        protected void initialize(IHyracksTaskContext taskContext) throws HyracksDataException {
            rangeMap = rangeMapSupplier.getRangeMap(taskContext);
            if (rangeMap == null) {
                throw HyracksDataException.create(ErrorCode.RANGEMAP_NOT_FOUND, sourceLoc);
            }
            if (comparators == null) {
                comparators = createBinaryComparators();
            }
        }

        int partition(IFrameTupleAccessor accessor, int tIndex, int[] rangeFields, int nParts)
                throws HyracksDataException {
            int slotIndex = findRangeMapSlot(accessor, tIndex, rangeFields);
            return mapRangeMapSlotToPartition(slotIndex, nParts);
        }

        int exclusivePartition(IFrameTupleAccessor accessor, int tIndex, int[] rangeFields, int nParts)
                throws HyracksDataException {
            int slotIndex = findRangeMapExclusiveSlot(accessor, tIndex, rangeFields);
            return mapRangeMapSlotToPartition(slotIndex, nParts);
        }

        private int mapRangeMapSlotToPartition(int slotIndex, int nParts) {
            // Map range partition to node partitions.
            double rangesPerPart = 1;
            if (rangeMap.getSplitCount() + 1 > nParts) {
                rangesPerPart = ((double) rangeMap.getSplitCount() + 1) / nParts;
            }
            return (int) Math.floor(slotIndex / rangesPerPart);
        }

        private int findRangeMapSlot(IFrameTupleAccessor accessor, int tIndex, int[] rangeFields)
                throws HyracksDataException {
            int slotIndex = 0;
            for (int slotNumber = 0, n = rangeMap.getSplitCount(); slotNumber < n; ++slotNumber) {
                int c = compareSlotAndFields(accessor, tIndex, rangeFields, slotNumber);
                if (c < 0) {
                    return slotIndex;
                }
                slotIndex++;
            }
            return slotIndex;
        }

        private int findRangeMapExclusiveSlot(IFrameTupleAccessor accessor, int tIndex, int[] rangeFields)
                throws HyracksDataException {
            int slotIndex = 0;
            for (int slotNumber = 0, n = rangeMap.getSplitCount(); slotNumber < n; ++slotNumber) {
                int c = compareSlotAndFields(accessor, tIndex, rangeFields, slotNumber);
                if (c <= 0) {
                    return slotIndex;
                }
                slotIndex++;
            }
            return slotIndex;
        }

        private int compareSlotAndFields(IFrameTupleAccessor accessor, int tIndex, int[] rangeFields, int slotNumber)
                throws HyracksDataException {
            int c = 0;
            int startOffset = accessor.getTupleStartOffset(tIndex);
            int slotLength = accessor.getFieldSlotsLength();
            for (int fieldNum = 0; fieldNum < comparators.length; ++fieldNum) {
                int fIdx = rangeFields[fieldNum];
                int fStart = accessor.getFieldStartOffset(tIndex, fIdx);
                int fEnd = accessor.getFieldEndOffset(tIndex, fIdx);
                c = comparators[fieldNum].compare(accessor.getBuffer().array(), startOffset + slotLength + fStart,
                        fEnd - fStart, rangeMap.getByteArray(), rangeMap.getStartOffset(fieldNum, slotNumber),
                        rangeMap.getLength(fieldNum, slotNumber));
                if (c != 0) {
                    return c;
                }
            }
            return c;
        }
    }
}
