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
package org.apache.hyracks.dataflow.common.data.partition;

import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class SpatialPartitionComputerFactory implements ITupleMultiPartitionComputerFactory {
    private final int[] partitioningFields;
    double minX;
    double minY;
    double maxX;
    double maxY;
    int numRows;
    int numColumns;

    public SpatialPartitionComputerFactory(int[] partitioningFields, double minX, double minY, double maxX, double maxY,
            int numRows, int numColumns) {
        this.partitioningFields = partitioningFields;
        this.minX = minX;
        this.minY = minY;
        this.maxX = maxX;
        this.maxY = maxY;
        this.numRows = numRows;
        this.numColumns = numColumns;
    }

    @Override
    public ITupleMultiPartitionComputer createPartitioner(IHyracksTaskContext hyracksTaskContext) {
        return new SpatialMultiPartitionComputer(partitioningFields, minX, minY, maxX, maxY, numRows, numColumns);
    }

    class SpatialMultiPartitionComputer implements ITupleMultiPartitionComputer {
        private final int[] partitioningFields;
        double minX;
        double minY;
        double maxX;
        double maxY;
        int numRows;
        int numColumns;
        private BitSet result;

        public SpatialMultiPartitionComputer(int[] partitioningFields, double minX, double minY, double maxX,
                double maxY, int numRows, int numColumns) {
            this.partitioningFields = partitioningFields;
            this.minX = minX;
            this.minY = minY;
            this.maxX = maxX;
            this.maxY = maxY;
            this.numRows = numRows;
            this.numColumns = numColumns;
        }

        @Override
        public BitSet partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
            int fieldCount = accessor.getFieldCount();
            int tupleCount = accessor.getTupleCount();
            int startOffset = accessor.getTupleStartOffset(tIndex);
            int slotLength = accessor.getFieldSlotsLength();
            int fIdx = partitioningFields[0];
            int fStart = accessor.getFieldStartOffset(tIndex, fIdx);
//            int fEnd = accessor.getFieldEndOffset(tIndex, fIdx);
            double x1 = accessor.getBuffer().getDouble(startOffset + slotLength + fStart + 1);
            double y1 = accessor.getBuffer().getDouble(startOffset + slotLength + fStart + 8 + 1);
            double x2 = accessor.getBuffer().getDouble(startOffset + slotLength + fStart + 16 + 1);
            double y2 = accessor.getBuffer().getDouble(startOffset + slotLength + fStart + 24 + 1);

            int row1 = (int) Math.floor((x1 - minX) * numRows / (maxX - minX));
            int col1 = (int) Math.floor((y1 - minY) * numColumns / (maxY - minY));
            int row2 = (int) Math.floor((x2 - minX) * numRows / (maxX - minX));
            int col2 = (int) Math.floor((y2 - minY) * numColumns / (maxY - minY));

            int minRow = Math.min(row1, row2);
            int maxRow = Math.max(row1, row2);
            int minCol = Math.min(col1, col2);
            int maxCol = Math.max(col1, col2);

            result.clear();

            for (int i = minRow; i <= maxRow; i++) {
                for (int j = minCol; j <= maxCol; j++) {
                    int tileId = i * numColumns + j;
                    if (tileId < nParts) {
                        result.set(tileId);
                    } else {
                        result.set(0);
                    }
                }
            }
            return result;
        }

        @Override
        public void initialize() throws HyracksDataException {
            if (result == null) {
                result = new BitSet();
            }
        }
    }
}
