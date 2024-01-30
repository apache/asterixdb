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
package org.apache.hyracks.algebricks.runtime.operators.writer;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PointableTupleReference;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupWriter;

class WriterPartitioner implements IWriterPartitioner {
    private final int[] partitionColumns;
    private final IBinaryComparator[] partitionComparators;
    private final PointableTupleReference partitionColumnsPrevCopy;
    private final PermutingFrameTupleReference partitionColumnsRef;
    private boolean first = true;

    public WriterPartitioner(int[] partitionColumns, IBinaryComparator[] partitionComparators) {
        this.partitionColumns = partitionColumns;
        this.partitionComparators = partitionComparators;
        partitionColumnsRef = new PermutingFrameTupleReference(partitionColumns);
        partitionColumnsPrevCopy =
                PointableTupleReference.create(partitionColumns.length, ArrayBackedValueStorage::new);
    }

    @Override
    public boolean isNewPartition(FrameTupleAccessor tupleAccessor, int index) throws HyracksDataException {
        if (first) {
            first = false;
            partitionColumnsRef.reset(tupleAccessor, index);
            partitionColumnsPrevCopy.set(partitionColumnsRef);
            return true;
        }

        boolean newPartition = !PreclusteredGroupWriter.sameGroup(partitionColumnsPrevCopy, tupleAccessor, index,
                partitionColumns, partitionComparators);

        // Set previous
        partitionColumnsRef.reset(tupleAccessor, index);
        partitionColumnsPrevCopy.set(partitionColumnsRef);

        return newPartition;
    }
}