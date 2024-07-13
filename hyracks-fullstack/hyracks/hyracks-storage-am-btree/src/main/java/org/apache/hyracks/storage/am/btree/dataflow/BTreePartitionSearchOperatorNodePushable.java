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
package org.apache.hyracks.storage.am.btree.dataflow;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

public class BTreePartitionSearchOperatorNodePushable extends BTreeSearchOperatorNodePushable {

    private final int pIdx;

    public BTreePartitionSearchOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            RecordDescriptor inputRecDesc, int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive,
            boolean highKeyInclusive, int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory nonMatchWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            boolean appendIndexFilter, IMissingWriterFactory nonFilterWriterFactory,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, boolean appendOpCallbackProceedResult,
            byte[] searchCallbackProceedResultFalseValue, byte[] searchCallbackProceedResultTrueValue,
            ITupleProjectorFactory projectorFactory, ITuplePartitionerFactory tuplePartitionerFactory,
            int[][] partitionsMap, int targetStoragePartition) throws HyracksDataException {
        super(ctx, partition, inputRecDesc, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive,
                minFilterFieldIndexes, maxFilterFieldIndexes, indexHelperFactory, retainInput, retainMissing,
                nonMatchWriterFactory, searchCallbackFactory, appendIndexFilter, nonFilterWriterFactory,
                tupleFilterFactory, outputLimit, appendOpCallbackProceedResult, searchCallbackProceedResultFalseValue,
                searchCallbackProceedResultTrueValue, projectorFactory, tuplePartitionerFactory, partitionsMap);
        pIdx = storagePartitionId2Index.getOrDefault(targetStoragePartition, Integer.MIN_VALUE);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        try {
            searchPartition(tupleCount);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private void searchPartition(int tupleCount) throws Exception {
        if (pIdx >= 0 && pIdx < cursors.length) {
            for (int i = 0; i < tupleCount && !finished; i++) {
                resetSearchPredicate(i);
                cursors[pIdx].close();
                indexAccessors[pIdx].search(cursors[pIdx], searchPred);
                writeSearchResults(i, cursors[pIdx]);
            }
        }
    }
}
