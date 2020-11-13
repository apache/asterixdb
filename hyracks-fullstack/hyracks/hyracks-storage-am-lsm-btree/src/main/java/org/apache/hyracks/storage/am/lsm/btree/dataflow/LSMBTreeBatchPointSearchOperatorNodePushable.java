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
package org.apache.hyracks.storage.am.lsm.btree.dataflow;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.btree.impls.BatchPredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeBatchPointSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class LSMBTreeBatchPointSearchOperatorNodePushable extends BTreeSearchOperatorNodePushable {

    private final int[] keyFields;

    public LSMBTreeBatchPointSearchOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            RecordDescriptor inputRecDesc, int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive,
            boolean highKeyInclusive, int[] minFilterKeyFields, int[] maxFilterKeyFields,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            ITupleFilterFactory tupleFilterFactory, long outputLimit) throws HyracksDataException {
        super(ctx, partition, inputRecDesc, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive,
                minFilterKeyFields, maxFilterKeyFields, indexHelperFactory, retainInput, retainMissing,
                missingWriterFactory, searchCallbackFactory, false, tupleFilterFactory, outputLimit, false, null, null);
        this.keyFields = lowKeyFields;
    }

    @Override
    protected IIndexCursor createCursor() throws HyracksDataException {
        ILSMIndexAccessor lsmAccessor = (ILSMIndexAccessor) indexAccessor;
        return new LSMBTreeBatchPointSearchCursor(lsmAccessor.getOpContext());
    }

    @Override
    protected ISearchPredicate createSearchPredicate() {
        ITreeIndex treeIndex = (ITreeIndex) index;
        lowKeySearchCmp =
                highKeySearchCmp = BTreeUtils.getSearchMultiComparator(treeIndex.getComparatorFactories(), lowKey);
        return new BatchPredicate(accessor, lowKeySearchCmp, keyFields, minFilterFieldIndexes, maxFilterFieldIndexes);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        if (accessor.getTupleCount() > 0) {
            BatchPredicate batchPred = (BatchPredicate) searchPred;
            batchPred.reset(accessor);
            try {
                indexAccessor.search(cursor, batchPred);
                writeSearchResults();
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            } finally {
                cursor.close();
            }
        }
    }

    protected void writeSearchResults() throws IOException {
        long matchingTupleCount = 0;
        LSMBTreeBatchPointSearchCursor batchCursor = (LSMBTreeBatchPointSearchCursor) cursor;
        int tupleIndex = 0;
        while (cursor.hasNext()) {
            cursor.next();
            matchingTupleCount++;
            ITupleReference tuple = cursor.getTuple();
            if (tupleFilter != null) {
                referenceFilterTuple.reset(tuple);
                if (!tupleFilter.accept(referenceFilterTuple)) {
                    continue;
                }
            }
            tb.reset();

            if (retainInput && retainMissing) {
                appendMissingTuple(tupleIndex, batchCursor.getKeyIndex());
            }

            tupleIndex = batchCursor.getKeyIndex();

            if (retainInput) {
                frameTuple.reset(accessor, tupleIndex);
                for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                    dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    tb.addFieldEndOffset();
                }
            }
            writeTupleToOutput(tuple);
            FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            if (outputLimit >= 0 && ++outputCount >= outputLimit) {
                finished = true;
                break;
            }
        }
        stats.getTupleCounter().update(matchingTupleCount);

    }

    private void appendMissingTuple(int start, int end) throws HyracksDataException {
        for (int i = start; i < end; i++) {
            FrameUtils.appendConcatToWriter(writer, appender, accessor, i, nonMatchTupleBuild.getFieldEndOffsets(),
                    nonMatchTupleBuild.getByteArray(), 0, nonMatchTupleBuild.getSize());
        }
    }

}
