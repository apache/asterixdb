/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.storage.am.invertedindex.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.dataflow.PermutingFrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex.InvertedIndexBulkLoadContext;

public class InvertedIndexBulkLoadOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
    private final TreeIndexDataflowHelper btreeDataflowHelper;
    private final InvertedIndexDataflowHelper invIndexDataflowHelper;
    private InvertedIndex invIndex;
    private InvertedIndex.InvertedIndexBulkLoadContext bulkLoadCtx;

    private FrameTupleAccessor accessor;
    private PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();

    private IRecordDescriptorProvider recordDescProvider;

    public InvertedIndexBulkLoadOperatorNodePushable(AbstractInvertedIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, int[] fieldPermutation, IRecordDescriptorProvider recordDescProvider) {
        btreeDataflowHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory()
                .createIndexDataflowHelper(opDesc, ctx, partition);
        invIndexDataflowHelper = new InvertedIndexDataflowHelper(btreeDataflowHelper, opDesc, ctx, partition);
        this.recordDescProvider = recordDescProvider;
        tuple.setFieldPermutation(fieldPermutation);
    }

    @Override
    public void open() throws HyracksDataException {
        AbstractInvertedIndexOperatorDescriptor opDesc = (AbstractInvertedIndexOperatorDescriptor) btreeDataflowHelper
                .getOperatorDescriptor();
        RecordDescriptor recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        accessor = new FrameTupleAccessor(btreeDataflowHelper.getHyracksTaskContext().getFrameSize(), recDesc);

        // BTree.
        try {
            btreeDataflowHelper.init(false);
        } catch (Exception e) {
            // Cleanup in case of failure.
            btreeDataflowHelper.deinit();
            if (e instanceof HyracksDataException) {
                throw (HyracksDataException) e;
            } else {
                throw new HyracksDataException(e);
            }
        }

        // Inverted Index.
        try {
            invIndexDataflowHelper.init(false);
            invIndex = (InvertedIndex) invIndexDataflowHelper.getIndex();
            bulkLoadCtx = (InvertedIndexBulkLoadContext) invIndex.beginBulkLoad(BTree.DEFAULT_FILL_FACTOR);
        } catch (Exception e) {
            // Cleanup in case of failure.
            invIndexDataflowHelper.deinit();
            if (e instanceof HyracksDataException) {
                throw (HyracksDataException) e;
            } else {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            tuple.reset(accessor, i);
            invIndex.bulkLoadAddTuple(tuple, bulkLoadCtx);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            invIndex.endBulkLoad(bulkLoadCtx);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            try {
                btreeDataflowHelper.deinit();
            } finally {
                invIndexDataflowHelper.deinit();
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}