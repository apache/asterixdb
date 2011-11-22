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
import edu.uci.ics.hyracks.storage.am.common.api.PageAllocationException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.PermutingFrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.FixedSizeElementInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;

public class InvertedIndexBulkLoadOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
    private final TreeIndexDataflowHelper btreeDataflowHelper;
    private final InvertedIndexDataflowHelper invIndexDataflowHelper;
    private final IInvertedListBuilder invListBuilder;
    private InvertedIndex invIndex;
    private InvertedIndex.BulkLoadContext bulkLoadCtx;

    private final IHyracksTaskContext ctx;

    private FrameTupleAccessor accessor;
    private PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();

    private IRecordDescriptorProvider recordDescProvider;

    public InvertedIndexBulkLoadOperatorNodePushable(AbstractInvertedIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, int[] fieldPermutation, IRecordDescriptorProvider recordDescProvider) {
        btreeDataflowHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory()
                .createIndexDataflowHelper(opDesc, ctx, partition, true);
        invIndexDataflowHelper = new InvertedIndexDataflowHelper(btreeDataflowHelper, opDesc, ctx, partition, true);
        this.recordDescProvider = recordDescProvider;
        this.ctx = ctx;
        this.invListBuilder = new FixedSizeElementInvertedListBuilder(opDesc.getInvListsTypeTraits());
        tuple.setFieldPermutation(fieldPermutation);
    }

    @Override
    public void open() throws HyracksDataException {
        AbstractInvertedIndexOperatorDescriptor opDesc = (AbstractInvertedIndexOperatorDescriptor) btreeDataflowHelper
                .getOperatorDescriptor();
        RecordDescriptor recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
        accessor = new FrameTupleAccessor(btreeDataflowHelper.getHyracksTaskContext().getFrameSize(), recDesc);

        // BTree.
        try {
            btreeDataflowHelper.init();
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
            invIndexDataflowHelper.init();
            invIndex = (InvertedIndex) invIndexDataflowHelper.getIndex();
            bulkLoadCtx = invIndex.beginBulkLoad(invListBuilder, ctx.getFrameSize(), BTree.DEFAULT_FILL_FACTOR);
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
            try {
                invIndex.bulkLoadAddTuple(bulkLoadCtx, tuple);
            } catch (PageAllocationException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            invIndex.endBulkLoad(bulkLoadCtx);
        } catch (PageAllocationException e) {
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
    }
}