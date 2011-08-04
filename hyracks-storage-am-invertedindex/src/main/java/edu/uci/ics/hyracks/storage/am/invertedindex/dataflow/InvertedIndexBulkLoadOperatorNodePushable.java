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

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexHelperOpenMode;
import edu.uci.ics.hyracks.storage.am.common.dataflow.PermutingFrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexOpHelper;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;

public class InvertedIndexBulkLoadOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
    private final TreeIndexOpHelper treeIndexOpHelper;
    private float btreeFillFactor;

    private final InvertedIndexOpHelper invIndexOpHelper;
    protected final IInvertedListBuilder invListBuilder;
    private InvertedIndex.BulkLoadContext bulkLoadCtx;

    private final IHyracksStageletContext ctx;

    private FrameTupleAccessor accessor;
    private PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();

    private IRecordDescriptorProvider recordDescProvider;

    public InvertedIndexBulkLoadOperatorNodePushable(AbstractInvertedIndexOperatorDescriptor opDesc,
            IHyracksStageletContext ctx, int partition, int[] fieldPermutation, float btreeFillFactor,
            IInvertedListBuilder invListBuilder, IRecordDescriptorProvider recordDescProvider) {
        treeIndexOpHelper = opDesc.getTreeIndexOpHelperFactory().createTreeIndexOpHelper(opDesc, ctx, partition,
                IndexHelperOpenMode.CREATE);
        invIndexOpHelper = new InvertedIndexOpHelper(opDesc, ctx, partition, IndexHelperOpenMode.CREATE);
        this.btreeFillFactor = btreeFillFactor;
        this.recordDescProvider = recordDescProvider;
        this.ctx = ctx;
        this.invListBuilder = invListBuilder;
        tuple.setFieldPermutation(fieldPermutation);
    }

    @Override
    public void open() throws HyracksDataException {
        AbstractInvertedIndexOperatorDescriptor opDesc = (AbstractInvertedIndexOperatorDescriptor) treeIndexOpHelper
                .getOperatorDescriptor();
        RecordDescriptor recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
        accessor = new FrameTupleAccessor(treeIndexOpHelper.getHyracksStageletContext().getFrameSize(), recDesc);

        // btree
        try {
            treeIndexOpHelper.init();
            treeIndexOpHelper.getTreeIndex().open(treeIndexOpHelper.getIndexFileId());
        } catch (Exception e) {
            // cleanup in case of failure
            treeIndexOpHelper.deinit();
            throw new HyracksDataException(e);
        }

        // inverted index
        try {
            invIndexOpHelper.init();
            invIndexOpHelper.getInvIndex().open(invIndexOpHelper.getInvIndexFileId());
            bulkLoadCtx = invIndexOpHelper.getInvIndex().beginBulkLoad(invListBuilder, ctx.getFrameSize(),
                    btreeFillFactor);
        } catch (Exception e) {
            // cleanup in case of failure
            invIndexOpHelper.deinit();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            tuple.reset(accessor, i);
            invIndexOpHelper.getInvIndex().bulkLoadAddTuple(bulkLoadCtx, tuple);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            invIndexOpHelper.getInvIndex().endBulkLoad(bulkLoadCtx);
        } finally {
            treeIndexOpHelper.deinit();
        }
    }

    @Override
    public void flush() throws HyracksDataException {
    }
}