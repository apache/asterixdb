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
package edu.uci.ics.hyracks.storage.am.common.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOpContext;

public class TreeIndexInsertUpdateDeleteOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private final TreeIndexOpHelper treeIndexOpHelper;
    private FrameTupleAccessor accessor;
    private final IRecordDescriptorProvider recordDescProvider;
    private final IndexOp op;
    private final PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();
    private ByteBuffer writeBuffer;
    private IndexOpContext opCtx;

    public TreeIndexInsertUpdateDeleteOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, int[] fieldPermutation,
            IRecordDescriptorProvider recordDescProvider, IndexOp op) {
        treeIndexOpHelper = opDesc.getTreeIndexOpHelperFactory().createTreeIndexOpHelper(opDesc, ctx, partition,
                IndexHelperOpenMode.OPEN);
        this.recordDescProvider = recordDescProvider;
        this.op = op;
        tuple.setFieldPermutation(fieldPermutation);
    }

    @Override
    public void open() throws HyracksDataException {
        AbstractTreeIndexOperatorDescriptor opDesc = (AbstractTreeIndexOperatorDescriptor) treeIndexOpHelper
                .getOperatorDescriptor();
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
        accessor = new FrameTupleAccessor(treeIndexOpHelper.getHyracksTaskContext().getFrameSize(), inputRecDesc);
        writeBuffer = treeIndexOpHelper.getHyracksTaskContext().allocateFrame();
        writer.open();
        try {
            treeIndexOpHelper.init();
            treeIndexOpHelper.getTreeIndex().open(treeIndexOpHelper.getIndexFileId());
            opCtx = treeIndexOpHelper.getTreeIndex().createOpContext(op, treeIndexOpHelper.getLeafFrame(),
                    treeIndexOpHelper.getInteriorFrame(), new LIFOMetaDataFrame());
        } catch (Exception e) {
            // cleanup in case of failure
            treeIndexOpHelper.deinit();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        final ITreeIndex treeIndex = treeIndexOpHelper.getTreeIndex();
        accessor.reset(buffer);

        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            tuple.reset(accessor, i);
            try {
                switch (op) {

                    case INSERT: {
                        treeIndex.insert(tuple, opCtx);
                    }
                        break;

                    case DELETE: {
                        treeIndex.delete(tuple, opCtx);
                    }
                        break;

                    default: {
                        throw new HyracksDataException("Unsupported operation " + op
                                + " in tree index InsertUpdateDelete operator");
                    }

                }

            } catch (Exception e) {
                e.printStackTrace();
                throw new HyracksDataException(e);
            }
        }

        // pass a copy of the frame to next op
        System.arraycopy(buffer.array(), 0, writeBuffer.array(), 0, buffer.capacity());
        FrameUtils.flushFrame(writeBuffer, writer);
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            writer.close();
        } finally {
            treeIndexOpHelper.deinit();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}