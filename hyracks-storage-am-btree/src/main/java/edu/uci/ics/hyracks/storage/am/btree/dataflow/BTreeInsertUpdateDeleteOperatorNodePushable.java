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
package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.TreeIndexOp;

public class BTreeInsertUpdateDeleteOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private final BTreeOpHelper btreeOpHelper;
    private FrameTupleAccessor accessor;
    private final IRecordDescriptorProvider recordDescProvider;
    private final TreeIndexOp op;
    private final PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();
    private ByteBuffer writeBuffer;
    private BTreeOpContext opCtx;

    public BTreeInsertUpdateDeleteOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc,
            IHyracksStageletContext ctx, int partition, int[] fieldPermutation,
            IRecordDescriptorProvider recordDescProvider, TreeIndexOp op) {
        btreeOpHelper = new BTreeOpHelper(opDesc, ctx, partition, BTreeOpHelper.BTreeMode.OPEN_BTREE);
        this.recordDescProvider = recordDescProvider;
        this.op = op;
        tuple.setFieldPermutation(fieldPermutation);
    }

    @Override
    public void open() throws HyracksDataException {
    	AbstractBTreeOperatorDescriptor opDesc = btreeOpHelper.getOperatorDescriptor();
    	RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
    	accessor = new FrameTupleAccessor(btreeOpHelper.getHyracksStageletContext().getFrameSize(), inputRecDesc);
    	writeBuffer = btreeOpHelper.getHyracksStageletContext().allocateFrame();
    	try {
    		btreeOpHelper.init();
    		btreeOpHelper.getBTree().open(btreeOpHelper.getBTreeFileId());
    		opCtx = btreeOpHelper.getBTree().createOpContext(op, btreeOpHelper.getLeafFrame(),
    				btreeOpHelper.getInteriorFrame(), new LIFOMetaDataFrame());
    	} catch(Exception e) {
    		// cleanup in case of failure
    		btreeOpHelper.deinit();
    		throw new HyracksDataException(e);
    	}
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        final BTree btree = btreeOpHelper.getBTree();
        accessor.reset(buffer);

        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            tuple.reset(accessor, i);
            try {
                switch (op) {

                    case TI_INSERT: {
                        btree.insert(tuple, opCtx);
                    }
                        break;

                    case TI_DELETE: {
                        btree.delete(tuple, opCtx);
                    }
                        break;

                    default: {
                        throw new HyracksDataException("Unsupported operation " + op
                                + " in BTree InsertUpdateDelete operator");
                    }

                }

            } catch (Exception e) {
                e.printStackTrace();
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
            btreeOpHelper.deinit();
        }
    }

    @Override
    public void flush() throws HyracksDataException {
    }
}