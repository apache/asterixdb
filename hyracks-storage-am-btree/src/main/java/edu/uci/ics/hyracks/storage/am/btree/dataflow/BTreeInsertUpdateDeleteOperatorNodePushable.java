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

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.MetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOp;

public class BTreeInsertUpdateDeleteOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private final BTreeOpHelper btreeOpHelper;

    private FrameTupleAccessor accessor;

    private IRecordDescriptorProvider recordDescProvider;

    private IBTreeMetaDataFrame metaFrame;

    private BTreeOp op;

    private PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();

    public BTreeInsertUpdateDeleteOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc, IHyracksContext ctx,
            int[] fieldPermutation, IRecordDescriptorProvider recordDescProvider, BTreeOp op) {
        btreeOpHelper = new BTreeOpHelper(opDesc, ctx, false);
        this.recordDescProvider = recordDescProvider;
        this.op = op;
        tuple.setFieldPermutation(fieldPermutation);
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        final BTree btree = btreeOpHelper.getBTree();
        final IBTreeLeafFrame leafFrame = btreeOpHelper.getLeafFrame();
        final IBTreeInteriorFrame interiorFrame = btreeOpHelper.getInteriorFrame();

        accessor.reset(buffer);

        System.out.println("TUPLECOUNT: " + accessor.getTupleCount());
        
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            tuple.reset(accessor, i);
            try {

                switch (op) {

                    case BTO_INSERT: {
                        btree.insert(tuple, leafFrame, interiorFrame, metaFrame);
                    }
                        break;

                    case BTO_DELETE: {
                        btree.delete(tuple, leafFrame, interiorFrame, metaFrame);
                    }
                        break;

                    default: {
                        throw new HyracksDataException("Unsupported operation " + op + " in BTree InsertUpdateDelete operator");
                    }

                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // pass a copy of the frame to next op
        FrameUtils.flushFrame(buffer.duplicate(), writer);
    }

    @Override
    public void open() throws HyracksDataException {
        AbstractBTreeOperatorDescriptor opDesc = btreeOpHelper.getOperatorDescriptor();
        RecordDescriptor recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
        accessor = new FrameTupleAccessor(btreeOpHelper.getHyracksContext(), recDesc);
        try {
            btreeOpHelper.init();
            btreeOpHelper.getBTree().open(btreeOpHelper.getBTreeFileId());
            metaFrame = new MetaDataFrame();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() throws HyracksDataException {
    }
}