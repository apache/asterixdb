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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.MetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;

public class BTreeBulkLoadOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
    private float fillFactor;
    private final BTreeOpHelper btreeOpHelper;
    private FrameTupleAccessor accessor;
    private BTree.BulkLoadContext bulkLoadCtx;
    
    private IRecordDescriptorProvider recordDescProvider;

    private PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();
    
    public BTreeBulkLoadOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc, IHyracksContext ctx,
            int partition, int[] fieldPermutation, float fillFactor, IRecordDescriptorProvider recordDescProvider) {
        btreeOpHelper = new BTreeOpHelper(opDesc, ctx, partition, BTreeOpHelper.BTreeMode.CREATE_BTREE);
        this.fillFactor = fillFactor;
        this.recordDescProvider = recordDescProvider;
        tuple.setFieldPermutation(fieldPermutation);
    }
    
    @Override
    public void open() throws HyracksDataException {
        AbstractBTreeOperatorDescriptor opDesc = btreeOpHelper.getOperatorDescriptor();
        RecordDescriptor recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);        
        accessor = new FrameTupleAccessor(btreeOpHelper.getHyracksContext(), recDesc);
        IBTreeMetaDataFrame metaFrame = new MetaDataFrame();        
        btreeOpHelper.init();
        btreeOpHelper.getBTree().open(btreeOpHelper.getBTreeFileId());        
        try {
			bulkLoadCtx = btreeOpHelper.getBTree().beginBulkLoad(fillFactor, btreeOpHelper.getLeafFrame(), btreeOpHelper.getInteriorFrame(), metaFrame);
		} catch (Exception e) {
			throw new HyracksDataException(e);
		}
    }
    
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);

        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            tuple.reset(accessor, i);
            try {
                btreeOpHelper.getBTree().bulkLoadAddTuple(bulkLoadCtx, tuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    @Override
    public void close() throws HyracksDataException {
        try {
            btreeOpHelper.getBTree().endBulkLoad(bulkLoadCtx);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void flush() throws HyracksDataException {
    }
}