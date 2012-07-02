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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;

public class TreeIndexBulkLoadOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
    private float fillFactor;
    private final TreeIndexDataflowHelper treeIndexHelper;
    private FrameTupleAccessor accessor;
    private IIndexBulkLoader bulkLoader;
    private ITreeIndex treeIndex;

    private IRecordDescriptorProvider recordDescProvider;

    private PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();

    public TreeIndexBulkLoadOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, int[] fieldPermutation, float fillFactor, IRecordDescriptorProvider recordDescProvider) {
        treeIndexHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition);
        this.fillFactor = fillFactor;
        this.recordDescProvider = recordDescProvider;
        tuple.setFieldPermutation(fieldPermutation);
    }

    @Override
    public void open() throws HyracksDataException {
        AbstractTreeIndexOperatorDescriptor opDesc = (AbstractTreeIndexOperatorDescriptor) treeIndexHelper
                .getOperatorDescriptor();
        RecordDescriptor recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
        accessor = new FrameTupleAccessor(treeIndexHelper.getHyracksTaskContext().getFrameSize(), recDesc);
        try {
            treeIndexHelper.init(false);
            treeIndex = (ITreeIndex) treeIndexHelper.getIndex();
            bulkLoader = treeIndex.createBulkLoader(fillFactor);
        } catch (Exception e) {
            // cleanup in case of failure
            System.out.println("help");
            treeIndexHelper.deinit();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            tuple.reset(accessor, i);
            bulkLoader.add(tuple);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            bulkLoader.end();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            treeIndexHelper.deinit();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
    }
}