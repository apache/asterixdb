/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.pregelix.dataflow.std;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class TreeIndexBulkReLoadOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
    private final float fillFactor;
    private final IndexDataflowHelper treeIndexOpHelper;
    private final IIndexOperatorDescriptor opDesc;
    private final IRecordDescriptorProvider recordDescProvider;
    private final PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();

    private ITreeIndex index;
    private FrameTupleAccessor accessor;
    private IIndexBulkLoader bulkLoader;

    public TreeIndexBulkReLoadOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, int[] fieldPermutation, float fillFactor, IRecordDescriptorProvider recordDescProvider,
            IStorageManagerInterface storageManager, IIndexLifecycleManagerProvider lcManagerProvider,
            IFileSplitProvider fileSplitProvider) {
        this.fillFactor = fillFactor;
        treeIndexOpHelper = (IndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition);
        this.opDesc = opDesc;
        this.recordDescProvider = recordDescProvider;
        tuple.setFieldPermutation(fieldPermutation);
    }

    @Override
    public void open() throws HyracksDataException {
        RecordDescriptor recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        accessor = new FrameTupleAccessor(treeIndexOpHelper.getTaskContext().getFrameSize(), recDesc);
        treeIndexOpHelper.destroy();
        treeIndexOpHelper.create();
        treeIndexOpHelper.open();
        try {
            index = (ITreeIndex) treeIndexOpHelper.getIndexInstance();
            bulkLoader = index.createBulkLoader(fillFactor, false, 0, false);
        } catch (Exception e) {
            // cleanup in case of failure
            treeIndexOpHelper.close();
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
                bulkLoader.add(tuple);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            bulkLoader.end();
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        } finally {
            treeIndexOpHelper.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        try {
            bulkLoader.end();
        } catch (IndexException e) {
            treeIndexOpHelper.close();
            throw new HyracksDataException(e);
        } 
    }
}