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
package edu.uci.ics.pregelix.dataflow.std;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexRegistry;
import edu.uci.ics.hyracks.storage.am.common.dataflow.PermutingFrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class TreeIndexBulkReLoadOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
    private final TreeIndexDataflowHelper treeIndexOpHelper;
    private FrameTupleAccessor accessor;
    private IIndexBulkLoadContext bulkLoadCtx;

    private IRecordDescriptorProvider recordDescProvider;
    private PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();

    private final IStorageManagerInterface storageManager;
    private final IIndexRegistryProvider<IIndex> treeIndexRegistryProvider;
    private final IFileSplitProvider fileSplitProvider;
    private final int partition;
    private final float fillFactor;
    private IHyracksTaskContext ctx;
    private ITreeIndex index;

    public TreeIndexBulkReLoadOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, int[] fieldPermutation, float fillFactor, IRecordDescriptorProvider recordDescProvider,
            IStorageManagerInterface storageManager, IIndexRegistryProvider<IIndex> treeIndexRegistryProvider,
            IFileSplitProvider fileSplitProvider) {
        treeIndexOpHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition);
        this.recordDescProvider = recordDescProvider;
        tuple.setFieldPermutation(fieldPermutation);

        this.storageManager = storageManager;
        this.treeIndexRegistryProvider = treeIndexRegistryProvider;
        this.fileSplitProvider = fileSplitProvider;
        this.partition = partition;
        this.ctx = ctx;
        this.fillFactor = fillFactor;
    }

    @Override
    public void open() throws HyracksDataException {
        initDrop();
        init();
    }

    private void initDrop() throws HyracksDataException {
        try {
            IndexRegistry<IIndex> treeIndexRegistry = treeIndexRegistryProvider.getRegistry(ctx);
            IBufferCache bufferCache = storageManager.getBufferCache(ctx);
            IFileMapProvider fileMapProvider = storageManager.getFileMapProvider(ctx);

            FileReference f = fileSplitProvider.getFileSplits()[partition].getLocalFile();
            int indexFileId = -1;
            boolean fileIsMapped = false;
            synchronized (fileMapProvider) {
                fileIsMapped = fileMapProvider.isMapped(f);
                if (fileIsMapped)
                    indexFileId = fileMapProvider.lookupFileId(f);
            }

            /**
             * delete the file if it is mapped
             */
            if (fileIsMapped) {
                // Unregister tree instance.
                synchronized (treeIndexRegistry) {
                    treeIndexRegistry.unregister(indexFileId);
                }

                // remove name to id mapping
                bufferCache.deleteFile(indexFileId, false);
            }
        }
        // TODO: for the time being we don't throw,
        // with proper exception handling (no hanging job problem) we should
        // throw
        catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private void init() throws HyracksDataException {
        AbstractTreeIndexOperatorDescriptor opDesc = (AbstractTreeIndexOperatorDescriptor) treeIndexOpHelper
                .getOperatorDescriptor();
        RecordDescriptor recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        accessor = new FrameTupleAccessor(treeIndexOpHelper.getHyracksTaskContext().getFrameSize(), recDesc);
        try {
            treeIndexOpHelper.init(true);
            treeIndexOpHelper.getIndex().open(treeIndexOpHelper.getIndexFileId());
            index = (ITreeIndex) treeIndexOpHelper.getIndex();
            index.open(treeIndexOpHelper.getIndexFileId());
            bulkLoadCtx = index.beginBulkLoad(fillFactor);
        } catch (Exception e) {
            // cleanup in case of failure
            treeIndexOpHelper.deinit();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            tuple.reset(accessor, i);
            index.bulkLoadAddTuple(tuple, bulkLoadCtx);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            index.endBulkLoad(bulkLoadCtx);
        } finally {
            treeIndexOpHelper.deinit();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
    }
}