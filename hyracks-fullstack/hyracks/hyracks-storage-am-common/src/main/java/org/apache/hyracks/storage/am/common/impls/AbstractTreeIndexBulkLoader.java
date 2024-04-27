/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.storage.am.common.impls;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.PageWriteFailureCallback;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public abstract class AbstractTreeIndexBulkLoader extends PageWriteFailureCallback implements IIndexBulkLoader {
    protected final IBufferCache bufferCache;
    protected final IPageManager freePageManager;
    protected final AbstractTreeIndex treeIndex;
    protected final int fileId;
    protected final MultiComparator cmp;
    protected final int slotSize;
    protected final int leafMaxBytes;
    protected final int interiorMaxBytes;
    protected final ArrayList<NodeFrontier> nodeFrontiers = new ArrayList<>();
    protected final ITreeIndexMetadataFrame metaFrame;
    protected final ITreeIndexTupleWriter tupleWriter;
    protected ITreeIndexFrame leafFrame;
    protected ITreeIndexFrame interiorFrame;
    // Immutable bulk loaders write their root page at page -2, as needed e.g. by append-only file systems such as
    // HDFS.  Since loading this tree relies on the root page actually being at that point, no further inserts into
    // that tree are allowed.  Currently, this is not enforced.
    protected boolean releasedLatches;
    private final IFIFOPageWriter pageWriter;
    protected List<ICachedPage> pagesToWrite;
    private final ICompressedPageWriter compressedPageWriter;

    protected AbstractTreeIndexBulkLoader(float fillFactor, IPageWriteCallback callback, ITreeIndex index)
            throws HyracksDataException {
        this(fillFactor, callback, index, index.getLeafFrameFactory().createFrame(),
                DefaultBufferCacheWriteContext.INSTANCE);
    }

    protected AbstractTreeIndexBulkLoader(float fillFactor, IPageWriteCallback callback, ITreeIndex index,
            ITreeIndexFrame leafFrame, IBufferCacheWriteContext writeContext) throws HyracksDataException {
        this.bufferCache = index.getBufferCache();
        this.freePageManager = index.getPageManager();
        this.fileId = index.getFileId();
        this.treeIndex = (AbstractTreeIndex) index;
        this.leafFrame = leafFrame;
        interiorFrame = treeIndex.getInteriorFrameFactory().createFrame();
        metaFrame = freePageManager.createMetadataFrame();

        pageWriter = bufferCache.createFIFOWriter(callback, this, writeContext);

        if (!treeIndex.isEmptyTree(leafFrame)) {
            throw HyracksDataException.create(ErrorCode.CANNOT_BULK_LOAD_NON_EMPTY_TREE);
        }

        this.cmp = MultiComparator.create(treeIndex.getCmpFactories());

        leafFrame.setMultiComparator(cmp);
        interiorFrame.setMultiComparator(cmp);

        tupleWriter = leafFrame.getTupleWriter();
        NodeFrontier leafFrontier = new NodeFrontier(createTupleReference());
        leafFrontier.pageId = freePageManager.takePage(metaFrame);
        leafFrontier.page = bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId));

        interiorFrame.setPage(leafFrontier.page);
        interiorFrame.initBuffer((byte) 0);
        interiorMaxBytes = (int) (interiorFrame.getBuffer().capacity() * fillFactor);

        leafFrame.setPage(leafFrontier.page);
        leafFrame.initBuffer((byte) 0);
        leafMaxBytes = (int) (leafFrame.getBuffer().capacity() * fillFactor);
        slotSize = leafFrame.getSlotSize();

        nodeFrontiers.add(leafFrontier);
        pagesToWrite = new ArrayList<>();
        compressedPageWriter = bufferCache.getCompressedPageWriter(fileId);
    }

    protected ITreeIndexTupleReference createTupleReference() {
        return leafFrame.createTupleReference();
    }

    protected void handleException() {
        // Unlatch and unpin pages that weren't in the queue to avoid leaking memory.
        compressedPageWriter.abort();
        for (NodeFrontier nodeFrontier : nodeFrontiers) {
            if (nodeFrontier != null && nodeFrontier.page != null) {
                ICachedPage frontierPage = nodeFrontier.page;
                if (frontierPage.confiscated()) {
                    bufferCache.returnPage(frontierPage, false);
                }
            }
        }
        for (ICachedPage pageToDiscard : pagesToWrite) {
            if (pageToDiscard != null) {
                bufferCache.returnPage(pageToDiscard, false);
            }
        }
        releasedLatches = true;
    }

    @Override
    public void end() throws HyracksDataException {
        if (hasFailed()) {
            throw HyracksDataException.create(getFailure());
        }
        freePageManager.setRootPageId(treeIndex.getRootPageId());
    }

    protected void setRootPageId(int rootPage) {
        treeIndex.rootPage = rootPage;
    }

    protected void addLevel() throws HyracksDataException {
        NodeFrontier frontier = new NodeFrontier(tupleWriter.createTupleReference());
        frontier.page = bufferCache.confiscatePage(IBufferCache.INVALID_DPID);
        frontier.pageId = -1;
        frontier.lastTuple.setFieldCount(cmp.getKeyFieldCount());
        interiorFrame.setPage(frontier.page);
        interiorFrame.initBuffer((byte) nodeFrontiers.size());
        nodeFrontiers.add(frontier);
    }

    public ITreeIndexFrame getLeafFrame() {
        return leafFrame;
    }

    public void setLeafFrame(ITreeIndexFrame leafFrame) {
        this.leafFrame = leafFrame;
    }

    public void write(ICachedPage cPage) throws HyracksDataException {
        compressedPageWriter.prepareWrite(cPage);
        pageWriter.write(cPage);
    }

    @Override
    public void force() throws HyracksDataException {
        bufferCache.force(fileId, false);
    }
}
