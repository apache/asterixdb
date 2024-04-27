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
package org.apache.hyracks.storage.am.btree.impls;

import java.util.List;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.common.api.ISplitKey;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndexBulkLoader;
import org.apache.hyracks.storage.am.common.impls.NodeFrontier;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.JSONUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class BTreeNSMBulkLoader extends AbstractTreeIndexBulkLoader {
    private static final Logger LOGGER = LogManager.getLogger();
    protected final ISplitKey splitKey;
    protected final boolean verifyInput;
    private final int maxTupleSize;

    public BTreeNSMBulkLoader(float fillFactor, boolean verifyInput, IPageWriteCallback callback, ITreeIndex index)
            throws HyracksDataException {
        this(fillFactor, verifyInput, callback, index, index.getLeafFrameFactory().createFrame(),
                DefaultBufferCacheWriteContext.INSTANCE);
    }

    protected BTreeNSMBulkLoader(float fillFactor, boolean verifyInput, IPageWriteCallback callback, ITreeIndex index,
            ITreeIndexFrame leafFrame, IBufferCacheWriteContext writeContext) throws HyracksDataException {
        super(fillFactor, callback, index, leafFrame, writeContext);
        this.verifyInput = verifyInput;
        splitKey = new BTreeSplitKey(tupleWriter.createTupleReference());
        splitKey.getTuple().setFieldCount(cmp.getKeyFieldCount());
        maxTupleSize = ((BTree) index).maxTupleSize;
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        try {
            int tupleSize = Math.max(leafFrame.getBytesRequiredToWriteTuple(tuple),
                    interiorFrame.getBytesRequiredToWriteTuple(tuple));
            NodeFrontier leafFrontier = nodeFrontiers.get(0);
            int spaceNeeded = tupleWriter.bytesRequired(tuple) + slotSize;
            int spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();

            // try to free space by compression
            if (spaceUsed + spaceNeeded > leafMaxBytes) {
                leafFrame.compress();
                spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();
            }
            //full, allocate new page
            if (spaceUsed + spaceNeeded > leafMaxBytes) {
                if (leafFrame.getTupleCount() == 0) {
                    //The current page is empty. Return it.
                    bufferCache.returnPage(leafFrontier.page, false);
                } else {
                    leafFrontier.lastTuple.resetByTupleIndex(leafFrame, leafFrame.getTupleCount() - 1);
                    if (verifyInput) {
                        verifyInputTuple(tuple, leafFrontier.lastTuple);
                    }
                    //The current page is not empty. Write it.
                    writeFullLeafPage();
                }
                if (tupleSize > maxTupleSize) {
                    //We need a large page
                    final long dpid = BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId);
                    // calculate required number of pages.
                    int headerSize = Math.max(leafFrame.getPageHeaderSize(), interiorFrame.getPageHeaderSize());
                    final int multiplier =
                            (int) Math.ceil((double) tupleSize / (bufferCache.getPageSize() - headerSize));
                    if (multiplier > 1) {
                        leafFrontier.page = bufferCache.confiscateLargePage(dpid, multiplier,
                                freePageManager.takeBlock(metaFrame, multiplier - 1));
                    } else {
                        leafFrontier.page = bufferCache.confiscatePage(dpid);
                    }
                    leafFrame.setPage(leafFrontier.page);
                    leafFrame.initBuffer((byte) 0);
                    ((IBTreeLeafFrame) leafFrame).setLargeFlag(true);
                } else {
                    //allocate a new page
                    confiscateNewLeafPage();
                }
            } else {
                if (verifyInput && leafFrame.getTupleCount() > 0) {
                    leafFrontier.lastTuple.resetByTupleIndex(leafFrame, leafFrame.getTupleCount() - 1);
                    verifyInputTuple(tuple, leafFrontier.lastTuple);
                }
            }
            ((IBTreeLeafFrame) leafFrame).insertSorted(tuple);
        } catch (HyracksDataException | RuntimeException e) {
            logState(tuple, e);
            handleException();
            throw e;
        }
    }

    protected void verifyInputTuple(ITupleReference tuple, ITupleReference prevTuple) throws HyracksDataException {
        // New tuple should be strictly greater than last tuple.
        int cmpResult = cmp.compare(tuple, prevTuple);
        if (cmpResult < 0) {
            throw HyracksDataException.create(ErrorCode.UNSORTED_LOAD_INPUT);
        }
        if (cmpResult == 0) {
            throw HyracksDataException.create(ErrorCode.DUPLICATE_LOAD_INPUT);
        }
    }

    protected void propagateBulk(int level, List<ICachedPage> pagesToWrite) throws HyracksDataException {
        if (splitKey.getBuffer() == null) {
            return;
        }

        if (level >= nodeFrontiers.size()) {
            addLevel();
        }

        NodeFrontier frontier = nodeFrontiers.get(level);
        interiorFrame.setPage(frontier.page);

        ITupleReference tuple = splitKey.getTuple();
        int tupleBytes = tupleWriter.bytesRequired(tuple, 0, cmp.getKeyFieldCount());
        int spaceNeeded = tupleBytes + slotSize + 4;
        if (tupleBytes > interiorFrame.getMaxTupleSize(bufferCache.getPageSize())) {
            throw HyracksDataException.create(ErrorCode.RECORD_IS_TOO_LARGE, tupleBytes,
                    interiorFrame.getMaxTupleSize(bufferCache.getPageSize()));
        }

        int spaceUsed = interiorFrame.getBuffer().capacity() - interiorFrame.getTotalFreeSpace();
        if (spaceUsed + spaceNeeded > interiorMaxBytes) {
            ISplitKey copyKey = splitKey.duplicate(tupleWriter.createTupleReference());
            tuple = copyKey.getTuple();

            frontier.lastTuple.resetByTupleIndex(interiorFrame, interiorFrame.getTupleCount() - 1);
            int splitKeySize = tupleWriter.bytesRequired(frontier.lastTuple, 0, cmp.getKeyFieldCount());
            splitKey.initData(splitKeySize);
            tupleWriter.writeTupleFields(frontier.lastTuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer().array(),
                    0);
            splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer().array(), 0);

            ((IBTreeInteriorFrame) interiorFrame).deleteGreatest();
            int finalPageId = freePageManager.takePage(metaFrame);
            frontier.page.setDiskPageId(BufferedFileHandle.getDiskPageId(fileId, finalPageId));
            pagesToWrite.add(frontier.page);
            splitKey.setLeftPage(finalPageId);

            propagateBulk(level + 1, pagesToWrite);
            frontier.page = bufferCache.confiscatePage(IBufferCache.INVALID_DPID);
            interiorFrame.setPage(frontier.page);
            interiorFrame.initBuffer((byte) level);
        }
        ((IBTreeInteriorFrame) interiorFrame).insertSorted(tuple);
    }

    private void persistFrontiers(int level, int rightPage) throws HyracksDataException {
        if (level >= nodeFrontiers.size()) {
            setRootPageId(nodeFrontiers.get(level - 1).pageId);
            releasedLatches = true;
            return;
        }
        if (level < 1) {
            ICachedPage lastLeaf = nodeFrontiers.get(level).page;
            int lastLeafPage = nodeFrontiers.get(level).pageId;
            lastLeaf.setDiskPageId(BufferedFileHandle.getDiskPageId(fileId, nodeFrontiers.get(level).pageId));
            writeLastLeaf(lastLeaf);
            nodeFrontiers.get(level).page = null;
            persistFrontiers(level + 1, lastLeafPage);
            return;
        }
        NodeFrontier frontier = nodeFrontiers.get(level);
        interiorFrame.setPage(frontier.page);
        //just finalize = the layer right above the leaves has correct righthand pointers already
        if (rightPage < 0) {
            throw new HyracksDataException("Error in index creation. Internal node appears to have no rightmost guide");
        }
        ((IBTreeInteriorFrame) interiorFrame).setRightmostChildPageId(rightPage);
        int finalPageId = freePageManager.takePage(metaFrame);
        frontier.page.setDiskPageId(BufferedFileHandle.getDiskPageId(fileId, finalPageId));
        write(frontier.page);
        frontier.pageId = finalPageId;
        persistFrontiers(level + 1, finalPageId);
    }

    @Override
    public void end() throws HyracksDataException {
        try {
            persistFrontiers(0, -1);
            super.end();
        } catch (HyracksDataException | RuntimeException e) {
            handleException();
            throw e;
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        super.handleException();
    }

    protected void writeFullLeafPage() throws HyracksDataException {
        final NodeFrontier leafFrontier = nodeFrontiers.get(0);
        leafFrontier.lastTuple.resetByTupleIndex(leafFrame, leafFrame.getTupleCount() - 1);
        final int splitKeySize = tupleWriter.bytesRequired(leafFrontier.lastTuple, 0, cmp.getKeyFieldCount());
        splitKey.initData(splitKeySize);
        tupleWriter.writeTupleFields(leafFrontier.lastTuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer().array(),
                0);
        splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer().array(), 0);
        splitKey.setLeftPage(leafFrontier.pageId);

        propagateBulk(1, pagesToWrite);

        leafFrontier.pageId = freePageManager.takePage(metaFrame);

        ((IBTreeLeafFrame) leafFrame).setNextLeaf(leafFrontier.pageId);

        write(leafFrontier.page);
        for (ICachedPage c : pagesToWrite) {
            write(c);
        }
        pagesToWrite.clear();
        splitKey.setRightPage(leafFrontier.pageId);
    }

    protected void writeLastLeaf(ICachedPage page) throws HyracksDataException {
        write(page);
    }

    protected final void confiscateNewLeafPage() throws HyracksDataException {
        final NodeFrontier leafFrontier = nodeFrontiers.get(0);
        final long dpid = BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId);
        leafFrontier.page = bufferCache.confiscatePage(dpid);
        leafFrame.setPage(leafFrontier.page);
        leafFrame.initBuffer((byte) 0);
    }

    private void logState(ITupleReference tuple, Exception e) {
        try {
            ObjectNode state = JSONUtil.createObject();
            state.set("leafFrame", leafFrame.getState());
            state.set("interiorFrame", interiorFrame.getState());
            int tupleSize = Math.max(leafFrame.getBytesRequiredToWriteTuple(tuple),
                    interiorFrame.getBytesRequiredToWriteTuple(tuple));
            state.put("tupleSize", tupleSize);
            state.put("spaceNeeded", tupleWriter.bytesRequired(tuple) + slotSize);
            state.put("spaceUsed", leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace());
            state.put("leafMaxBytes", leafMaxBytes);
            state.put("maxTupleSize", maxTupleSize);
            LOGGER.error("failed to add tuple {}", state, e);
        } catch (Throwable t) {
            e.addSuppressed(t);
        }
    }
}
