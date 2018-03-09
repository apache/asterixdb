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

package org.apache.hyracks.storage.am.rtree.impls;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import org.apache.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

public class RTreeOpContext implements IIndexOperationContext, IExtraPageBlockHelper {
    private static final int INITIAL_TRAVERSE_LIST_SIZE = 100;
    private static final int INITIAL_HEIGHT = 8;
    private final MultiComparator cmp;
    private final IRTreeInteriorFrame interiorFrame;
    private final IRTreeLeafFrame leafFrame;
    private IndexOperation op;
    private ITreeIndexCursor cursor;
    private RTreeCursorInitialState cursorInitialState;
    private final IPageManager freePageManager;
    private final ITreeIndexMetadataFrame metaFrame;
    private RTreeSplitKey splitKey;
    private ITupleReference tuple;
    // Used to record the pageIds and pageLsns of the visited pages.
    private PathList pathList;
    // Used for traversing the tree.
    private PathList traverseList;

    private ArrayList<ICachedPage> NSNUpdates;
    private ArrayList<ICachedPage> LSNUpdates;

    private IModificationOperationCallback modificationCallback;

    private boolean destroyed = false;

    public RTreeOpContext(IRTreeLeafFrame leafFrame, IRTreeInteriorFrame interiorFrame, IPageManager freePageManager,
            IBinaryComparatorFactory[] cmpFactories, IModificationOperationCallback modificationCallback) {

        if (cmpFactories[0] != null) {
            this.cmp = MultiComparator.create(cmpFactories);
        } else {
            this.cmp = null;
        }

        this.interiorFrame = interiorFrame;
        this.leafFrame = leafFrame;
        this.freePageManager = freePageManager;
        this.metaFrame = freePageManager.createMetadataFrame();
        this.modificationCallback = modificationCallback;
        pathList = new PathList(INITIAL_HEIGHT, INITIAL_HEIGHT);
        NSNUpdates = new ArrayList<>();
        LSNUpdates = new ArrayList<>();
    }

    public ITupleReference getTuple() {
        return tuple;
    }

    public void setTuple(ITupleReference tuple) {
        this.tuple = tuple;
    }

    @Override
    public void reset() {
        if (pathList != null) {
            pathList.clear();
        }
        if (traverseList != null) {
            traverseList.clear();
        }
        NSNUpdates.clear();
        LSNUpdates.clear();
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        if (op != null && newOp == op) {
            return;
        }
        if (op != IndexOperation.SEARCH && op != IndexOperation.DISKORDERSCAN) {
            if (splitKey == null) {
                splitKey = new RTreeSplitKey(interiorFrame.getTupleWriter().createTupleReference(),
                        interiorFrame.getTupleWriter().createTupleReference());
            }
            if (traverseList == null) {
                traverseList = new PathList(INITIAL_TRAVERSE_LIST_SIZE, INITIAL_TRAVERSE_LIST_SIZE);
            }
        }
        if (cursorInitialState == null) {
            cursorInitialState = new RTreeCursorInitialState(pathList, 1);
        }
        this.op = newOp;
    }

    @Override
    public IndexOperation getOperation() {
        return op;
    }

    public void setModificationCallback(IModificationOperationCallback modificationCallback) {
        this.modificationCallback = modificationCallback;
    }

    @Override
    public int getFreeBlock(int size) throws HyracksDataException {
        return freePageManager.takeBlock(metaFrame, size);
    }

    @Override
    public void returnFreePageBlock(int blockPageId, int size) throws HyracksDataException {
        freePageManager.releaseBlock(metaFrame, blockPageId, size);
    }

    public IRTreeInteriorFrame getInteriorFrame() {
        return interiorFrame;
    }

    public PathList getPathList() {
        return pathList;
    }

    public MultiComparator getCmp() {
        return cmp;
    }

    public IRTreeLeafFrame getLeafFrame() {
        return leafFrame;
    }

    public IModificationOperationCallback getModificationCallback() {
        return modificationCallback;
    }

    public List<ICachedPage> getLSNUpdates() {
        return LSNUpdates;
    }

    public RTreeSplitKey getSplitKey() {
        return splitKey;
    }

    public ITreeIndexMetadataFrame getMetaFrame() {
        return metaFrame;
    }

    public List<ICachedPage> getNSNUpdates() {
        return NSNUpdates;
    }

    public PathList getTraverseList() {
        return traverseList;
    }

    public ITreeIndexCursor getCursor() {
        return cursor;
    }

    public void setCursor(ITreeIndexCursor cursor) {
        this.cursor = cursor;
    }

    public RTreeCursorInitialState getCursorInitialState() {
        return cursorInitialState;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (destroyed) {
            return;
        }
        destroyed = true;
        if (cursor != null) {
            cursor.destroy();
        }
    }
}
