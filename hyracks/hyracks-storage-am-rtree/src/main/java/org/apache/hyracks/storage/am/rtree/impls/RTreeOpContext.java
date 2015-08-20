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

package edu.uci.ics.hyracks.storage.am.rtree.impls;

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class RTreeOpContext implements IIndexOperationContext {
    private static final int INITIAL_TRAVERSE_LIST_SIZE = 100;
    private static final int INITIAL_HEIGHT = 8;
    public final MultiComparator cmp;
    public final IRTreeInteriorFrame interiorFrame;
    public final IRTreeLeafFrame leafFrame;
    public IndexOperation op;
    public ITreeIndexCursor cursor;
    public RTreeCursorInitialState cursorInitialState;
    public ITreeIndexMetaDataFrame metaFrame;
    public RTreeSplitKey splitKey;
    public ITupleReference tuple;
    // Used to record the pageIds and pageLsns of the visited pages.
    public PathList pathList;
    // Used for traversing the tree.
    public PathList traverseList;

    public ArrayList<ICachedPage> NSNUpdates;
    public ArrayList<ICachedPage> LSNUpdates;

    public final IModificationOperationCallback modificationCallback;

    public RTreeOpContext(IRTreeLeafFrame leafFrame, IRTreeInteriorFrame interiorFrame,
            ITreeIndexMetaDataFrame metaFrame, IBinaryComparatorFactory[] cmpFactories,
            IModificationOperationCallback modificationCallback) {

        if (cmpFactories[0] != null) {
            this.cmp = MultiComparator.create(cmpFactories);
        } else {
            this.cmp = null;
        }

        this.interiorFrame = interiorFrame;
        this.leafFrame = leafFrame;
        this.metaFrame = metaFrame;
        this.modificationCallback = modificationCallback;
        pathList = new PathList(INITIAL_HEIGHT, INITIAL_HEIGHT);
        NSNUpdates = new ArrayList<ICachedPage>();
        LSNUpdates = new ArrayList<ICachedPage>();
    }

    public ITupleReference getTuple() {
        return tuple;
    }

    public void setTuple(ITupleReference tuple) {
        this.tuple = tuple;
    }

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
                splitKey = new RTreeSplitKey(interiorFrame.getTupleWriter().createTupleReference(), interiorFrame
                        .getTupleWriter().createTupleReference());
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
}
