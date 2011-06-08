package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOpContext;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;

public final class RTreeOpContext implements IndexOpContext {
    public final IndexOp op;
    public final IRTreeFrame interiorFrame;
    public final IRTreeFrame leafFrame;
    public ITreeIndexCursor cursor;
    public CursorInitialState cursorInitialState;
    public final ITreeIndexMetaDataFrame metaFrame;
    public final RTreeSplitKey splitKey;
    public ITupleReference tuple;
    public final PathList pathList; // used to record the pageIds and pageLsns
                                    // of the visited pages
    public final TraverseList traverseList; // used for traversing the tree

    public RTreeOpContext(IndexOp op, IRTreeFrame leafFrame, IRTreeFrame interiorFrame,
            ITreeIndexMetaDataFrame metaFrame, int treeHeightHint) {
        this.op = op;
        this.interiorFrame = interiorFrame;
        this.leafFrame = leafFrame;
        this.metaFrame = metaFrame;
        pathList = new PathList(treeHeightHint, treeHeightHint);
        if (op != IndexOp.SEARCH && op != IndexOp.DISKORDERSCAN) {
            splitKey = new RTreeSplitKey(interiorFrame.getTupleWriter().createTupleReference(), interiorFrame
                    .getTupleWriter().createTupleReference());
            traverseList = new TraverseList(100, 100);
        } else {
            splitKey = null;
            traverseList = null;
            cursorInitialState = new CursorInitialState(pathList, 1);
        }
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
    }
}
