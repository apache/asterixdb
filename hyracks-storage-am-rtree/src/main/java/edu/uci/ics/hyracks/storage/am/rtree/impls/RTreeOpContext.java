package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeCursor;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;

public final class RTreeOpContext {
    public final IndexOp op;
    public final IRTreeFrame interiorFrame;
    public final IRTreeFrame leafFrame;
    public IRTreeCursor cursor;
    public final ITreeIndexMetaDataFrame metaFrame;
    public final RTreeSplitKey splitKey;
    public final SpatialUtils spatialUtils;
    public ITupleReference tuple;
    public TupleEntryArrayList tupleEntries1; // used for split and checking enlargement
    public TupleEntryArrayList tupleEntries2; // used for split
    public final PathList pathList; // used to record the pageIds and pageLsns of the visited pages 
    public final TraverseList traverseList; // used for traversing the tree
    public Rectangle[] rec;
    public String threadName; // for debugging

    public RTreeOpContext(IndexOp op, IRTreeFrame interiorFrame, IRTreeFrame leafFrame,
            ITreeIndexMetaDataFrame metaFrame, int treeHeightHint, int dim, String threadName) {
        this.op = op;
        this.interiorFrame = interiorFrame;
        this.leafFrame = leafFrame;
        this.metaFrame = metaFrame;
        splitKey = new RTreeSplitKey(interiorFrame.getTupleWriter().createTupleReference(), interiorFrame
                .getTupleWriter().createTupleReference());
        spatialUtils = new SpatialUtils();
        // TODO: find a better way to know number of entries per node
        tupleEntries1 = new TupleEntryArrayList(100, 100, spatialUtils);
        tupleEntries2 = new TupleEntryArrayList(100, 100, spatialUtils);
        pathList = new PathList(treeHeightHint, treeHeightHint);
        traverseList = new TraverseList(100, 100);
        rec = new Rectangle[4];
        for (int i = 0; i < 4; i++) {
            rec[i] = new Rectangle(dim);
        }
        this.threadName = threadName;
    }

    public ITupleReference getTuple() {
        return tuple;
    }

    public void setTuple(ITupleReference tuple) {
        this.tuple = tuple;
    }

    public void reset() {
        if (tupleEntries1 != null) {
            tupleEntries1.clear();
        }
        if (tupleEntries2 != null) {
            tupleEntries2.clear();
        }
        if (pathList != null) {
            pathList.clear();
        }
        if (traverseList != null) {
            traverseList.clear();
        }
    }
}
