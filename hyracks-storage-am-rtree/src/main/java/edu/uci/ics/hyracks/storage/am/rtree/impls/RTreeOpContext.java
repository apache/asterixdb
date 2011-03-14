package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.TreeIndexOp;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;

public final class RTreeOpContext {
    public final TreeIndexOp op;
    public final IRTreeFrame interiorFrame;
    public final IRTreeFrame leafFrame;
    public final ITreeIndexMetaDataFrame metaFrame;
    public final ByteArrayList overflowArray;
    public final RTreeSplitKey leftSplitKey;
    public final RTreeSplitKey rightSplitKey;
    public int insertLevel;
    public ITupleReference tuple;

    public RTreeOpContext(TreeIndexOp op, IRTreeFrame interiorFrame, IRTreeFrame leafFrame,
            ITreeIndexMetaDataFrame metaFrame, int treeHeightHint) {
        this.op = op;
        this.interiorFrame = interiorFrame;
        this.leafFrame = leafFrame;
        this.metaFrame = metaFrame;
        leftSplitKey = new RTreeSplitKey(leafFrame.getTupleWriter().createTupleReference());
        rightSplitKey = new RTreeSplitKey(leafFrame.getTupleWriter().createTupleReference());
        overflowArray = new ByteArrayList(treeHeightHint, treeHeightHint);
    }

    public ITupleReference getTuple() {
        return tuple;
    }

    public void setTuple(ITupleReference tuple) {
        this.tuple = tuple;
    }

    public void reset() {
        if (overflowArray != null)
            overflowArray.clear();
    }
}
