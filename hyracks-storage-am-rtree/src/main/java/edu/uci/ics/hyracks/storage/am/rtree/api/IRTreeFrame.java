package edu.uci.ics.hyracks.storage.am.rtree.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSplitKey;

public interface IRTreeFrame extends ITreeIndexFrame {

    public int getChildPageId(ITupleReference tuple, MultiComparator cmp);
    
    public int split(IRTreeFrame rightFrame, ITupleReference tuple, MultiComparator cmp, RTreeSplitKey leftSplitKey,
            RTreeSplitKey rightSplitKey) throws Exception;
    
    public void adjustNode(ITreeIndexTupleReference[] tuples, MultiComparator cmp);

    public void adjustTuple(ITupleReference tuple, MultiComparator cmp);
}
