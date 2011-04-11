package edu.uci.ics.hyracks.storage.am.rtree.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.impls.Rectangle;
import edu.uci.ics.hyracks.storage.am.rtree.impls.TupleEntryArrayList;

public interface IRTreeFrame extends ITreeIndexFrame {

    public int getRightPage();

    public void setRightPage(int rightPage);

    public int getChildPageId(ITupleReference tuple, TupleEntryArrayList entries, ITreeIndexTupleReference[] nodesMBRs,
            MultiComparator cmp);

    public void adjustNodeMBR(ITreeIndexTupleReference[] tuples, MultiComparator cmp);

    public void adjustKey(ITupleReference tuple, MultiComparator cmp);

    public int split(ITreeIndexFrame rightFrame, ITupleReference tuple, MultiComparator cmp, ISplitKey splitKey,
            TupleEntryArrayList entries1, TupleEntryArrayList entries2, Rectangle[] rec) throws Exception;

    public void reinsert(ITupleReference tuple, ITreeIndexTupleReference nodeMBR, TupleEntryArrayList entries,
            ISplitKey splitKey, MultiComparator cmp) throws Exception;

    public Rectangle intersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp);

    public int getChildPageIdIfIntersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp);
}
