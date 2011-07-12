package edu.uci.ics.hyracks.storage.am.rtree.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.impls.TraverseList;

public interface IRTreeInteriorFrame extends IRTreeFrame {

    public boolean findBestChild(ITupleReference tuple, MultiComparator cmp);

    public int getBestChildPageId(MultiComparator cmp);

    public int getChildPageIdIfIntersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp);

    public int findTupleByPointer(ITupleReference tuple, MultiComparator cmp);

    public int findTupleByPointer(ITupleReference tuple, TraverseList traverseList, int parentId, MultiComparator cmp);

    public void adjustKey(ITupleReference tuple, int tupleIndex, MultiComparator cmp);

    public boolean recomputeMBR(ITupleReference tuple, int tupleIndex, MultiComparator cmp);

    public void enlarge(ITupleReference tuple, MultiComparator cmp);
}
