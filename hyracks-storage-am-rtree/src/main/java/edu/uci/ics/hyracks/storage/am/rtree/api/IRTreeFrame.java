package edu.uci.ics.hyracks.storage.am.rtree.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.impls.Rectangle;
import edu.uci.ics.hyracks.storage.am.rtree.impls.TraverseList;
import edu.uci.ics.hyracks.storage.am.rtree.impls.TupleEntryArrayList;

public interface IRTreeFrame extends ITreeIndexFrame {
    
    public ITreeIndexTupleReference createTupleReference();
    
    public boolean recomputeMBR(ITupleReference tuple, int tupleIndex, MultiComparator cmp);

    public void computeMBR(ISplitKey splitKey, MultiComparator cmp);
    
    public void delete(int tupleIndex, MultiComparator cmp) throws Exception;
    
    public int findTupleByPointer(int pageId, MultiComparator cmp);
    
    public int findTupleByPointer(ITupleReference tuple, TraverseList traverseList, int parentId, MultiComparator cmp);

    public int findTupleByPointer(ITupleReference tuple, MultiComparator cmp);
    
    public int findTupleIndex(ITupleReference tuple, MultiComparator cmp);

    public int getPageNsn();

    public void setPageNsn(int pageNsn);

    public int getRightPage();

    public void setRightPage(int rightPage);

    public int getBestChildPageId(MultiComparator cmp);

    public boolean findBestChild(ITupleReference tuple, TupleEntryArrayList entries, MultiComparator cmp);

    public void adjustMBR(ITreeIndexTupleReference[] tuples, MultiComparator cmp);

    public void adjustKey(ITupleReference tuple, int tupleIndex, MultiComparator cmp);

    public int split(ITreeIndexFrame rightFrame, ITupleReference tuple, MultiComparator cmp, ISplitKey splitKey,
            TupleEntryArrayList entries1, TupleEntryArrayList entries2, Rectangle[] rec) throws Exception;

    public boolean intersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp);
    
    public Rectangle checkIntersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp);

    public int getChildPageIdIfIntersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp);

    public void enlarge(ITupleReference tuple, MultiComparator cmp);
}
