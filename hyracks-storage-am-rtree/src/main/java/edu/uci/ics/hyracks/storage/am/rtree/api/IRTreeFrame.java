package edu.uci.ics.hyracks.storage.am.rtree.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.impls.Rectangle;
import edu.uci.ics.hyracks.storage.am.rtree.impls.TupleEntryArrayList;

public interface IRTreeFrame extends ITreeIndexFrame {

    public ITreeIndexTupleReference createTupleReference();

    public void generateDist(ITupleReference tuple, TupleEntryArrayList entries, Rectangle rec, int start, int end);

    public void computeMBR(ISplitKey splitKey, MultiComparator cmp);

    public void insert(ITupleReference tuple, MultiComparator cmp, int tupleIndex) throws Exception;
    
    public void delete(int tupleIndex, MultiComparator cmp) throws Exception;

    public int getPageNsn();

    public void setPageNsn(int pageNsn);

    public int getRightPage();

    public void setRightPage(int rightPage);

    public void adjustMBR(ITreeIndexTupleReference[] tuples, MultiComparator cmp);

}
