package edu.uci.ics.hyracks.storage.am.btree.interfaces;

import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;

public interface IBTreeFrameInterior extends IBTreeFrame {
	//public int getChildPageId(IFieldAccessor[] fields, MultiComparator cmp);
	public int getChildPageId(RangePredicate pred, MultiComparator srcCmp);
	public int getLeftmostChildPageId(MultiComparator cmp);
	public int getRightmostChildPageId(MultiComparator cmp);
	public void setRightmostChildPageId(int pageId);
	public void deleteGreatest(MultiComparator cmp);
}
