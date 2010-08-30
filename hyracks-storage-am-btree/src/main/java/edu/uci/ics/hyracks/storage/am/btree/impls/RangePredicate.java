package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.storage.am.btree.interfaces.ISearchPredicate;

public class RangePredicate implements ISearchPredicate {
	
	protected boolean isForward = true;
	protected byte[] lowKeys = null;
	protected byte[] highKeys = null;
	protected MultiComparator cmp;
	
	public RangePredicate() {
	}
	
	// TODO: for now range is [lowKey, highKey] but depending on user predicate the range could be exclusive on any end
	// need to model this somehow	
	// for point queries just use same value for low and high key
	public RangePredicate(boolean isForward, byte[] lowKeys, byte[] highKeys, MultiComparator cmp) {
		this.isForward = isForward;
		this.lowKeys = lowKeys;
		this.highKeys = highKeys;
		this.cmp = cmp;
	}
	
	public MultiComparator getComparator() {
		return cmp;
	}
	
	public void setComparator(MultiComparator cmp) {
		this.cmp = cmp;
	}
	
	public boolean isForward() {
		return isForward;
	}	
	
	public byte[] getLowKeys() {
		return lowKeys;
	}
	
	public byte[] getHighKeys() {
		return highKeys;
	}
}
