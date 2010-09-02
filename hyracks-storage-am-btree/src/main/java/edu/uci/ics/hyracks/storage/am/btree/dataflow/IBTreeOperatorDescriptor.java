package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;

public interface IBTreeOperatorDescriptor {
	public String getBTreeFileName();
	public int getBTreeFileId();
	
	public MultiComparator getMultiComparator();	
	public RangePredicate getRangePred();
	
	public IBTreeInteriorFrameFactory getInteriorFactory();
	public IBTreeLeafFrameFactory getLeafFactory();
	
	public IBufferCacheProvider getBufferCacheProvider();
	public IBTreeRegistryProvider getBTreeRegistryProvider();
}
