package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameLeaf;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameLeafFactory;

public class BTreeNSMLeafFactory implements IBTreeFrameLeafFactory {
	@Override
	public IBTreeFrameLeaf getFrame() {		
		return new BTreeNSMLeaf();
	}
}
