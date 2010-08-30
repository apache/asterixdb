package edu.uci.ics.asterix.indexing.btree.impls;

import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameLeaf;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameLeafFactory;

public class BTreeNSMLeafFactory implements IBTreeFrameLeafFactory {
	@Override
	public IBTreeFrameLeaf getFrame() {		
		return new BTreeNSMLeaf();
	}
}
