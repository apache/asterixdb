package edu.uci.ics.asterix.indexing.btree.impls;

import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameInterior;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameInteriorFactory;

public class BTreeNSMInteriorFactory implements IBTreeFrameInteriorFactory {
	@Override
	public IBTreeFrameInterior getFrame() {		
		return new BTreeNSMInterior();
	}	
}
