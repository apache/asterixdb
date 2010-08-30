package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameInterior;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameInteriorFactory;

public class BTreeNSMInteriorFactory implements IBTreeFrameInteriorFactory {
	@Override
	public IBTreeFrameInterior getFrame() {		
		return new BTreeNSMInterior();
	}	
}
