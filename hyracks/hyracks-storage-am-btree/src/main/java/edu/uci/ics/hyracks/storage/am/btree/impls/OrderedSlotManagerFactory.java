package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.storage.am.btree.interfaces.ISlotManager;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.ISlotManagerFactory;

public class OrderedSlotManagerFactory implements ISlotManagerFactory {
	
	@Override
	public ISlotManager getSlotManager() {
		return new OrderedSlotManager();
	}	
}
