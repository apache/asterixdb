package edu.uci.ics.asterix.indexing.btree.impls;

import edu.uci.ics.asterix.indexing.btree.interfaces.ISlotManager;
import edu.uci.ics.asterix.indexing.btree.interfaces.ISlotManagerFactory;

public class OrderedSlotManagerFactory implements ISlotManagerFactory {
	
	@Override
	public ISlotManager getSlotManager() {
		return new OrderedSlotManager();
	}	
}
