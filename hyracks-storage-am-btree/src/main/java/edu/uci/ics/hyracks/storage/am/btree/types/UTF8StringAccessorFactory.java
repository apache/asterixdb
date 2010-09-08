package edu.uci.ics.hyracks.storage.am.btree.types;

import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessor;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessorFactory;

public class UTF8StringAccessorFactory implements IFieldAccessorFactory {
	
	private static final long serialVersionUID = 1L;

	@Override
	public IFieldAccessor getFieldAccessor() {
		return new UTF8StringAccessor();
	}	
}
