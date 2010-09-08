package edu.uci.ics.hyracks.storage.am.btree.types;

import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessor;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessorFactory;

public class Int32AccessorFactory implements IFieldAccessorFactory {
	
	private static final long serialVersionUID = 1L;
		
	@Override
	public IFieldAccessor getFieldAccessor() {	
		return new Int32Accessor();
	}
	
}
