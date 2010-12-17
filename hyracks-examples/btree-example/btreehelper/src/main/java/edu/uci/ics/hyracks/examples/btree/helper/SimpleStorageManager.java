package edu.uci.ics.hyracks.examples.btree.helper;

import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class SimpleStorageManager implements IStorageManagerInterface {

	private static final long serialVersionUID = 1L;
	
	public static SimpleStorageManager INSTANCE = new SimpleStorageManager();	

	@Override
	public IBufferCache getBufferCache() {
		return RuntimeContext.getInstance().getBufferCache();
	}

	@Override
	public IFileMapProvider getFileMapProvider() {
		return RuntimeContext.getInstance().getFileMapManager();
	}

}
