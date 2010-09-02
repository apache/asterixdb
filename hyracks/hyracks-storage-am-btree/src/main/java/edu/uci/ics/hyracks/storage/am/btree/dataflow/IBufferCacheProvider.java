package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.Serializable;

import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.FileManager;

public interface IBufferCacheProvider extends Serializable {
	public IBufferCache getBufferCache();
	public FileManager getFileManager();
}
