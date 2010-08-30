package edu.uci.ics.asterix.indexing.btree.interfaces;

import edu.uci.ics.asterix.storage.buffercache.IBufferCache;
import edu.uci.ics.asterix.storage.buffercache.ICachedPage;

public interface IBTreeCursor {
	public void reset();
	public boolean hasNext() throws Exception;
	public int getOffset();
	public void next() throws Exception;	
	public void open(ICachedPage page, ISearchPredicate searchPred) throws Exception;
	public ICachedPage getPage();
	public void close() throws Exception;
	public void setBufferCache(IBufferCache bufferCache);
	public void setFileId(int fileId);
}
