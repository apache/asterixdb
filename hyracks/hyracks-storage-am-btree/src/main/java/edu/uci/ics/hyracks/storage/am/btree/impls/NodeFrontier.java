package edu.uci.ics.asterix.indexing.btree.impls;

import edu.uci.ics.asterix.storage.buffercache.ICachedPage;

public class NodeFrontier {
	public int bytesInserted;
	public ICachedPage page;
	public int pageId;
	public byte[] lastRecord;
}

