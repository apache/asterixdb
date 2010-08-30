package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class NodeFrontier {
	public int bytesInserted;
	public ICachedPage page;
	public int pageId;
	public byte[] lastRecord;
}

