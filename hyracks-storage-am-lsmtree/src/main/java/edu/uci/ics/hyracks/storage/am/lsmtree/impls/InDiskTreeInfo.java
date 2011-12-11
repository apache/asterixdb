package edu.uci.ics.hyracks.storage.am.lsmtree.impls;

import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;

public class InDiskTreeInfo {

	private BTree bTree;
	
	public InDiskTreeInfo(BTree bTree) {
		this.bTree = bTree;
	}

	public BTree getBTree() {
		return bTree;
	}
}
