package edu.uci.ics.hyracks.storage.am.btree.interfaces;

import edu.uci.ics.hyracks.storage.am.btree.frames.FieldPrefixNSMLeaf;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;

public interface IFrameCompressor {
	public boolean compress(FieldPrefixNSMLeaf frame, MultiComparator cmp) throws Exception;
}
