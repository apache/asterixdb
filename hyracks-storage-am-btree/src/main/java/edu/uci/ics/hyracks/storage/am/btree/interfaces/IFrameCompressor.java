package edu.uci.ics.asterix.indexing.btree.interfaces;

import edu.uci.ics.asterix.indexing.btree.frames.FieldPrefixNSMLeaf;
import edu.uci.ics.asterix.indexing.btree.impls.MultiComparator;

public interface IFrameCompressor {
	public boolean compress(FieldPrefixNSMLeaf frame, MultiComparator cmp) throws Exception;
}
