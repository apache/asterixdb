package edu.uci.ics.asterix.indexing.btree.frames;

import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameLeaf;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameLeafFactory;

public class FieldPrefixNSMLeafFactory implements IBTreeFrameLeafFactory {
	@Override
	public IBTreeFrameLeaf getFrame() {		
		return new FieldPrefixNSMLeaf();
	}
}