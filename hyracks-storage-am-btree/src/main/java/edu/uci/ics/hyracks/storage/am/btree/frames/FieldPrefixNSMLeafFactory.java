package edu.uci.ics.hyracks.storage.am.btree.frames;

import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameLeaf;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameLeafFactory;

public class FieldPrefixNSMLeafFactory implements IBTreeFrameLeafFactory {
	@Override
	public IBTreeFrameLeaf getFrame() {		
		return new FieldPrefixNSMLeaf();
	}
}