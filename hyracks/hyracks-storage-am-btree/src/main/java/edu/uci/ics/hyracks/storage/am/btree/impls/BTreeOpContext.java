package edu.uci.ics.asterix.indexing.btree.impls;

import java.util.ArrayList;
import java.util.Stack;

import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeCursor;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameInterior;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameLeaf;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameMeta;

public final class BTreeOpContext {
	public BTreeOp op;
	public IBTreeFrameLeaf leafFrame;
	public IBTreeFrameInterior interiorFrame;
	public IBTreeFrameMeta metaFrame;
	public IBTreeCursor cursor;
	public RangePredicate pred;	
	public SplitKey splitKey;
	public int opRestarts = 0;
	public ArrayList<Integer> smPages;	
	public Stack<Integer> pageLsns;
	public ArrayList<Integer> freePages;
}
