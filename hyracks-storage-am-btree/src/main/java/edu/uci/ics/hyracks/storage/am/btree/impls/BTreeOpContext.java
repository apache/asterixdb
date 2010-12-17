/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeMetaDataFrame;

public final class BTreeOpContext {
	public final BTreeOp op;
	public final IBTreeLeafFrame leafFrame;
	public final IBTreeInteriorFrame interiorFrame;
	public final IBTreeMetaDataFrame metaFrame;
	public IBTreeCursor cursor;
	public RangePredicate pred;	
	public final SplitKey splitKey;
	public int opRestarts = 0;
	public final IntArrayList pageLsns; // used like a stack
	public final IntArrayList smPages;	
	public final IntArrayList freePages;

	public BTreeOpContext(BTreeOp op, IBTreeLeafFrame leafFrame, IBTreeInteriorFrame interiorFrame,
			IBTreeMetaDataFrame metaFrame, int treeHeightHint) {
		this.op = op;
		this.leafFrame = leafFrame;
		this.interiorFrame = interiorFrame;
		this.metaFrame = metaFrame;
		
		pageLsns = new IntArrayList(treeHeightHint, treeHeightHint);	
		if(op != BTreeOp.BTO_SEARCH) {			
			smPages = new IntArrayList(treeHeightHint, treeHeightHint);
			freePages = new IntArrayList(treeHeightHint, treeHeightHint);
			pred = new RangePredicate(true, null, null, true, true, null, null);
			splitKey = new SplitKey(leafFrame.getTupleWriter().createTupleReference());
		}
		else {			
			smPages = null;
			freePages = null;	
			splitKey = null;
		}		
	}
	
	public void reset() {
		if(pageLsns != null) pageLsns.clear();
		if(freePages != null) freePages.clear();
		if(smPages != null) smPages.clear();
		opRestarts = 0;
	}
}
