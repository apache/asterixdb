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

package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOpContext;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;

public final class RTreeOpContext implements IndexOpContext {
	public final IndexOp op;
	public final IRTreeInteriorFrame interiorFrame;
	public final IRTreeLeafFrame leafFrame;
	public ITreeIndexCursor cursor;
	public RTreeCursorInitialState cursorInitialState;
	public final ITreeIndexMetaDataFrame metaFrame;
	public final RTreeSplitKey splitKey;
	public ITupleReference tuple;
	public final PathList pathList; // used to record the pageIds and pageLsns
									// of the visited pages
	public final PathList traverseList; // used for traversing the tree
	private static final int initTraverseListSize = 100;

	public RTreeOpContext(IndexOp op, IRTreeLeafFrame leafFrame,
			IRTreeInteriorFrame interiorFrame,
			ITreeIndexMetaDataFrame metaFrame, int treeHeightHint) {
		this.op = op;
		this.interiorFrame = interiorFrame;
		this.leafFrame = leafFrame;
		this.metaFrame = metaFrame;
		pathList = new PathList(treeHeightHint, treeHeightHint);
		if (op != IndexOp.SEARCH && op != IndexOp.DISKORDERSCAN) {
			splitKey = new RTreeSplitKey(interiorFrame.getTupleWriter()
					.createTupleReference(), interiorFrame.getTupleWriter()
					.createTupleReference());
			traverseList = new PathList(initTraverseListSize,
					initTraverseListSize);
		} else {
			splitKey = null;
			traverseList = null;
			cursorInitialState = new RTreeCursorInitialState(pathList, 1);
		}
	}

	public ITupleReference getTuple() {
		return tuple;
	}

	public void setTuple(ITupleReference tuple) {
		this.tuple = tuple;
	}

	public void reset() {
		if (pathList != null) {
			pathList.clear();
		}
		if (traverseList != null) {
			traverseList.clear();
		}
	}
}
