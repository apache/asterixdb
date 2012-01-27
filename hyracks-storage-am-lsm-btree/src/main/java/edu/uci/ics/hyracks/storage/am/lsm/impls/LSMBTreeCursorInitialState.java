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

package edu.uci.ics.hyracks.storage.am.lsm.impls;

import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMBTreeCursorInitialState implements ICursorInitialState {

	private int numBTrees;
	private ITreeIndexFrameFactory leafFrameFactory;
	private MultiComparator cmp;
	private LSMBTree lsm;
	
	public LSMBTreeCursorInitialState(int numBTrees, ITreeIndexFrameFactory leafFrameFactory, MultiComparator cmp, LSMBTree lsm) {
		this.numBTrees = numBTrees;
		this.leafFrameFactory = leafFrameFactory;
		this.cmp = cmp;
		this.lsm = lsm;
	}
	
	public int getNumBTrees() {
		return numBTrees;
	}

	public ITreeIndexFrameFactory getLeafFrameFactory() {
		return leafFrameFactory;
	}

	public MultiComparator getCmp() {
		return cmp;
	}

	@Override
	public ICachedPage getPage() {
		return null;
	}

	@Override
	public void setPage(ICachedPage page) {
	}

	public LSMBTree getLsm() {
		return lsm;
	}

}
