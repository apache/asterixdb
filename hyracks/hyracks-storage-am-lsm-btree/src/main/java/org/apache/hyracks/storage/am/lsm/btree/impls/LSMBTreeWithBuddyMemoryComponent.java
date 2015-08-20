/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.storage.am.lsm.btree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractMemoryLSMComponent;

/*
 * This class is also not needed at the moment but is implemented anyway
 */
public class LSMBTreeWithBuddyMemoryComponent extends
		AbstractMemoryLSMComponent {

	private final BTree btree;
	private final BTree buddyBtree;

	public LSMBTreeWithBuddyMemoryComponent(BTree btree, BTree buddyBtree,
			IVirtualBufferCache vbc, boolean isActive) {
		super(vbc, isActive);
		this.btree = btree;
		this.buddyBtree = buddyBtree;
	}

	public BTree getBTree() {
		return btree;
	}

	public BTree getBuddyBTree() {
		return buddyBtree;
	}

	@Override
	protected void reset() throws HyracksDataException {
		super.reset();
		btree.deactivate();
		btree.destroy();
		btree.create();
		btree.activate();
		buddyBtree.deactivate();
		buddyBtree.destroy();
		buddyBtree.create();
		buddyBtree.activate();
	}

}
