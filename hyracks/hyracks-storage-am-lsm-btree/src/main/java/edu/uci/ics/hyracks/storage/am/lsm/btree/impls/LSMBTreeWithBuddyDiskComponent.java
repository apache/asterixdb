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
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractDiskLSMComponent;

public class LSMBTreeWithBuddyDiskComponent extends AbstractDiskLSMComponent {

	private final BTree btree;
	private final BTree buddyBtree;
	private final BloomFilter bloomFilter;

	public LSMBTreeWithBuddyDiskComponent(BTree btree, BTree buddyBtree,
			BloomFilter bloomFilter) {
		this.btree = btree;
		this.buddyBtree = buddyBtree;
		this.bloomFilter = bloomFilter;
	}

	@Override
	protected void destroy() throws HyracksDataException {
		btree.deactivate();
		btree.destroy();
		buddyBtree.deactivate();
		buddyBtree.destroy();
		bloomFilter.deactivate();
		bloomFilter.destroy();
	}

	public BTree getBTree() {
		return btree;
	}

	public BTree getBuddyBTree() {
		return buddyBtree;
	}

	public BloomFilter getBloomFilter() {
		return bloomFilter;
	}

	@Override
	public long getComponentSize() {
		long size = btree.getFileReference().getFile().length();
		size += buddyBtree.getFileReference().getFile().length();
		size += bloomFilter.getFileReference().getFile().length();
		return size;
	}

}
