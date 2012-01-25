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

import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class BTreeFactory {

	private IBufferCache bufferCache;
	private int fieldCount;
	private MultiComparator cmp;
	private ITreeIndexFrameFactory interiorFrameFactory;
	private ITreeIndexFrameFactory leafFrameFactory;
	private LinkedListFreePageManagerFactory freePageManagerFactory;
	
	public BTreeFactory(IBufferCache bufferCache, LinkedListFreePageManagerFactory freePageManagerFactory, MultiComparator cmp, 
			int fieldCount, ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory) {
		this.bufferCache = bufferCache;
		this.fieldCount = fieldCount;
		this.cmp = cmp;
		this.interiorFrameFactory = interiorFrameFactory;
		this.leafFrameFactory = leafFrameFactory;
		this.freePageManagerFactory = freePageManagerFactory;
	}
	
    public BTree createBTreeInstance(int fileId) {
        return new BTree(bufferCache, fieldCount, cmp, freePageManagerFactory.createFreePageManager(fileId),
                interiorFrameFactory, leafFrameFactory);
    }
	
    public IBufferCache getBufferCache() {
        return bufferCache;
    }
    
}
