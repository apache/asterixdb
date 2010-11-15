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

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.FileInfo;

public class RangeSearchCursor implements IBTreeCursor {

	private ISearchPredicate searchPred = null;	
	private int tupleIndex = 0;
	private int fileId = -1;
	private ICachedPage page = null;
	private IBTreeLeafFrame frame = null;
	private IBufferCache bufferCache = null;
	
	private IBTreeTupleReference frameTuple;
			
	public RangeSearchCursor(IBTreeLeafFrame frame) {
		this.frame = frame;
		this.frameTuple = frame.getTupleWriter().createTupleReference();
	}
	
	@Override
	public void close() throws Exception {
		page.releaseReadLatch();
		bufferCache.unpin(page);
		page = null;
	}
	
	public ITupleReference getTuple() {
		return frameTuple;
	}
	
	@Override
	public ICachedPage getPage() {
		return page;
	}
		
	@Override
	public boolean hasNext() throws Exception {
		if(tupleIndex >= frame.getTupleCount()) {
			int nextLeafPage = -1;
			if(searchPred.isForward()) {
				nextLeafPage = frame.getNextLeaf();
			}
			else {
				nextLeafPage = frame.getPrevLeaf();
			}
						
			if(nextLeafPage >= 0) {			
				ICachedPage nextLeaf = bufferCache.pin(FileInfo.getDiskPageId(fileId, nextLeafPage), false);
				nextLeaf.acquireReadLatch();
								
				page.releaseReadLatch();
				bufferCache.unpin(page);
				
				page = nextLeaf;
				frame.setPage(page);
				
				tupleIndex = 0;
			}
			else {
				return false;
			}
		}
		
		// in any case compare current key
		RangePredicate pred = (RangePredicate)searchPred;
		MultiComparator cmp = pred.getComparator();
		if(searchPred.isForward()) {
			ITupleReference highKey = pred.getHighKey();		
			frameTuple.resetByTupleIndex(frame, tupleIndex);
									
			if(highKey == null) return true;
			if(cmp.compare(highKey, frameTuple) < 0) {
				return false;
			}
			else {
				return true;
			}
		}
		else {
			ITupleReference lowKey = pred.getLowKey();						
			frameTuple.resetByTupleIndex(frame, frame.getTupleCount() - tupleIndex - 1);
			if(lowKey == null) return true;
			
			if(cmp.compare(lowKey, frameTuple) > 0) {
				return false;
			}
			else {
				return true;
			}
		}		
	}

	@Override
	public void next() throws Exception {		
		tupleIndex++;
	}
	
	@Override
	public void open(ICachedPage page, ISearchPredicate searchPred) throws Exception {		
		// in case open is called multiple times without closing
		if(this.page != null) {
			this.page.releaseReadLatch();
			bufferCache.unpin(this.page);
		}
						
		this.searchPred = searchPred;
		this.page = page;
		frame.setPage(page);					
		
		// position tupleIndex to the first appropriate key
		// TODO: can be done more efficiently with binary search but this needs some thinking/refactoring
		RangePredicate pred = (RangePredicate)searchPred;
		MultiComparator cmp = pred.getComparator();
		frameTuple.setFieldCount(cmp.getFieldCount());
		if(searchPred.isForward()) {
			ITupleReference lowKey = pred.getLowKey();			
			
			frameTuple.resetByTupleIndex(frame, tupleIndex);
			if(lowKey == null) return; // null means -infinity
						
			while(cmp.compare(lowKey, frameTuple) > 0 && tupleIndex < frame.getTupleCount()) {
			    tupleIndex++;
			    frameTuple.resetByTupleIndex(frame, tupleIndex);
			}						
		}
		else {
			ITupleReference highKey = pred.getHighKey();
			
			frameTuple.resetByTupleIndex(frame, frame.getTupleCount() - tupleIndex - 1);
			if(highKey == null) return; // null means +infinity
			    
			while(cmp.compare(highKey, frameTuple) < 0 && tupleIndex < frame.getTupleCount()) {				
			    tupleIndex++;
			    frameTuple.resetByTupleIndex(frame, frame.getTupleCount() - tupleIndex - 1);
			}						
		}
	}
	
	@Override
	public void reset() {
		tupleIndex = 0;
		page = null;	
		searchPred = null;
	}

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        this.bufferCache = bufferCache;        
    }

    @Override
    public void setFileId(int fileId) {
        this.fileId = fileId;
    }	
}
