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

import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameLeaf;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.ISearchPredicate;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.FileInfo;

public class BTreeRangeSearchCursor implements IBTreeCursor {

	private ISearchPredicate searchPred = null;	
	private int recordNum = 0;
	private int recordOffset = -1;
	private int fileId = -1;
	private ICachedPage page = null;
	private IBTreeFrameLeaf frame = null;
	private IBufferCache bufferCache = null;
	
	public BTreeRangeSearchCursor(IBTreeFrameLeaf frame) {
		this.frame = frame;		
	}
	
	@Override
	public void close() throws Exception {
		page.releaseReadLatch();
		bufferCache.unpin(page);
		page = null;
	}

	@Override
	public int getOffset() {
		return recordOffset;
	}

	@Override
	public ICachedPage getPage() {
		return page;
	}
		
	@Override
	public boolean hasNext() throws Exception {
		if(recordNum >= frame.getNumRecords()) {
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
				
				recordNum = 0;
			}
			else {
				return false;
			}
		}
		
		// in any case compare current key
		RangePredicate pred = (RangePredicate)searchPred;
		MultiComparator cmp = pred.getComparator();
		if(searchPred.isForward()) {
			byte[] highKeys = pred.getHighKeys();			
			recordOffset = frame.getRecordOffset(recordNum);
			
			if(highKeys == null) return true;						
			if(cmp.compare(highKeys, 0, page.getBuffer().array(), recordOffset) < 0) {
				return false;
			}
			else {
				return true;
			}
		}
		else {
			byte[] lowKeys = pred.getLowKeys();			
			recordOffset = frame.getRecordOffset(frame.getNumRecords() - recordNum - 1);
			if(lowKeys == null) return true;
			
			if(cmp.compare(lowKeys, 0, page.getBuffer().array(), recordOffset) > 0) {
				return false;
			}
			else {
				return true;
			}
		}		
	}

	@Override
	public void next() throws Exception {		
		recordNum++;
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
		
		// position recordNum to the first appropriate key
		// TODO: can be done more efficiently with binary search but this needs some thinking/refactoring
		RangePredicate pred = (RangePredicate)searchPred;
		MultiComparator cmp = pred.getComparator();
		if(searchPred.isForward()) {
			byte[] lowKeys = pred.getLowKeys();
						
			recordOffset = frame.getRecordOffset(recordNum);
			if(lowKeys == null) return; // null means -infinity
			
			while(cmp.compare(lowKeys, 0, page.getBuffer().array(), recordOffset) > 0 && recordNum < frame.getNumRecords()) {				
			    recordNum++;
			    recordOffset = frame.getRecordOffset(recordNum);
			}
		}
		else {
			byte[] highKeys = pred.getHighKeys();
						
			recordOffset = frame.getRecordOffset(frame.getNumRecords() - recordNum - 1);			
			if(highKeys != null) return; // null means +infinity
			    
			while(cmp.compare(highKeys, 0, page.getBuffer().array(), recordOffset) < 0 && recordNum < frame.getNumRecords()) {				
			    recordNum++;
			    recordOffset = frame.getRecordOffset(frame.getNumRecords() - recordNum - 1);				
			}						
		}
	}
	
	@Override
	public void reset() {
		recordNum = 0;
		recordOffset = 0;
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
