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
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldIterator;
import edu.uci.ics.hyracks.storage.am.btree.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.FileInfo;

public class RangeSearchCursor implements IBTreeCursor {

	private ISearchPredicate searchPred = null;	
	private int recordNum = 0;
	private int recordOffset = -1;
	private int fileId = -1;
	private ICachedPage page = null;
	private IBTreeLeafFrame frame = null;
	private IBufferCache bufferCache = null;
	
	private IFieldIterator fieldIter;
	
	
	public RangeSearchCursor(IBTreeLeafFrame frame) {
		this.frame = frame;
		this.fieldIter = frame.createFieldIterator();
		this.fieldIter.setFrame(frame);
	}
	
	@Override
	public void close() throws Exception {
		page.releaseReadLatch();
		bufferCache.unpin(page);
		page = null;
	}
	
	public IFieldIterator getFieldIterator() {
		fieldIter.reset();
		return fieldIter;
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
			ITupleReference highKey = pred.getHighKey();		
			fieldIter.openRecSlotNum(recordNum);
			//recordOffset = frame.getRecordOffset(recordNum);
									
			if(highKey == null) return true;									
			//if(cmp.compare(highKeys, 0, page.getBuffer().array(), recordOffset) < 0) {
			if(cmp.compare(highKey, fieldIter) < 0) {
				return false;
			}
			else {
				return true;
			}
		}
		else {
			ITupleReference lowKey = pred.getLowKey();						
			fieldIter.openRecSlotNum(frame.getNumRecords() - recordNum - 1);
			//recordOffset = frame.getRecordOffset(frame.getNumRecords() - recordNum - 1);
			if(lowKey == null) return true;
			
			if(cmp.compare(lowKey, fieldIter) > 0) {
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
		this.fieldIter.setFields(cmp.getFields());
		if(searchPred.isForward()) {
			ITupleReference lowKey = pred.getLowKey();
						
			//recordOffset = frame.getRecordOffset(recordNum);			
			fieldIter.openRecSlotNum(recordNum);
			if(lowKey == null) return; // null means -infinity
						
			while(cmp.compare(lowKey, fieldIter) > 0 && recordNum < frame.getNumRecords()) {
			//while(cmp.compare(lowKeys, 0, page.getBuffer().array(), recordOffset) > 0 && recordNum < frame.getNumRecords()) {
			    recordNum++;
			    fieldIter.openRecSlotNum(recordNum);
			}						
		}
		else {
			ITupleReference highKey = pred.getHighKey();
			
			//recordOffset = frame.getRecordOffset(frame.getNumRecords() - recordNum - 1);
			fieldIter.openRecSlotNum(frame.getNumRecords() - recordNum - 1);
			if(highKey != null) return; // null means +infinity
			    
			while(cmp.compare(highKey, fieldIter) < 0 && recordNum < frame.getNumRecords()) {				
			    recordNum++;
			    fieldIter.openRecSlotNum(recordNum);
			    //recordOffset = frame.getRecordOffset(frame.getNumRecords() - recordNum - 1);			
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
