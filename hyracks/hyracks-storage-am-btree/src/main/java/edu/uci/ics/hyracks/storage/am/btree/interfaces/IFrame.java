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
package edu.uci.ics.hyracks.storage.am.btree.interfaces;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public interface IFrame {	
	public void setPage(ICachedPage page);
	public ICachedPage getPage();
	public ByteBuffer getBuffer();
	
	public void insert(byte[] data, MultiComparator cmp) throws Exception;
	public void update(int rid, byte[] data) throws Exception;
	public void delete(byte[] data, MultiComparator cmp, boolean exactDelete) throws Exception;
	
	public void compact(MultiComparator cmp);
	public void compress(MultiComparator cmp) throws Exception;
	
	public void initBuffer(byte level);
	
	public int getNumRecords();
		
	// assumption: page must be write-latched at this point
	public SpaceStatus hasSpaceInsert(byte[] data, MultiComparator cmp);
	public SpaceStatus hasSpaceUpdate(int rid, byte[] data, MultiComparator cmp);
	
	public int getRecordOffset(int slotNum);
	
	public int getTotalFreeSpace();	
	
	public void setPageLsn(int pageLsn);
	public int getPageLsn();
	
	// for debugging
	public void printHeader();
	public String printKeys(MultiComparator cmp);
}