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

import java.nio.ByteBuffer;

public class SplitKey {		
	public byte[] data = null;	
	public ByteBuffer buf = null;
	public SelfDescTupleReference tuple = new SelfDescTupleReference();
	
	public void initData(int keySize) {
		// try to reuse existing memory from a lower-level split if possible
		// in general this is always possible for fixed-sized keys		
		/*
		if(data != null) {
			if(data.length < keySize + 8) {
				data = new byte[keySize + 8]; // add 8 for left and right page
				buf = ByteBuffer.wrap(data);
			}				
		}
		else { 
			data = new byte[keySize + 8]; // add 8 for left and right page
			buf = ByteBuffer.wrap(data);
		}
		*/		
		data = new byte[keySize + 8]; // add 8 for left and right page
		buf = ByteBuffer.wrap(data);
		tuple.reset(buf, 0);
	}
	
	public void reset() {
		data = null;
		buf = null;
	}
	
	public void setData(byte[] data) {
		this.data = data;
	}
	
	public ByteBuffer getBuffer() {
		return buf;
	}
	
	public SelfDescTupleReference getTuple() {
		return tuple;
	}
	
	public int getLeftPage() {
		return buf.getInt(data.length - 8);
	}
	
	public int getRightPage() {
		return buf.getInt(data.length - 4);
	}
		
	public void setLeftPage(int leftPage) {
		buf.putInt(data.length - 8, leftPage);
	}
	
	public void setRightPage(int rightPage) {
		buf.putInt(data.length - 4, rightPage);
	}
	
	public void setPages(int leftPage, int rightPage) {
		buf.putInt(data.length - 8, leftPage);
		buf.putInt(data.length - 4, rightPage);
	}
	
	public SplitKey duplicate() {
		SplitKey copy = new SplitKey();
		copy.data = data.clone();		
		copy.buf = ByteBuffer.wrap(copy.data);
		copy.tuple = new SelfDescTupleReference();
		copy.tuple.setFields(tuple.getFields());
		copy.tuple.reset(copy.buf, 0);	
		return copy;
	}
}