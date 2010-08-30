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

import edu.uci.ics.hyracks.storage.am.btree.interfaces.ISearchPredicate;

public class RangePredicate implements ISearchPredicate {
	
	protected boolean isForward = true;
	protected byte[] lowKeys = null;
	protected byte[] highKeys = null;
	protected MultiComparator cmp;
	
	public RangePredicate() {
	}
	
	// TODO: for now range is [lowKey, highKey] but depending on user predicate the range could be exclusive on any end
	// need to model this somehow	
	// for point queries just use same value for low and high key
	public RangePredicate(boolean isForward, byte[] lowKeys, byte[] highKeys, MultiComparator cmp) {
		this.isForward = isForward;
		this.lowKeys = lowKeys;
		this.highKeys = highKeys;
		this.cmp = cmp;
	}
	
	public MultiComparator getComparator() {
		return cmp;
	}
	
	public void setComparator(MultiComparator cmp) {
		this.cmp = cmp;
	}
	
	public boolean isForward() {
		return isForward;
	}	
	
	public byte[] getLowKeys() {
		return lowKeys;
	}
	
	public byte[] getHighKeys() {
		return highKeys;
	}
}
