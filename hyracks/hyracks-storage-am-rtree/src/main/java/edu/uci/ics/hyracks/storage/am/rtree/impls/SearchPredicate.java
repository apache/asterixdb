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

package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class SearchPredicate implements ISearchPredicate {

	private static final long serialVersionUID = 1L;

	protected ITupleReference searchKey;
	protected MultiComparator cmp;

	public SearchPredicate(ITupleReference searchKey, MultiComparator cmp) {
		this.searchKey = searchKey;
		this.cmp = cmp;
	}

	public ITupleReference getSearchKey() {
		return searchKey;
	}

	public void setSearchKey(ITupleReference searchKey) {
		this.searchKey = searchKey;
	}

	public MultiComparator getLowKeyComparator() {
		return cmp;
	}

	public MultiComparator getHighKeyComparator() {
		return cmp;
	}
}
