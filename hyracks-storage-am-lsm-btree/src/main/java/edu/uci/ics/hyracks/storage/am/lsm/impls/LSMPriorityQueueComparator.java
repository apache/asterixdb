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

import java.util.Comparator;

import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class LSMPriorityQueueComparator implements Comparator<LSMPriorityQueueElement> {

	private final MultiComparator cmp;
	
	public LSMPriorityQueueComparator(MultiComparator cmp) {
		this.cmp = cmp;
	}
	
	@Override
	public int compare(LSMPriorityQueueElement elementA, LSMPriorityQueueElement elementB) {
		
		int result = cmp.compare(elementA.getTuple(), elementB.getTuple());
		
		if(result == 1) {
			return 1;
		}
		else if(result == -1) {
			return -1;
		}
		else {
			if(elementA.getCursorIndex() > elementB.getCursorIndex()) {
				return 1;
			}
			else {
				return -1;
			}
		}
	}

}
