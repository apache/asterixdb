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

package edu.uci.ics.hyracks.storage.am.common.ophelpers;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class MultiComparator {

	private final IBinaryComparator[] cmps;

	public MultiComparator(IBinaryComparator[] cmps) {
		this.cmps = cmps;
	}

	public int compare(ITupleReference tupleA, ITupleReference tupleB) {
		for (int i = 0; i < cmps.length; i++) {
			int cmp = cmps[i].compare(tupleA.getFieldData(i),
					tupleA.getFieldStart(i), tupleA.getFieldLength(i),
					tupleB.getFieldData(i), tupleB.getFieldStart(i),
					tupleB.getFieldLength(i));
			if (cmp < 0)
				return -1;
			else if (cmp > 0)
				return 1;
		}
		return 0;
	}

	public int fieldRangeCompare(ITupleReference tupleA,
			ITupleReference tupleB, int startFieldIndex, int numFields) {
		for (int i = startFieldIndex; i < startFieldIndex + numFields; i++) {
			int cmp = cmps[i].compare(tupleA.getFieldData(i),
					tupleA.getFieldStart(i), tupleA.getFieldLength(i),
					tupleB.getFieldData(i), tupleB.getFieldStart(i),
					tupleB.getFieldLength(i));
			if (cmp < 0)
				return -1;
			else if (cmp > 0)
				return 1;
		}
		return 0;
	}

	public IBinaryComparator[] getComparators() {
		return cmps;
	}

    public int getKeyFieldCount() {
		return cmps.length;
	}
}