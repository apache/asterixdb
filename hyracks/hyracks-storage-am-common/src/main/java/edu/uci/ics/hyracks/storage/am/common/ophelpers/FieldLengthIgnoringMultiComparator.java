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

package edu.uci.ics.hyracks.storage.am.common.ophelpers;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * MultiComparator that always passes 0 as a tuple's field length. This may speed up comparisons.
 */
public class FieldLengthIgnoringMultiComparator extends MultiComparator {

    public FieldLengthIgnoringMultiComparator(IBinaryComparator[] cmps) {
        super(cmps);
    }

    @Override
    public int compare(ITupleReference tupleA, ITupleReference tupleB) {
        for (int i = 0; i < cmps.length; i++) {
            int cmp = cmps[i].compare(tupleA.getFieldData(i), tupleA.getFieldStart(i), 0, tupleB.getFieldData(i),
                    tupleB.getFieldStart(i), 0);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    @Override
    public int selectiveFieldCompare(ITupleReference tupleA, ITupleReference tupleB, int[] fields) {
        for (int j = 0; j < cmps.length; j++) {
            int i = fields[j];
            int cmp = cmps[j].compare(tupleA.getFieldData(i), tupleA.getFieldStart(i), 0, tupleB.getFieldData(i),
                    tupleB.getFieldStart(i), 0);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    @Override
    public int compare(ITupleReference tupleA, ITupleReference tupleB, int startFieldIndex) {
        for (int i = 0; i < cmps.length; i++) {
            int ix = startFieldIndex + i;
            int cmp = cmps[i].compare(tupleA.getFieldData(ix), tupleA.getFieldStart(ix), 0, tupleB.getFieldData(ix),
                    tupleB.getFieldStart(ix), 0);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    @Override
    public int fieldRangeCompare(ITupleReference tupleA, ITupleReference tupleB, int startFieldIndex, int numFields) {
        for (int i = startFieldIndex; i < startFieldIndex + numFields; i++) {
            int cmp = cmps[i].compare(tupleA.getFieldData(i), tupleA.getFieldStart(i), 0, tupleB.getFieldData(i),
                    tupleB.getFieldStart(i), 0);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }
}