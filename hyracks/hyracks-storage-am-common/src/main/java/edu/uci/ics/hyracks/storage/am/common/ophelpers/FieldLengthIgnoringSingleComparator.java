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
 * MultiComparator optimized for the special case where there is only a single comparator.
 * Further speeds up comparisons by always passing 0 as the field's length.
 */
public class FieldLengthIgnoringSingleComparator extends MultiComparator {
    private final IBinaryComparator cmp;

    protected FieldLengthIgnoringSingleComparator(IBinaryComparator[] cmps) {
        super(cmps);
        this.cmp = cmps[0];
    }

    public int compare(ITupleReference tupleA, ITupleReference tupleB) {
        return cmp.compare(tupleA.getFieldData(0), tupleA.getFieldStart(0), 0, tupleB.getFieldData(0),
                tupleB.getFieldStart(0), 0);
    }
}