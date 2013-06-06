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
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class MultiComparator {

    protected final IBinaryComparator[] cmps;

    public MultiComparator(IBinaryComparator[] cmps) {
        this.cmps = cmps;
    }

    public int compare(ITupleReference tupleA, ITupleReference tupleB) {
        for (int i = 0; i < cmps.length; i++) {
            int cmp = cmps[i].compare(tupleA.getFieldData(i), tupleA.getFieldStart(i), tupleA.getFieldLength(i),
                    tupleB.getFieldData(i), tupleB.getFieldStart(i), tupleB.getFieldLength(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    public int selectiveFieldCompare(ITupleReference tupleA, ITupleReference tupleB, int[] fields) {
        for (int j = 0; j < cmps.length; j++) {
            int i = fields[j];
            int cmp = cmps[j].compare(tupleA.getFieldData(i), tupleA.getFieldStart(i), tupleA.getFieldLength(i),
                    tupleB.getFieldData(i), tupleB.getFieldStart(i), tupleB.getFieldLength(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    public int compare(ITupleReference tupleA, ITupleReference tupleB, int startFieldIndex) {
        for (int i = 0; i < cmps.length; i++) {
            int ix = startFieldIndex + i;
            int cmp = cmps[i].compare(tupleA.getFieldData(ix), tupleA.getFieldStart(ix), tupleA.getFieldLength(ix),
                    tupleB.getFieldData(ix), tupleB.getFieldStart(ix), tupleB.getFieldLength(ix));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    public int fieldRangeCompare(ITupleReference tupleA, ITupleReference tupleB, int startFieldIndex, int numFields) {
        for (int i = startFieldIndex; i < startFieldIndex + numFields; i++) {
            int cmp = cmps[i].compare(tupleA.getFieldData(i), tupleA.getFieldStart(i), tupleA.getFieldLength(i),
                    tupleB.getFieldData(i), tupleB.getFieldStart(i), tupleB.getFieldLength(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    public IBinaryComparator[] getComparators() {
        return cmps;
    }

    public int getKeyFieldCount() {
        return cmps.length;
    }

    public static MultiComparator create(IBinaryComparatorFactory[] cmpFactories) {
        IBinaryComparator[] cmps = new IBinaryComparator[cmpFactories.length];
        for (int i = 0; i < cmpFactories.length; i++) {
            cmps[i] = cmpFactories[i].createBinaryComparator();
        }
        if (cmps.length == 1) {
            return new SingleComparator(cmps);
        } else {
            return new MultiComparator(cmps);
        }
    }

    public static MultiComparator createIgnoreFieldLength(IBinaryComparatorFactory[] cmpFactories) {
        IBinaryComparator[] cmps = new IBinaryComparator[cmpFactories.length];
        for (int i = 0; i < cmpFactories.length; i++) {
            cmps[i] = cmpFactories[i].createBinaryComparator();
        }
        if (cmps.length == 1) {
            return new FieldLengthIgnoringSingleComparator(cmps);
        } else {
            return new FieldLengthIgnoringMultiComparator(cmps);
        }
    }

    public static MultiComparator createIgnoreFieldLength(IBinaryComparatorFactory[] cmpFactories, int startIndex,
            int numCmps) {
        IBinaryComparator[] cmps = new IBinaryComparator[numCmps];
        for (int i = startIndex; i < startIndex + numCmps; i++) {
            cmps[i] = cmpFactories[i].createBinaryComparator();
        }
        if (cmps.length == 1) {
            return new FieldLengthIgnoringSingleComparator(cmps);
        } else {
            return new FieldLengthIgnoringMultiComparator(cmps);
        }
    }

    public static MultiComparator create(IBinaryComparatorFactory[] cmpFactories, int startIndex, int numCmps) {
        IBinaryComparator[] cmps = new IBinaryComparator[numCmps];
        for (int i = startIndex; i < startIndex + numCmps; i++) {
            cmps[i] = cmpFactories[i].createBinaryComparator();
        }
        if (cmps.length == 1) {
            return new SingleComparator(cmps);
        } else {
            return new MultiComparator(cmps);
        }
    }

    public static MultiComparator create(IBinaryComparatorFactory[]... cmpFactories) {
        int size = 0;
        for (int i = 0; i < cmpFactories.length; i++) {
            size += cmpFactories[i].length;
        }
        IBinaryComparator[] cmps = new IBinaryComparator[size];
        int x = 0;
        for (int i = 0; i < cmpFactories.length; i++) {
            for (int j = 0; j < cmpFactories[i].length; j++) {
                cmps[x++] = cmpFactories[i][j].createBinaryComparator();
            }
        }
        if (cmps.length == 1) {
            return new SingleComparator(cmps);
        } else {
            return new MultiComparator(cmps);
        }
    }
}