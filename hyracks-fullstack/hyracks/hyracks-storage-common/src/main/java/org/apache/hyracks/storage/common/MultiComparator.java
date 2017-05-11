/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.common;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class MultiComparator {

    protected final IBinaryComparator[] cmps;

    public MultiComparator(IBinaryComparator[] cmps) {
        this.cmps = cmps;
    }

    public int compare(ITupleReference tupleA, ITupleReference tupleB) throws HyracksDataException {
        for (int i = 0; i < cmps.length; i++) {
            int cmp = cmps[i].compare(tupleA.getFieldData(i), tupleA.getFieldStart(i), tupleA.getFieldLength(i),
                    tupleB.getFieldData(i), tupleB.getFieldStart(i), tupleB.getFieldLength(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    public int selectiveFieldCompare(ITupleReference tupleA, ITupleReference tupleB, int[] fields)
            throws HyracksDataException {
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

    public int compare(ITupleReference tupleA, ITupleReference tupleB, int startFieldIndex)
            throws HyracksDataException {
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

    public int fieldRangeCompare(ITupleReference tupleA, ITupleReference tupleB, int startFieldIndex, int numFields)
            throws HyracksDataException {
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
