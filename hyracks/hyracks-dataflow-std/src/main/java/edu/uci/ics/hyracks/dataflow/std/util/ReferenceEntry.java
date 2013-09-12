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
package edu.uci.ics.hyracks.dataflow.std.util;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class ReferenceEntry {
    private final int runid;
    private FrameTupleAccessor acccessor;
    private int tupleIndex;
    private int[] tPointers;

    public ReferenceEntry(int runid, FrameTupleAccessor fta, int tupleIndex, int[] keyFields,
            INormalizedKeyComputer nmkComputer) {
        super();
        this.runid = runid;
        this.acccessor = fta;
        this.tPointers = new int[1 + 2 * keyFields.length];
        if (fta != null) {
            initTPointer(fta, tupleIndex, keyFields, nmkComputer);
        }
    }

    public int getRunid() {
        return runid;
    }

    public FrameTupleAccessor getAccessor() {
        return acccessor;
    }

    public void setAccessor(FrameTupleAccessor fta) {
        this.acccessor = fta;
    }

    public int[] getTPointers() {
        return tPointers;
    }

    public int getTupleIndex() {
        return tupleIndex;
    }

    public int getNormalizedKey() {
        return tPointers[0];
    }

    public void setTupleIndex(int tupleIndex, int[] keyFields, INormalizedKeyComputer nmkComputer) {
        initTPointer(acccessor, tupleIndex, keyFields, nmkComputer);
    }

    private void initTPointer(FrameTupleAccessor fta, int tupleIndex, int[] keyFields,
            INormalizedKeyComputer nmkComputer) {
        this.tupleIndex = tupleIndex;
        byte[] b1 = fta.getBuffer().array();
        for (int f = 0; f < keyFields.length; ++f) {
            int fIdx = keyFields[f];
            tPointers[2 * f + 1] = fta.getTupleStartOffset(tupleIndex) + fta.getFieldSlotsLength()
                    + fta.getFieldStartOffset(tupleIndex, fIdx);
            tPointers[2 * f + 2] = fta.getFieldEndOffset(tupleIndex, fIdx) - fta.getFieldStartOffset(tupleIndex, fIdx);
            if (f == 0) {
                if (nmkComputer != null) {
                    tPointers[0] = nmkComputer.normalize(b1, tPointers[1], tPointers[2]);
                } else {
                    tPointers[0] = 0;
                }
            }
        }
    }
}